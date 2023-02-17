# =================================================================
#
# Author: Etienne <etienne.pelletier@canada.ca>
#
# Copyright (c) 2023 Etienne Pelletier
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging

from elasticsearch import Elasticsearch, logger as elastic_logger
from elasticsearch.helpers import streaming_bulk, BulkIndexError

from msc_pygeoapi.connector.base import BaseConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_ES_USERNAME,
    MSC_PYGEOAPI_ES_PASSWORD,
    MSC_PYGEOAPI_ES_URL,
    MSC_PYGEOAPI_ES_TIMEOUT,
    MSC_PYGEOAPI_LOGGING_LOGLEVEL
)

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))


class ElasticsearchConnector(BaseConnector):
    """Elasticsearch Connector"""

    def __init__(self, connector_def={}):
        """
        Elasticsearch connection initialization

        :param connector_def: connection definition dictionnary

        :returns: msc_pygeoapi.connector.elasticsearch_.ElasticsearchConnector
        """

        self.url = connector_def.get('url', MSC_PYGEOAPI_ES_URL)

        # if no URL passed in connector def or env variable not set default
        # to default ES port on localhost
        if not self.url:
            self.url = 'http://localhost:9200'

        self.verify_certs = connector_def.get('verify_certs', True)

        if 'auth' in connector_def:
            self.auth = connector_def['auth']
        elif all([MSC_PYGEOAPI_ES_USERNAME, MSC_PYGEOAPI_ES_PASSWORD]):
            self.auth = (MSC_PYGEOAPI_ES_USERNAME, MSC_PYGEOAPI_ES_PASSWORD)
        else:
            self.auth = None

        self.Elasticsearch = self.connect()

    def connect(self):
        """create Elasticsearch connection"""

        LOGGER.debug('Connecting to Elasticsearch')

        # if no protocol specified in url append http:// by default
        if not self.url.startswith('http'):
            self.url = 'http://{}'.format(self.url)

        es_args = {
            'hosts': [self.url],
            'verify_certs': self.verify_certs,
            'retry_on_timeout': True,
            'max_retries': 3
        }

        if self.auth:
            es_args['http_auth'] = self.auth

        return Elasticsearch(**es_args)

    def create(self, index_name, mapping, overwrite=False):
        """
        create an Elasticsearch index

        :param index_name: name of in index to create
        :mapping: `dict` mapping of index to create
        :overwrite: `bool` indicating whether to overwrite index if it already
                    exists

        :returns: `bool` of creation status
        """

        # if overwrite is False, do not recreate an existing index
        if (
            self.Elasticsearch.indices.exists(index=index_name)
            and not overwrite
        ):
            LOGGER.info('{} index already exists.')
            return False

        elif self.Elasticsearch.indices.exists(index=index_name) and overwrite:
            self.Elasticsearch.indices.delete(index=index_name)
            LOGGER.info('Deleted existing {} index.'.format(index_name))

        self.Elasticsearch.indices.create(
            index=index_name,
            body=mapping,
            request_timeout=MSC_PYGEOAPI_ES_TIMEOUT
        )

        return True

    def get(self, pattern):
        """
        get list of Elasticsearch index matching a pattern

        :param pattern: `str` of pattern to match

        :returns: `list` of index names matching patterns
        """

        return list(self.Elasticsearch.indices.get(index=pattern).keys())

    def exists(self, index_name):
        """
        determines where an index exists

        :param index: index name

        :return: `bool` of result
        """

        return self.Elasticsearch.indices.exists(index=index_name)

    def delete(self, indexes):
        """
        delete ES index(es)

        :param indexes: indexes to delete, comma-seperated if multiple.

        :returns: `bool` of deletion status
        """

        if indexes in ['*', '_all'] or not self.Elasticsearch.indices.exists(
            index=indexes
        ):
            msg = (
                'Cannot delete {}. '.format(indexes),
                'Either the index does not exist or an unaccepted index ',
                'pattern was given (\'*\' or \'_all\')',
            )
            LOGGER.error(msg)
            raise ValueError(msg)

        LOGGER.info('Deleting indexes {}'.format(indexes))
        self.Elasticsearch.indices.delete(index=indexes)

        return True

    def create_template(self, name, settings):
        """
        create an Elasticsearch index template

        :param name: `str` index template name
        :param settings: `dict` settings dictionnary for index template

        :return: `bool` of index template creation status
        """

        if not self.Elasticsearch.indices.exists_template(name=name):
            self.Elasticsearch.indices.put_template(name=name, body=settings)

        return True

    def delete_template(self, name):
        """
        delete an Elasticsearch index template

        :param name: `str` index template name

        :return: `bool` of index template deletion status
        """

        if self.Elasticsearch.indices.exists_template(name=name):
            self.Elasticsearch.indices.delete_template(name=name)

        return True

    def submit_elastic_package(
        self, package, request_size=10000, refresh=False
    ):
        """
        helper function to send an update request to Elasticsearch and
        log the status of the request. Returns True if the upload succeeded.

        :param package: Iterable of bulk API update actions.
        :param request_size: Number of documents to upload per request.
        :param refresh: indicates whether to refresh the index
        :returns: `bool` of whether the operation was successful.
        """

        inserts = 0
        updates = 0
        noops = 0
        errors = []

        try:
            for ok, response in streaming_bulk(
                self.Elasticsearch,
                package,
                chunk_size=request_size,
                request_timeout=MSC_PYGEOAPI_ES_TIMEOUT,
                raise_on_error=False,
                refresh=refresh
            ):
                if not ok:
                    errors.append(response)
                else:
                    status = response['update']['result']

                    if status == 'created':
                        inserts += 1
                    elif status == 'updated':
                        updates += 1
                    elif status == 'noop':
                        noops += 1
                    else:
                        LOGGER.error('Unhandled status code {}'.format(status))
                        errors.append(response)
        except BulkIndexError as err:
            LOGGER.error(
                'Unable to perform bulk insert due to: {}'.format(err.errors)
            )
            return False

        total = inserts + updates + noops
        LOGGER.info(
            'Inserted package of {} documents ({} inserts, {} updates,'
            ' {} no-ops)'.format(total, inserts, updates, noops)
        )

        if len(errors) > 0:
            LOGGER.warning(
                '{} errors encountered in bulk insert: {}'.format(
                    len(errors), errors
                )
            )
            return False

        return True

    def update_by_query(self, query, name):
        """
        update an Elasticsearch feature

        :param query: `str` query template
        :param name: `str` index name

        :return: `bool` of index update status
        """

        self.Elasticsearch.update_by_query(
            body=query, index=name, refresh=True
        )

        return True

    def __repr__(self):
        return '<ElasticsearchConnector> {}'.format(self.url)
