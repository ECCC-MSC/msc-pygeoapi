# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2020 Tom Kralidis
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
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, BulkIndexError

LOGGER = logging.getLogger(__name__)

VERIFY = False


def get_es(url, auth=None):
    """
    helper function to instantiate an Elasticsearch connection

    :param url: URL of ES endpoint
    :param auth: HTTP username-password tuple for authentication (optional)
    :returns: `elasticsearch.Elasticsearch` object
    """

    url_parsed = urlparse(url)
    url_settings = {
        'host': url_parsed.netloc
    }

    LOGGER.debug('Connecting to Elasticsearch')

    if url_parsed.port is None:  # proxy to default HTTP(S) port
        if url_parsed.scheme == 'https':
            url_settings['port'] = 443
            url_settings['scheme'] = url_parsed.scheme
        else:
            url_settings['port'] = 80
    else:  # was set explictly
        url_settings['port'] = url_parsed.port

    if url_parsed.path:
        url_settings['url_prefix'] = url_parsed.path

    LOGGER.debug('URL settings: {}'.format(url_settings))

    if auth is None:
        es = Elasticsearch([url_settings], verify_certs=VERIFY)
    else:
        es = Elasticsearch([url_settings], http_auth=auth, verify_certs=VERIFY)

    if not es.ping():
        msg = 'Cannot connect to Elasticsearch'
        LOGGER.error(msg)
        raise RuntimeError(msg)

    return es


def submit_elastic_package(es, package, request_size=10000):
    """
    Helper function to send an update request to Elasticsearch and
    log the status of the request. Returns True iff the upload succeeded.

    :param es: Elasticsearch client object.
    :param package: Iterable of bulk API update actions.
    :param request_size: Number of documents to upload per request.
    :returns: `bool` of whether the operation was successful.
    """

    inserts = 0
    updates = 0
    noops = 0
    errors = []

    try:
        for ok, response in streaming_bulk(es, package,
                                           chunk_size=request_size,
                                           request_timeout=30,
                                           raise_on_error=False):
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
        LOGGER.error('Unable to perform bulk insert due to: {}'
                     .format(err.errors))
        return False

    if len(errors) != 0:
        LOGGER.error('Errors encountered in bulk insert: {}'.format(errors))
        return False

    total = inserts + updates + noops
    LOGGER.info('Inserted package of {} observations ({} inserts, {} updates,'
                ' {} no-ops'.format(total, inserts, updates, noops))
    return True


def click_abort_if_false(ctx, param, value):
    """
    Helper function to abort (or not) `click` command on prompt/confirm

    :param ctx: context manager
    :param param: name of click option
    :param value: `bool` of value

    :returns: abort of click command if `False`
    """

    if not value:
        ctx.abort()
