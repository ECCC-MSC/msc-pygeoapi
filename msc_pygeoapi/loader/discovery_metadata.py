# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
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

import click
import logging
import os

from pygeometa.core import read_mcf
from pygeometa.schemas.ogcapi_records import OGCAPIRecordOutputSchema

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection


LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'discovery-metadata'

SETTINGS = {
    'settings': {
        'number_of_shards': 1, 'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            }
        }
    }
}


class DiscoveryMetadataLoader(BaseLoader):
    """Discovery Metadata loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.items = []

        if not self.conn.indices.exists(INDEX_NAME):
            LOGGER.debug('Creating index {}'.format(INDEX_NAME))
            self.conn.create(INDEX_NAME, SETTINGS)

    def load_data(self, json_dict):
        """
        loads data from event to target

        :param json_dict: `dict` of JSON metadata representation

        :returns: `bool` of status result
        """

        identifier = json_dict['f']

        LOGGER.debug('Adding record {} to index {}'.format(identifier, INDEX_NAME))  # noqa
        _ = self.conn.index(index=INDEX_NAME, id=identifier, body=json_dict)

        return True

    def generate_metadata(filepath):
        """
        Generates discovery metadata based on MCF

        :param filepath: path to MCF file on disk

        :retgurns: `dict` of discovery metadata
        """

        LOGGER.info('Processing MCF: {}'.format(filepath))

        try:
            m = read_mcf(filepath)
        except Exception as err:
            msg = 'ERROR {}'.format(err)
            LOGGER.error(msg)
            raise

        output_schema = OGCAPIRecordOutputSchema

        try:
            metadata = output_schema().write(m, stringify=False)
        except Exception as err:
            msg = 'ERROR {}'.format(err)
            LOGGER.error(msg)
            raise

        return metadata


@click.group()
def discovery_metadata():
    """Manages Discovery Metadata"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_DIRECTORY()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, directory, es, username, password, ignore_certs):
    """Adds discovery metadata"""

    if directory is None:
        raise click.ClickException('Missing --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = DiscoveryMetadataLoader(conn_config)

    total = 0
    passed = 0
    failed = 0

    for root, dirs, files in os.walk('{}/mcf'.format(directory)):
        for name in files:
            total += 1
            if any(['shared' in root,
                    'template' in name, not name.endswith('yml')]):
                continue
            mcf_file = ('{}/{}'.format(root, name))
            try:
                metadata = loader.generate_metadata(mcf_file)
                _ = loader.load_data(metadata)
                passed += 1
            except Exception as err:
                failed += 1
                msg = 'ERROR: {}'.format(err)
                LOGGER.error(msg)
                continue

    LOGGER.debug('TOTAL: {}'.format(total))
    LOGGER.debug('PASSED: {}'.format(passed))
    LOGGER.debug('FAILED: {}'.format(failed))


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs):
    """Delete discovery metadata index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if conn.indices.exists(INDEX_NAME):
        click.echo('Deleting index {}'.format(INDEX_NAME))
        conn.delete(INDEX_NAME)

    click.echo('Done')


discovery_metadata.add_command(add)
discovery_metadata.add_command(delete_index)
