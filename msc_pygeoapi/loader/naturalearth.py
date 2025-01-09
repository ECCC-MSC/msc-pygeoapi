# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#           <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2022 Louis-Philippe Rousseau-Lambert
# Copyright (c) 2023 Tom Kralidis
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

import click
from elasticsearch import logger as elastic_logger
import fiona

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_LOGGING_LOGLEVEL,
)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

DATETIME_FORMAT = '%Y%m%dT%H%M%S.%fZ'

# cleanup settings
DAYS_TO_KEEP = 7

INDEX_NAME = 'naturalearth_data'

MAPPINGS = {'properties': {'geometry': {'type': 'geo_shape'}}}

SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}


class NaturalEarthDataLoader(BaseLoader):
    """Loads NaturalEarth data into Elasticsearch from a provided GeoPackage"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        SETTINGS['mappings'] = MAPPINGS
        self.conn.create(INDEX_NAME, SETTINGS)

    def generate_geojson_features(self, file):
        """
        Generates and yields a series of features,
        one for each feature in the provided GeoPackage.
        Features are returned as Elasticsearch bulk API
        upsert actions, with documents in GeoJSON to match the Elasticsearch
        index mappings.
        :param file: Path to the GeoPackage file
        :returns: Generator of Elasticsearch actions
        """
        data = fiona.open(file)

        for feature in data:
            # convert fiona Feature object to dict
            feature = fiona.model.to_dict(feature)
            feature_dict = {
                'type': 'Feature',
                'id': feature['id'],
                'geometry': feature['geometry'],
                'properties': feature['properties']
            }
            action = {
                '_id': feature['id'],
                '_index': INDEX_NAME,
                '_op_type': 'update',
                'doc': feature_dict,
                'doc_as_upsert': True
            }

            yield action


@click.group()
def naturalearth():
    """Manage NaturalEarth data index in Elasticsearch"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, es, username, password, ignore_certs):
    """add data to system"""

    if file_ is None:
        raise click.ClickException('Missing --file/-f')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = NaturalEarthDataLoader(conn_config)

    try:
        features = loader.generate_geojson_features(file_)
        loader.conn.submit_elastic_package(features)
    except Exception as err:
        msg = f'Could not populate naturalearth_data index: {err}'
        raise click.ClickException(msg)


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete this index?')
def delete_index(ctx, es, username, password, ignore_certs, index_template):
    """Delete NaturalEarth data index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    all_indexes = f'{INDEX_NAME}'

    click.echo(f'Deleting indexes {all_indexes}')
    conn.delete(all_indexes)

    if index_template:
        click.echo(f'Deleting index template {INDEX_NAME}')
        conn.delete_template(INDEX_NAME)

    click.echo('Done')


naturalearth.add_command(add)
naturalearth.add_command(delete_index)
