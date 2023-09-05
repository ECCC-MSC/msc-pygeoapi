# =================================================================
#
# Author: Philippe Theroux <Philippe.Theroux@ec.gc.ca>
#
# Copyright (c) 2022 Philippe Theroux
# Copyright (c) 2023 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from datetime import datetime
import json
import logging
import os
from pathlib import Path

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    check_es_indexes_to_delete,
    configure_es_connection,
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 10 * 365.25

# index settings
INDEX_BASENAME = 'cumulative_effects_hs.'

MAPPINGS = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'rep_date': {
                    'type': 'date',
                    'format': 'yyyy/MM/dd HH:mm:ss',
                },
                '_rep_date': {
                    'type': 'date',
                    'format': 'yyyy/M/d H:m:s.SSS',
                },
                'identifier': {
                    'type': 'integer',
                },
                'tfc': {
                    'type': 'float',
                },
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [INDEX_BASENAME],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}


class CumulativeEffectsHSLoader(BaseLoader):
    """Cumulative Effects Hotspots loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.filepath = None
        self.datetime = None
        self.conn = ElasticsearchConnector(conn_config)

        SETTINGS['mappings'] = MAPPINGS
        self.conn.create_template(INDEX_BASENAME, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of cumulative effects hotspots.
        They are returned as Elasticsearch bulk API upsert actions,
        with documents in GeoJSON to match the Elasticsearch index mappings.

        :returns: Generator of Elasticsearch actions to upsert the
                  Cumulative Effects Hotspots
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)['features']

        for feature in data:
            self.datetime = datetime.strptime(
                feature['properties']['rep_date'], '%Y/%m/%d %H:%M:%S'
            )

            # set ES index name for feature
            es_index = f"{INDEX_BASENAME}{self.datetime.strftime('%Y')}"
            feature['id'] = feature['properties']['identifier']

            # add properties
            feature['properties']['_rep_date'] = self.datetime.strftime(
                '%Y/%m/%d %H:%M:%S.%f'
            )[:-3]

            action = {
                '_id': feature['properties']['identifier'],
                '_index': es_index,
                '_op_type': 'update',
                'doc': feature,
                'doc_as_upsert': True,
            }

            yield action

    def load_data(self, filepath):
        """
        loads data from event to target

        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)

        LOGGER.debug(f'Received file {self.filepath}')

        # generate geojson features
        package = self.generate_geojson_features()
        try:
            r = self.conn.submit_elastic_package(package, request_size=80000)
            LOGGER.debug(f'Result: {r}')
            return True
        except Exception as err:
            LOGGER.warning(f'Error indexing: {err}')
            return False


@click.group()
def cumulative_effects_hs():
    """Manages cumulative effects hotspots index"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_DIRECTORY()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, directory, es, username, password, ignore_certs):
    """add data to system"""

    if file_ is None and directory is None:
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [f for f in files if f.endswith('.json')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = CumulativeEffectsHSLoader(conn_config)
        result = loader.load_data(file_to_process)

        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help=f'Delete indexes older than n days (default={DAYS_TO_KEEP})'
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old indexes?'
)
def clean_indexes(ctx, days, es, username, password, ignore_certs):
    """Clean cumulative effects hotspots indexes older than n number of days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get(f'{INDEX_BASENAME}*')

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo(f'Deleting indexes {indexes_to_delete}')
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs, index_template):
    """Delete cumulative effects hotspots index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    all_indexes = f'{INDEX_BASENAME}*'

    click.echo(f'Deleting indexes {all_indexes}')
    conn.delete(all_indexes)

    if index_template:
        click.echo(f'Deleting index template {INDEX_BASENAME}')
        conn.delete_template(INDEX_BASENAME)

    click.echo('Done')


cumulative_effects_hs.add_command(add)
cumulative_effects_hs.add_command(clean_indexes)
cumulative_effects_hs.add_command(delete_index)
