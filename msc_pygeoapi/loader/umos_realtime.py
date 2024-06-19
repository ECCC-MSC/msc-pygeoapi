# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2024 Louis-Philippe Rousseau-Lambert
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

from datetime import datetime
import json
import logging
import os
from pathlib import Path

import click
from parse import parse

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete,
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 7

# index settings
INDEX_BASENAME = 'umos-{}-realtime.'

MAPPINGS = {
    "dynamic_templates": [
        {
            "forecast_value": {
                "match_mapping_type": "double",
                "mapping": {
                    "type": "double"
                }
            }
        },
        {
            "forecast_value_string": {
                "match_mapping_type": "string",
                "mapping": {
                    "type": "text"
                }
            }
        }
    ],
    'properties': {
        'geometry': {
            'type': 'geo_shape'
        },
        'properties': {
            'properties': {
                'id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                    },
                'ssp_system': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'nwep_system_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'nwep_system_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'reference_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis||strict_date_optional_time'  # noqa
                },
                'forecast_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis||strict_date_optional_time'  # noqa
                },
                'stat_method': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'umos_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'scribe_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'station_name': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'province': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'forecast_leadtime': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'vertical_coordinate': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'variable': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'unit': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [f'{INDEX_BASENAME}*'],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}

MODEL_LIST = ['gdps', 'rdps', 'hrdps', 'raqdps']


class UMOSRealtimeLoader(BaseLoader):
    """UMOS Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = None
        self.date_ = None
        self.model = None
        self.items = []

    def parse_filename(self, filename):
        """
        Parses a umos filename in order to get the date

        :return: `bool` of parse status
        """

        # parse filepath
        pattern = '{date_}_MSC_{model}-UMOS-{stat}_{variable}_{elevation}_PT{fcts}H.json' # noqa
        parsed_filename = parse(pattern, filename)

        self.model = parsed_filename.named['model'].lower()

        self.date_ = datetime.strptime(
            parsed_filename.named['date_'], '%Y%m%dT%H%MZ'
        )

        return True

    def generate_geojson_features(self):
        """
        Generates and yields a series of umos.
        Umos are returned as Elasticsearch bulk API
        upsert actions,with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :returns: Generator of Elasticsearch actions to upsert the UMOS
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)
            features = data['features']

        for feature in features:
            # set ES index name for feature
            es_index = '{}{}'.format(
                INDEX_BASENAME.format(self.model),
                self.date_.strftime('%Y-%m-%d'),
            )

            self.items.append(feature)

            action = {
                '_id': feature['id'],
                '_index': es_index,
                '_op_type': 'update',
                'doc': feature,
                'doc_as_upsert': True
            }

            yield action

    def load_data(self, filepath):
        """
        loads data from event to target
        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)

        filename = self.filepath.name

        # set class variables from filename
        self.parse_filename(filename)

        template_name = INDEX_BASENAME.format(self.model)

        SETTINGS['index_patterns'] = [f'{template_name}*']
        SETTINGS['mappings'] = MAPPINGS
        self.conn.create_template(template_name, SETTINGS)

        LOGGER.debug(f'Received file {self.filepath}')

        # generate geojson features
        package = self.generate_geojson_features()
        self.conn.submit_elastic_package(package, request_size=80000)

        return True


@click.group()
def umos_realtime():
    """Manages UMOS indexes"""
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
    """Add UMOS data to Elasticsearch"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.json')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = UMOSRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help=f'Delete indexes older than n days (default={DAYS_TO_KEEP})',
)
@cli_options.OPTION_DATASET(
    help='UMOS dataset indexes to delete.',
    type=click.Choice(MODEL_LIST + ['all']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, days, dataset, es, username, password, ignore_certs):
    """Delete old UMOS realtime indexes older than n days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes_to_fetch = '{}*'.format(INDEX_BASENAME.format('*'))
    else:
        indexes_to_fetch = '{}*'.format(INDEX_BASENAME.format(dataset))

    indexes = conn.get(indexes_to_fetch)

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo(f'Deleting indexes {indexes_to_delete}')
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='UMOS dataset indexes to delete.',
    type=click.Choice(MODEL_LIST + ['all']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(ctx, dataset, es, username, password, ignore_certs,
                   index_template):
    """Delete all UMOS realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes = 'umos-*'
    else:
        indexes = '{}*'.format(INDEX_BASENAME.format(dataset))

    click.echo(f'Deleting indexes {indexes}')

    conn.delete(indexes)

    if index_template:
        for type_ in MODEL_LIST:
            index_name = INDEX_BASENAME.format(type_)
            click.echo(f'Deleting index template {index_name}')
            conn.delete_template(index_name)

    click.echo('Done')


umos_realtime.add_command(add)
umos_realtime.add_command(clean_indexes)
umos_realtime.add_command(delete_indexes)
