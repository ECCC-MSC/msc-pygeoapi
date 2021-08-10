# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#         Felix Laframboise <felix.laframboise@ec.gc.ca>
#         Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2020 Etienne Pelletier
# Copyright (c) 2021 Felix Laframboise
# Copyright (c) 2021 Louis-Philippe Rousseau-Lambert
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
from elasticsearch import logger as elastic_logger
from parse import parse

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_LOGGING_LOGLEVEL
)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete,
)

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

# cleanup settings
DAYS_TO_KEEP = 3

# index settings
INDEX_BASENAME = 'aqhi-{}-realtime.'

MAPPINGS = {
    'forecasts': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {
                'properties': {
                    'id': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'aqhi_type': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_name_en': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_name_fr': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_id': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'publication_datetime': {
                        'type': 'date',
                        'format': 'strict_date_time_no_millis',
                    },
                    'forecast_datetime_text_en': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'forecast_datetime_text_fr': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'forecast_datetime': {
                        'type': 'date',
                        'format': 'strict_date_time_no_millis',
                    },
                    'aqhi': {'type': 'byte'},
                }
            }
        }
    },
    'observations': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {
                'properties': {
                    'id': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'aqhi_type': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_name_en': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_name_fr': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'location_id': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'observation_datetime': {
                        'type': 'date',
                        'format': 'strict_date_time_no_millis',
                    },
                    'observation_datetime_text_en': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'observation_datetime_text_fr': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'aqhi': {'type': 'float'},
                }
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': ['{}*'.format(INDEX_BASENAME)],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}


class AQHIRealtimeLoader(BaseLoader):
    """AQHI Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = None
        self.type = None
        self.region = None
        self.date_ = None
        self.items = []

        # only create index templates with forecasts and observations mappings
        template_mappings = {
            k: MAPPINGS[k]
            for k in ('forecasts', 'observations')
        }

        for aqhi_type in template_mappings:
            template_name = INDEX_BASENAME.format(aqhi_type)
            SETTINGS['index_patterns'] = ['{}*'.format(template_name)]
            SETTINGS['mappings'] = MAPPINGS[aqhi_type]
            self.conn.create_template(template_name, SETTINGS)

    def parse_filename(self):
        """
        Parses a aqhi filename in order to get the date, forecast issued
        time, and region name.
        :return: `bool` of parse status
        """
        # parse filepath
        pattern = '{date_}_MSC_AQHI-{type}_{region}.json'
        filename = self.filepath.name
        parsed_filename = parse(pattern, filename)

        # set class attributes
        type_ = parsed_filename.named['type']
        if type_ == 'Forecasts':
            self.type = 'forecasts'
        if type_ == 'Observation':
            self.type = 'observations'

        self.region = parsed_filename.named['region']
        self.date_ = datetime.strptime(
            parsed_filename.named['date_'], '%Y%m%dT%H%MZ'
        )

        return True

    def generate_geojson_features(self):
        """
        Generates and yields a series of aqhi forecasts or observations.
        Forecasts and observations are returned as Elasticsearch bulk API
        upsert actions,with documents in GeoJSON to match the Elasticsearch
        index mappings.
        :returns: Generator of Elasticsearch actions to upsert the AQHI
                  forecasts/observations
        """
        with open(self.filepath.resolve()) as f:
            data = json.load(f)
            if self.type == "forecasts":
                features = data['features']
            elif self.type == "observations":
                features = [data]

        for feature in features:
            # set ES index name for feature
            es_index = '{}{}'.format(
                INDEX_BASENAME.format(self.type),
                self.date_.strftime('%Y-%m-%d'),
            )

            self.items.append(feature)

            action = {
                '_id': feature['id'],
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

        # set class variables from filename
        self.parse_filename()

        LOGGER.debug('Received file {}'.format(self.filepath))

        # generate geojson features
        package = self.generate_geojson_features()
        self.conn.submit_elastic_package(package, request_size=80000)

        return True


@click.group()
def aqhi_realtime():
    """Manages AQHI indexes"""
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
    """Add AQHI data to Elasticsearch"""

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
        loader = AQHIRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help='Delete indexes older than n days (default={})'.format(DAYS_TO_KEEP),
)
@cli_options.OPTION_DATASET(
    help='AQHI dataset indexes to delete.',
    type=click.Choice(['all', 'forecasts', 'observations']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, days, dataset, es, username, password, ignore_certs):
    """Delete old AQHI realtime indexes older than n days"""

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
            click.echo('Deleting indexes {}'.format(indexes_to_delete))
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='AQHI dataset indexes to delete.',
    type=click.Choice(['all', 'forecasts', 'observations']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(ctx, dataset, es, username, password, ignore_certs,
                   index_template):
    """Delete all AQHI realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes = 'aqhi-*'
    else:
        indexes = '{}*'.format(INDEX_BASENAME.format(dataset))

    click.echo('Deleting indexes {}'.format(indexes))

    conn.delete(indexes)

    if index_template:
        for type_ in ('forecasts', 'observations'):
            index_name = INDEX_BASENAME.format(type_)
            click.echo('Deleting index template {}'.format(index_name))
            conn.delete_template(index_name)

    click.echo('Done')


aqhi_realtime.add_command(add)
aqhi_realtime.add_command(clean_indexes)
aqhi_realtime.add_command(delete_indexes)
