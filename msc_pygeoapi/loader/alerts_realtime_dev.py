# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2026 Louis-Philippe Rousseau-Lambert
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
from msc_pygeoapi.env import GEOMET_LOCAL_BASEPATH
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
HOURS_TO_KEEP = 2

# index settings
INDEX_BASENAME = 'alerts-{}-realtime.'

ALIAS = 'alerts-{}-realtime'

MAPPINGS = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'id': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'publication_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time'
                },
                'expiration_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time'
                },
                'validity_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time'
                },
                'event_end_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time'
                },
                'alert_code': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_type': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_name_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_name_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_short_name_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_short_name_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_text_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'alert_text_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'risk_colour_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'risk_colour_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'confidence_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'confidence_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'impact_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'impact_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'feature_name_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'feature_name_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'province': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'status_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'status_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'feature_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [f'{INDEX_BASENAME}*'],
    'settings': {
        'number_of_shards': 1, 'number_of_replicas': 0
    },
    'mappings': {}
}


class AlertsRealtimeLoaderDev(BaseLoader):
    """Alerts Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = None
        self.date_ = None
        self.index_date = None
        self.items = []

    def parse_filename(self, filename):
        """
        Parses an alerts filename in order to get the date

        :return: `bool` of parse status
        """

        # parse filepath
        # example 20251126T182051.607Z_MSC_Alerts.json:DMS:CMC:ALERTS:JSON:20251126182137  # noqa
        pattern = '{date_}_MSC_Alerts.json{_}'  # noqa
        parsed_filename = parse(pattern, filename)

        self.date_ = datetime.strptime(
            parsed_filename.named['date_'], '%Y%m%dT%H%M%S.%fZ'
        )
        self.index_date = datetime.strftime(self.date_, '%Y-%m-%dt%H%M%S.%fz')

        return True

    def swap_alias(self, index_name, alias_name):
        """
        Swap aliases to point to the new alerts index

        :return: `bool` of parse status
        """

        self.conn.create_alias(alias_name,
                               index_name,
                               overwrite=True)

        return True

    # Uncommented because this is for testing scenarios only (dev)
    def delete_indices(self, indices):
        for idx in indices:
            self.conn.delete(idx)

        return True

    def generate_geojson_features(self, es_index, alert_type):
        """
        Generates and yields a series of alerts.
        Alerts are returned as Elasticsearch bulk API
        upsert actions,with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :returns: Generator of Elasticsearch actions to upsert the alerts
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)
            # writting latest alerts file locally
            # this will be used by geomet-weather
            # to avoid the 500 hit limit per minute
            alerts_local_path = os.path.join(GEOMET_LOCAL_BASEPATH,
                                             'alerts',
                                             f'dms-alerts-{alert_type}.json')
            LOGGER.debug(f'new local file: {alerts_local_path}')
            with open(alerts_local_path, 'w', encoding='utf-8') as alerts:
                alerts.write(json.dumps(data))

            features = data['features']

        for feature in features:
            if feature['properties'].get('display_status') != 'contour':
                prop_id = feature['properties']['id']
                feat_id = feature['properties']['feature_id']
                feature['id'] = f'{prop_id}_{feat_id}'
                feature['properties']['id'] = feature['id']

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

        # get alerts feed (alpha, dev or stage)
        pattern_type = '{basepath}/dms-{alert_type}/alerts/{filename}'
        alert_type_parsed = parse(pattern_type, filepath)
        alert_type = alert_type_parsed.named['alert_type']
        template_name = INDEX_BASENAME.format(alert_type)
        alias_name = ALIAS.format(alert_type)

        # set class variables from filename
        LOGGER.debug(f'Received file {self.filepath}')
        self.parse_filename(filename)

        # set new index name
        es_index = f'{template_name}{self.index_date}'
        LOGGER.debug(f'new index name: {es_index}')

        # Check if alias already exists
        LOGGER.debug(f'Checking if {self.filepath} is the most recent file')
        is_more_recent = False

        # using "or []" to avoid having current_indices = None
        current_indices = (self.conn.get_alias_indices(alias_name)) or []
        LOGGER.debug(f'Current indices {current_indices}')

        is_more_recent = all(
            self.date_ > datetime.strptime('.'.join(idx.split('.')[1:]),
                                           '%Y-%m-%dt%H%M%S.%fz')
            for idx in current_indices
            )
        LOGGER.debug(f'Is new file more recent --> {is_more_recent}')

        if is_more_recent:
            LOGGER.debug(f'{self.filepath} is the most recent file')
            SETTINGS['index_patterns'] = [f'{template_name}*']
            SETTINGS['mappings'] = MAPPINGS
            self.conn.create_template(template_name, SETTINGS)

            # create index
            # necessary for empty alerts json
            self.conn.create(es_index, {'mappings': MAPPINGS})

            # generate geojson features
            package = self.generate_geojson_features(es_index, alert_type)
            self.conn.submit_elastic_package(package, request_size=80000)

            # Swap alias
            LOGGER.debug(f'Swapping alias: {es_index}')
            self.swap_alias(es_index, alias_name)

            # Delete old indices
            LOGGER.debug(f'Deleting previous indexes: {current_indices}')
            self.delete_indices(current_indices)

        return True


@click.group()
def alerts_realtime_dev():
    """Manages alerts indexes"""
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
    """Add alerts data to Elasticsearch"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file_ for file_ in files if '.json' in file_]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = AlertsRealtimeLoaderDev(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_HOURS(
    default=HOURS_TO_KEEP,
    help=f'Delete indexes older than n hours (default={HOURS_TO_KEEP})',
)
@cli_options.OPTION_DATASET(
    help='AQHI dataset indexes to delete.',
    type=click.Choice(['all', 'alpha', 'dev', 'stage']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, hours, dataset, es, username, password, ignore_certs):
    """Delete old alerts realtime indexes older than n hours"""

    # ex: alerts-realtime.2025-12-03t151500.000000z
    pattern = '{index_name}.{year:d}-{month:d}-{day:d}t{hour:02d}{minute:02d}{second:02d}.{microsecond:d}z'  # noqa

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes_to_fetch = []
    if dataset == 'all':
        for alert_type in ['alpha', 'dev', 'stage']:
            idx_name = '{}*'.format(INDEX_BASENAME.format(alert_type))
            indexes_to_fetch.append(idx_name)
    else:
        indexes_to_fetch = '{}*'.format(INDEX_BASENAME.format(dataset))

    indexes = conn.get(indexes_to_fetch)

    click.echo(f'indexes: {indexes}')

    if indexes:
        # newest index should not be deleted
        indexes.sort()
        indexes.pop(-1)
        days = hours / 24.0
        indexes_to_delete = check_es_indexes_to_delete(indexes, days, pattern)
        click.echo(f'indexes to delete: {indexes_to_delete}')

        if indexes_to_delete:
            click.echo(f'Deleting indexes {indexes_to_delete}')
            # delete indices individually to safeguard against HTTP 411 errors
            for index in indexes_to_delete:
                conn.delete(index)

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='AQHI dataset indexes to delete.',
    type=click.Choice(['all', 'alpha', 'dev', 'stage']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(ctx, dataset, es, username, password, ignore_certs,
                   index_template):
    """Delete all alerts realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes = '{}*'.format(INDEX_BASENAME.format('*'))
    else:
        indexes = '{}*'.format(INDEX_BASENAME.format(dataset))

    matching = list(conn.get(indexes))
    click.echo(f'Deleting indexes {matching}')
    if matching:
        conn.delete(matching)

    if index_template:
        index_name = indexes
        click.echo(f'Deleting index template {index_name}')
        conn.delete_template(index_name)

    click.echo('Done')


alerts_realtime_dev.add_command(add)
alerts_realtime_dev.add_command(clean_indexes)
alerts_realtime_dev.add_command(delete_indexes)
