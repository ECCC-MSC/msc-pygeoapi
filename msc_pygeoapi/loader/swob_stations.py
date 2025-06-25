# =================================================================
#
# Author: Bruno Fang
#             <bruno.fang@ec.gc.ca>
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2024 Bruno Fang
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

import csv
import logging
import os
import urllib.request
# import chardet

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_CACHEDIR
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_BASENAME = 'swob-{}-stations'
STATIONS_LIST_NAME = 'swob-xml_{}station_list.csv'
STATIONS_LIST_URL = f'https://dd.weather.gc.ca/today/observations/doc/{STATIONS_LIST_NAME}' # noqa
STATIONS_CACHE = os.path.join(MSC_PYGEOAPI_CACHEDIR, STATIONS_LIST_NAME)
IGNORE_STATIONS_PARTNER = ['ON-MNRF-AFFES_0QD', 'ON-MNRF-AFFES_1QD',
                           'ON-MNRF-AFFES_2QD', 'ON-MNRF-AFFES_3QD',
                           'ON-MNRF-AFFES_4QD']
IGNORE_STATIONS_MARINE = ['9100990', '9300425', '9300990',
                          '9300991', '9400790', '9400990',
                          '9400991']

if not os.path.exists(MSC_PYGEOAPI_CACHEDIR):
    os.makedirs(MSC_PYGEOAPI_CACHEDIR)

MAPPINGS = {
    'properties': {
        'geometry': {
            'type': 'geo_shape'
        },
        'properties': {
            'properties': {
                'iata_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                    },
                'name': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'name_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'name_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'province_territory': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'auto_man': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'wmo_id': {
                    "type": "integer"
                },
                'msc_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'icao_id': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'dst_time': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'std_time': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_provider': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_provider_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_provider_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_attribution_notice': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_attribution_notice_en': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'data_attribution_notice_fr': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'dataset_network': {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                }
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': None,
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': MAPPINGS
}

DATASET_LIST = ['surface', 'partner', 'marine']


class SWOBStationLoader(BaseLoader):
    """SWOB Station loader"""

    def __init__(self, conn_config={}, file_=None, dataset=None):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = file_
        self.items = []
        self.dataset = dataset
        if not os.path.exists(self.filepath):
            LOGGER.debug(f'file {self.filepath} does not exist')
            LOGGER.debug(f'downloading {self.dataset}')
            download_stations(self.dataset)

        # create index templates for surface, marine and partner settings
        for swob_stations_dataset in DATASET_LIST:
            template_name = INDEX_BASENAME.format(swob_stations_dataset)
            SETTINGS['index_patterns'] = [template_name]
            self.conn.create_template(template_name, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of SWOB station.
        Forecasts and observations are returned as Elasticsearch bulk API
        upsert actions,with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :returns: Generator of Elasticsearch actions to upsert the SWOB
                  stations
        """

        if os.path.exists(self.filepath):
            file_list = self.filepath

        elif self.dataset == 'surface':
            file_list = os.path.join(MSC_PYGEOAPI_CACHEDIR,
                                     STATIONS_LIST_NAME.format(''))
            with open(file_list, newline='', encoding='utf-8') as file_swob_stations: # noqa
                csv_reader = csv.DictReader(file_swob_stations)
                for row in csv_reader:
                    id_ = row['MSC_ID']
                    # id_ = f"{row['IATA_ID']}_{row['Dataset/Network']}"
                    if row['WMO_ID'] == '':
                        row['WMO_ID'] = None
                    else:
                        row['WMO_ID'] = int(row['WMO_ID'])
                    # ignore stations with undefined coordinates
                    if (
                        row['Latitude'] != ''
                        and row['Longitude'] != ''
                        and row['Elevation(m)'] != ''
                    ):
                        station_info = {
                            'type': 'Feature',
                            'id': id_,
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [
                                    float(row['Longitude']),
                                    float(row['Latitude']),
                                    float(row['Elevation(m)'])
                                ]
                            },
                            'properties': {
                                'iata_id': row['IATA_ID'],
                                'name': row['Name'],
                                'wmo_id': row['WMO_ID'],
                                'msc_id': id_,
                                'data_provider': row['Data_Provider'],
                                'dataset_network': row['Dataset/Network'],
                                'auto_man': row['AUTO/MAN'],
                                'province_territory': row['Province/Territory']
                            }
                        }
                        self.items.append(station_info)

        elif self.dataset == 'partner':
            file_list = os.path.join(MSC_PYGEOAPI_CACHEDIR,
                                     STATIONS_LIST_NAME.format('partner_'))
            with open(file_list, newline='', encoding='utf-8') as file_swob_stations: # noqa
                # use result = chardet.detect(file.read()) to find encoding
                csv_reader = csv.DictReader(file_swob_stations)
                for row in csv_reader:
                    id_ = row['# MSC ID']
                    if row['# WMO ID'] == '':
                        row['# WMO ID'] = None
                    else:
                        row['# WMO ID'] = int(row['# WMO ID'])
                    # ignore test, temporary, inactive or
                    # quick deploy (lat/lon not fixed) stations
                    if id_ not in IGNORE_STATIONS_PARTNER and (
                        row['Latitude'] != ''
                        and row['Longitude'] != ''
                        and row['Elevation'] != ''
                    ):
                        station_info = {
                            'type': 'Feature',
                            'id': id_,
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [
                                    float(row['Longitude']),
                                    float(row['Latitude']),
                                    float(row['Elevation'])
                                ]
                            },
                            'properties': {
                                'iata_id': row['#IATA'],
                                'name_fr': row['FR name'],
                                'name_en': row['FR name'],
                                'province_territory': row['Province'],
                                'auto_man': row['AUTO/MAN'],
                                'icao_id': row['# ICAO ID'],
                                'wmo_id': row['# WMO ID'],
                                'msc_id': id_,
                                'dst_time': row['DST Time'],
                                'std_time': row['STD Time'],
                                'data_provider_en': row['Data Provider'],
                                'data_provider_fr': row['Data Provider French'], # noqa
                                'data_attribution_notice_en': row['Data Attribution Notice'], # noqa
                                'data_attribution_notice_fr': row['Data Attribution Notice French'] # noqa
                            }
                        }
                        self.items.append(station_info)

        elif self.dataset == 'marine':
            file_list = os.path.join(MSC_PYGEOAPI_CACHEDIR,
                                     STATIONS_LIST_NAME.format('marine_'))
            with open(file_list, newline='', encoding='utf-8') as file_swob_stations: # noqa
                csv_reader = csv.DictReader(file_swob_stations)
                for row in csv_reader:
                    id_ = row['# MSC']
                    if row['# WMO'] == '':
                        row['# WMO'] = None
                    else:
                        row['# WMO'] = int(row['# WMO'])
                    # ignore test, temporary, inactive or
                    # quick deploy (lat/lon not fixed) stations
                    if id_ not in IGNORE_STATIONS_MARINE and (
                        row['Latitude'] != ''
                        and row['Longitude'] != ''
                        and row['Elevation'] != ''
                    ):
                        station_info = {
                            'type': 'Feature',
                            'id': id_,
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [
                                    float(row['Longitude']),
                                    float(row['Latitude']),
                                    float(row['Elevation'])
                                ]
                            },
                            'properties': {
                                'iata_id': row['#IATA'],
                                'name_fr': row['FR name'],
                                'name_en': row['FR name'],
                                'province_territory': row['Province'],
                                'auto_man': row['AUTO/MAN'],
                                'icao_id': row['# ICAO'],
                                'wmo_id': row['# WMO'],
                                'msc_id': id_,
                                'dst_time': row['DST Time'],
                                'std_time': row['STD Time'],
                                'data_provider': row['Data Provider'],
                                'data_attribution_notice': row['Data Attribution Notice'] # noqa
                            }
                        }
                        self.items.append(station_info)

        for item in self.items:
            action = {
                    '_id': item['id'],
                    '_index': INDEX_BASENAME.format(self.dataset),
                    '_op_type': 'update',
                    'doc': item,
                    'doc_as_upsert': True
            }
            yield action

    def load_data(self):
        """
        loads data from event to target

        :returns: `bool` of status result
        """

        LOGGER.debug(f'Received file {self.filepath}')

        # generate geojson features
        package = self.generate_geojson_features()
        self.conn.submit_elastic_package(package)

        return True


def download_stations(dataset):
    """
    Download realtime stations

    :returns: void
    """

    dataset_dict = {
                    'surface': '',
                    'partner': 'partner_',
                    'marine': 'marine_'
                    }
    station = dataset_dict[dataset]

    try:
        station_file = STATIONS_LIST_NAME.format(station)
        station_url = (
            f'https://dd.weather.gc.ca/observations/doc/{station_file}'
        )
        station_cache = os.path.join(MSC_PYGEOAPI_CACHEDIR, station_file)
        LOGGER.debug(f'Caching {station_url} to {station_cache}')
        urllib.request.urlretrieve(station_url, station_cache)
    except urllib.error.HTTPError as e:
        LOGGER.error(f'{station_url} error: {e}')


@click.group()
def swob_stations():
    """Manages SWOB stations indexes"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='SWOB dataset indexes to delete.',
    type=click.Choice(DATASET_LIST),
)
@cli_options.OPTION_FILE()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, dataset, file_, es, username, password, ignore_certs):
    """Add SWOB stations to Elasticsearch"""

    if dataset is None:
        raise click.ClickException('Missing --dataset option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    if file_ is None:
        file_ = ''
    loader = SWOBStationLoader(conn_config, file_, dataset)
    result = loader.load_data()
    if not result:
        click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='SWOB dataset indexes to delete.',
    type=click.Choice(DATASET_LIST + ['all']),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(ctx, dataset, es, username, password, ignore_certs,
                   index_template):
    """Delete all SWOB stations indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes = 'swob-*'
    else:
        indexes = INDEX_BASENAME.format(dataset)

    click.echo(f'Deleting indexes {indexes}')

    conn.delete(indexes)

    if index_template:
        for type_ in DATASET_LIST:
            index_name = INDEX_BASENAME.format(type_)
            click.echo(f'Deleting index template {index_name}')
            conn.delete_template(index_name)

    click.echo('Done')


swob_stations.add_command(add)
swob_stations.add_command(delete_indexes)
