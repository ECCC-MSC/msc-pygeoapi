
# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2023 Louis-Philippe Rousseau-Lambert
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
import os
import urllib.request
import xml.etree.ElementTree as ET

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_CACHEDIR
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete,
)

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_BASENAME = 'aqhi_stations'

STATIONS_LIST_NAME = 'AQHI_XML_File_List.xml'
STATIONS_LIST_URL = f'https://dd.weather.gc.ca/today/air_quality/doc/{STATIONS_LIST_NAME}'  # noqa
STATIONS_CACHE = os.path.join(MSC_PYGEOAPI_CACHEDIR, STATIONS_LIST_NAME)

if not os.path.exists(MSC_PYGEOAPI_CACHEDIR):
    os.makedirs(MSC_PYGEOAPI_CACHEDIR)

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [INDEX_BASENAME],
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'properties': {
                'properties': {
                    'location_id': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                     },
                    'location_name_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'location_name_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'eccc_administrative-zone': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'eccc_administrative-zone_name_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'eccc_administrative-zone_name_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'url_msc-datamart_observation': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'url_msc-datamart_forecast': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    }
                }
            }
        }
    }
}


class AQHIStationLoader(BaseLoader):
    """AQHI Station loader"""

    def __init__(self, conn_config={}, station_file=STATIONS_CACHE):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = station_file
        self.items = []

        if not os.path.exists(self.filepath):
            self.filepath = download_stations()

        self.conn.create_template(INDEX_BASENAME, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of aqhi station.
        Forecasts and observations are returned as Elasticsearch bulk API
        upsert actions,with documents in GeoJSON to match the Elasticsearch
        index mappings.
        :returns: Generator of Elasticsearch actions to upsert the AQHI
                  stations
        """
        tree = ET.parse(self.filepath)
        root = tree.getroot()
        i = 0
        for administrative_region in root:
            for region_list in list(administrative_region):
                for region in list(region_list):
                    self.items.append(
                        {
                            'type': 'Feature',
                            'geometry': {
                                'type': 'Point',
                                'coordinates': [
                                    float(region.attrib['longitude']),
                                    float(region.attrib['latitude'])
                                ]
                            },
                            'properties': {}
                        }
                    )

                    properties_ = self.items[i]['properties']

                    # add values from EC_administrativeZone tag
                    for attr, val in administrative_region.attrib.items():
                        field_name = f'{administrative_region.tag}_{attr}'
                        if field_name == 'EC_administrativeZone_abreviation':
                            properties_['eccc_administrative-zone'] = val
                        elif field_name == 'EC_administrativeZone_name_en_CA':
                            properties_['eccc_administrative-zone_name_en'] = \
                                val
                        elif field_name == 'EC_administrativeZone_name_fr_CA':
                            properties_['eccc_administrative-zone_name_fr'] = \
                                val

                    # add values from region tag
                    for field_name, val in region.attrib.items():
                        if field_name == 'cgndb':
                            properties_['location_id'] = val
                        elif field_name == 'nameEn':
                            properties_['location_name_en'] = val
                        elif field_name == 'nameFr':
                            properties_['location_name_fr'] = val

                    # add values under region tag
                    for info in list(region):
                        if info.text.strip():
                            if info.tag == 'pathToCurrentForecast':
                                properties_['url_msc-datamart_forecast'] = (
                                    info.text
                                )
                            elif info.tag == 'pathToCurrentObservation':
                                properties_['url_msc-datamart_observation'] = (
                                    info.text
                                )

                        # means there are associated stations, add them too
                        if list(info):
                            prop_dict = properties_
                            prop_dict['station'] = {}

                            for index, station in enumerate(list(info)):
                                prop_dict['station'][index] = {}
                                for field, value in station.attrib.items():
                                    as_dict = prop_dict['station'][index]
                                    as_dict[field] = value

                    # I need to keep increasing otherwise you'll end up
                    # overwriting data, so can't use enumerate
                    id_ = properties_['location_id']
                    self.items[i]['id'] = id_

                    action = {
                        '_id': id_,
                        '_index': INDEX_BASENAME,
                        '_op_type': 'update',
                        'doc': self.items[i],
                        'doc_as_upsert': True
                    }

                    yield action

                    i += 1

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


def download_stations():
    """
    Download realtime stations
    :returns: `str` path to downloaded file
    """

    LOGGER.debug(f'Caching {STATIONS_LIST_URL} to {STATIONS_CACHE}')

    try:
        urllib.request.urlretrieve(STATIONS_LIST_URL, STATIONS_CACHE)
    except Exception as err:
        msg = f'Error downloading {STATIONS_LIST_URL}: {err}'
        LOGGER.error(msg)
        raise RuntimeError(msg)

    return STATIONS_CACHE


@click.group()
def aqhi_stations():
    """Manages AQHI indexes"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, es, username, password, ignore_certs):
    """Add AQHI data to Elasticsearch"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    loader = AQHIStationLoader(conn_config, file_ or STATIONS_CACHE)
    result = loader.load_data()
    if not result:
        click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old index?')
def clean_index(ctx, es, username, password, ignore_certs):
    """Delete old AQHI stations index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get(INDEX_BASENAME)

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes)
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
def delete_index(ctx, es, username, password, ignore_certs,
                 index_template):
    """Delete all AQHI realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = f'{INDEX_BASENAME}*'

    click.echo(f'Deleting indexes {indexes}')

    conn.delete(indexes)

    if index_template:
        index_name = INDEX_BASENAME
        click.echo(f'Deleting index template {index_name}')
        conn.delete_template(index_name)

    click.echo('Done')


aqhi_stations.add_command(add)
aqhi_stations.add_command(clean_index)
aqhi_stations.add_command(delete_index)
