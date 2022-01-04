# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
#
# Copyright (c) 2020 Etienne Pelletier
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
import os
from pathlib import Path

import click
from parse import parse
from osgeo import ogr

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'forecast_polygons_{}_{}'

FILE_PROPERTIES = {
    'water': {
        'F_MARSTDZA': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'POLY_ID': {
            'type': 'integer',
        },
        'PRIME_ID': {
            'type': 'integer',
        },
        'CLC': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'FEATURE_ID': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'NAME': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'},
                'normalize': {'type': 'keyword',
                              'normalizer': 'name_normalizer'}
            }
        },
        'NOM': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'},
                'normalize': {'type': 'keyword',
                              'normalizer': 'name_normalizer'}
            }
        },
        'PERIM_KM': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'AREA_KM2': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'LAT_DD': {
            'type': 'float'
        },
        'LON_DD': {
            'type': 'float'
        },
        'KIND': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'USAGE': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'DEPICTN': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'PROVINCE_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'COUNTRY_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'WATRBODY_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'version': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
    },
    'land': {
        'F_CLCBZA': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'POLY_ID': {
            'type': 'integer',
        },
        'PRIME_ID': {
            'type': 'integer',
        },
        'CLC': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'FEATURE_ID': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'NAME': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'},
                'normalize': {'type': 'keyword',
                              'normalizer': 'name_normalizer'}
            }
        },
        'NOM': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'},
                'normalize': {'type': 'keyword',
                              'normalizer': 'name_normalizer'}
            }
        },
        'PERIM_KM': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'AREA_KM2': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'LAT_DD': {
            'type': 'float'
        },
        'LON_DD': {
            'type': 'float'
        },
        'KIND': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'USAGE': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'DEPICTN': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'PROVINCE_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'COUNTRY_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'WATRBODY_C': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'version': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
    }
}

SETTINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'analysis': {
            'normalizer': {
                'name_normalizer': {
                    'type': 'custom',
                    'filter': [
                        'lowercase',
                        'asciifolding'
                    ],
                }
            }
        }
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'properties': {
                'properties': None
            }
        }
    }
}

INDICES = [
    INDEX_NAME.format(forecast_zone, detail_level)
    for forecast_zone in FILE_PROPERTIES
    for detail_level in ['coarse', 'detail']
]

SHAPEFILES_TO_LOAD = {
    'MSC_Geography_Pkg_V6_4_0_Water_Unproj':
        ['water_MarStdZone_coarse_unproj.shp',
         'water_MarStdZone_detail_unproj.shp'],
    'MSC_Geography_Pkg_V6_4_0_Land_Unproj':
        ['land_CLCBaseZone_coarse_unproj.shp',
         'land_CLCBaseZone_detail_unproj.shp']
}


class ForecastPolygonsLoader(BaseLoader):
    """Forecast polygons (land/water) loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = None
        self.version = None
        self.zone = None
        self.items = []

        # create forecast polygon indices if they don't exist
        for index in INDICES:
            zone = index.split('_')[2]
            SETTINGS['mappings']['properties']['properties'][
                'properties'
            ] = FILE_PROPERTIES[zone]
            self.conn.create(index, SETTINGS)

    def parse_filename(self):
        """
        Parses a meteocode filename in order to get the version,
        zone (land/water) and type (proj, unproj, kmz, etc.)
        :return: `bool` of parse status
        """
        # parse filepath
        pattern = 'MSC_Geography_Pkg_V{version:w}_{zone}_{type}.zip'
        filename = self.filepath.name
        parsed_filename = parse(pattern, filename)

        # set class variables
        self.version = parsed_filename.named['version'].replace('_', '.')
        self.zone = parsed_filename.named['zone']

        return True

    def generate_geojson_features(self, shapefile_name):
        """
        Generates and yields a series of meteocode geodata features,
        one for each feature in <self.filepath/self.filepath.stem/
        shapefile_name>. Features are returned as Elasticsearch bulk API
        upsert actions, with documents in GeoJSON to match the Elasticsearch
        index mappings.
        :returns: Generator of Elasticsearch actions to upsert the forecast
                  polygons for given shapefile in zip archive
        """
        filepath = str((self.filepath / self.filepath.stem /
                        shapefile_name).resolve())
        data = ogr.Open(r'/vsizip/{}'.format(filepath))
        lyr = data.GetLayer()

        for feature in lyr:
            feature_json = feature.ExportToJson(as_object=True,
                                                options=['RFC7946=YES'])
            feature_json['properties']['version'] = self.version

            _id = feature_json['properties']['FEATURE_ID']

            self.items.append(feature_json)

            action = {
                '_id': '{}'.format(_id),
                '_index': INDEX_NAME.format(self.zone.lower(),
                                            shapefile_name.split('_')[2]
                                            ),
                '_op_type': 'update',
                'doc': feature_json,
                'doc_as_upsert': True
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

        for shapefile in SHAPEFILES_TO_LOAD[self.filepath.stem]:
            # generate geojson features
            package = self.generate_geojson_features(shapefile)
            self.conn.submit_elastic_package(package, request_size=80000)

        return True


@click.group()
def forecast_polygons():
    """Manages forecast polygons indices"""
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

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.zip')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = ForecastPolygonsLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated: {}')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_NAME(
    type=click.Choice(INDICES),
)
def delete_indexes(ctx, index_name, es, username, password, ignore_certs):
    """
    Delete a particular ES index with a given name as argument or all if no
    argument is passed
    """

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if index_name:
        if click.confirm(
                'Are you sure you want to delete ES index named: {}?'.format(
                    click.style(index_name, fg='red')), abort=True):
            LOGGER.info('Deleting ES index {}'.format(index_name))
            conn.delete(index_name)
            return True
    else:
        if click.confirm(
                'Are you sure you want to delete {} forecast polygon'
                ' indices ({})?'.format(click.style('ALL', fg='red'),
                                        click.style(", ".join(INDICES),
                                                    fg='red')),
                abort=True):
            conn.delete(",".join(INDICES))
            return True


forecast_polygons.add_command(add)
forecast_polygons.add_command(delete_indexes)
