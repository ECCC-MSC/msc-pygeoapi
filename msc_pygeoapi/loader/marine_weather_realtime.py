# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
#
# Copyright (c) 2020 Etienne Pelletier
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

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path

import click
from elasticsearch import exceptions
from lxml import etree
from parse import parse

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_BASEPATH
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    strftime_rfc3339
)

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'marine_weather_{}'

FORECAST_POLYGONS_WATER_ES_INDEX = 'forecast_polygons_water_hybrid'

MAPPINGS = {
    'regular-forecasts': {
        'issued_datetime_utc': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'issued_datetime_local': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'area_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'area_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'sub_region_e': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'sub_region_f': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'forecasts_e': {
            'type': 'nested',
            'properties': {
                'location_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'period_of_coverage_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'wind_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'weather_visibility_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'air_temperature_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'freezing_spray_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'status_statement_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
            },
        },
        'forecasts_f': {
            'type': 'nested',
            'properties': {
                'location_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'period_of_coverage_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'wind_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'weather_visibility_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'air_temperature_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'freezing_spray_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'status_statement_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
            },
        },
    },
    'extended-forecasts': {
        'issued_datetime_utc': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'issued_datetime_local': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'area_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'area_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'sub_region_e': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'sub_region_f': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'extended_forecasts_e': {
            'type': 'nested',
            'properties': {
                'location_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'status_statement_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'forecast_periods_e': {
                    'type': 'nested',
                    'properties': {
                        'forecast_period_e': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}},
                        },
                        'forecast_e': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}},
                        },
                    },
                },
            },
        },
        'extended_forecasts_f': {
            'type': 'nested',
            'properties': {
                'location_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'status_statement_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'forecast_periods_f': {
                    'type': 'nested',
                    'properties': {
                        'forecast_period_f': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}},
                        },
                        'forecast_f': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}},
                        },
                    },
                },
            },
        },
    },
    'warnings': {
        'area_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'area_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_e': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'region_f': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}},
        'sub_region_e': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'sub_region_f': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'warnings_e': {
            'type': 'nested',
            'properties': {
                'location_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'issued_datetime_utc_e': {
                    'type': 'date',
                    'format': 'date_time_no_millis',
                    'ignore_malformed': False,
                },
                'issued_datetime_local_e': {
                    'type': 'date',
                    'format': 'date_time_no_millis',
                    'ignore_malformed': False,
                },
                'event_type_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_category_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_name_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_status_e': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
            },
        },
        'warnings_f': {
            'type': 'nested',
            'properties': {
                'location_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'issued_datetime_utc_f': {
                    'type': 'date',
                    'format': 'date_time_no_millis',
                    'ignore_malformed': False,
                },
                'issued_datetime_local_f': {
                    'type': 'date',
                    'format': 'date_time_no_millis',
                    'ignore_malformed': False,
                },
                'event_type_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_category_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_name_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
                'event_status_f': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}},
                },
            },
        },
    },
}

SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {'properties': None},
        }
    },
}

INDICES = [INDEX_NAME.format(weather_var) for weather_var in MAPPINGS]


class MarineWeatherRealtimeLoader(BaseLoader):
    """Marine weather real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filename_pattern = '{datetime}_MSC_MarineWeather_{region_name_code}_{lang}.xml'  # noqa
        self.parsed_filename = None
        self.region_name_code = None
        self.lang = None
        self.root = None
        self.area = {}
        self.items = []

        # create marine weather indices if it don't exist
        for item in MAPPINGS:
            SETTINGS['mappings']['properties']['properties'][
                'properties'
            ] = MAPPINGS[item]
            self.conn.create(INDEX_NAME.format(item), SETTINGS)

    def parse_filename(self):
        """
        Parses a marine weather forecast XML filename to get the
        region name code and language.
        :return: `bool` of parse status
        """
        # parse filepath
        filename = self.filepath.name
        parsed_filename = parse(self.filename_pattern, filename)
        # set class variables
        self.region_name_code = parsed_filename.named['region_name_code']
        self.lang = parsed_filename.named['lang']

        return True

    def create_datetime_dict(self, datetime_elems):
        """
        Used to pass a pair of timeStamp elements from the XML. These elements
        contain the UTC and local time for various marine forecast
        sections (warnings, regular forecasts, extended forecasts). The
        first element contains UTC datetime info and the second local datetime
        info.
        :param datetime_elems: list of lmxl `Element` objects representing the
        dateTime nodes to parse.
        :returns: `dict` with "utc" and "local" keys containing respective
        parsed datetime objects.
        """
        datetime_utc = datetime.strptime(
            datetime_elems[0].find('timeStamp').text, '%Y%m%d%H%M'
        )
        local_offset = float(datetime_elems[1].attrib['UTCOffset'])

        datetime_local = datetime_utc + timedelta(hours=local_offset)
        datetime_local = datetime_local.replace(
            tzinfo=timezone(timedelta(hours=local_offset))
        )

        return {'utc': datetime_utc, 'local': datetime_local}

    def set_area_info(self):
        """
        Gets the area name from the marine weather XML document and
        looks up the equivalent meteocode forecast polygon feature ID to
        query the forecast_polygons_water ES index for the corresponding
        document. If document is found, assigns the self.area class attribute
        that contains region name, subregion name, area name and the
        associated geometry.
        :return: `bool` representing successful setting of self.area attribute
        """
        area_name = self.root.find('area').text

        with open(
            os.path.join(
                MSC_PYGEOAPI_BASEPATH, 'resources/meteocode_lookup.json',
            )
        ) as json_file:
            meteocode_lookup = json.load(json_file)
            forecast_id = meteocode_lookup[self.region_name_code]

        try:
            result = self.conn.Elasticsearch.get(
                index=FORECAST_POLYGONS_WATER_ES_INDEX,
                id=forecast_id,
                _source=['geometry'],
            )
            self.area = {
                # get area element value
                **{'name': area_name},
                # get area element attribute values
                **{
                    key: self.root.find('area').attrib[key]
                    for key in ['countryCode', 'region', 'subRegion']
                },
                **result['_source'],
            }

            return True

        except exceptions.NotFoundError:
            LOGGER.warning(f'Could not get forecast polygon document with id: {forecast_id}')  # noqa

    def generate_warnings(self):
        """
        Generates and yields a series of marine weather warnings
        for a given marine weather area. Warnings are returned
        as Elasticsearch bulk API upsert actions, with a single
        document for the marine weather region in GeoJSON to match the
        Elasticsearch index mappings.
        :returns: Generator of Elasticsearch actions to upsert the marine
                  weather warnings.
        """
        warnings = self.root.findall('warnings/')

        feature = {'type': 'Feature', 'geometry': {}, 'properties': {}}

        feature['geometry'] = self.area['geometry']

        feature['properties'][f'area_{self.lang}'] = self.area['name']
        feature['properties'][f'region_{self.lang}'] = self.area['region']
        feature['properties'][
            f'sub_region_{self.lang}'
        ] = self.area['subRegion']
        feature['properties'][f'warnings_{self.lang}'] = []

        if len(warnings) > 0:
            for elem in warnings:
                datetimes = self.create_datetime_dict(
                    elem.findall('event/' 'dateTime')
                )
                location = {
                    f'location_{self.lang}': elem.attrib['name'],
                    f'issued_datetime_utc_{self.lang}': strftime_rfc3339(datetimes['utc']),  # noqa
                    f'issued_datetime_local_{self.lang}': strftime_rfc3339(datetimes['local']),  # noqa
                    f'event_type_{self.lang}': elem.find('event').attrib['type'],  # noqa
                    f'event_category_{self.lang}': elem.find('event').attrib['category'],  # noqa
                    f'event_name_{self.lang}': elem.find('event').attrib['name'],  # noqa
                    f'event_status_{self.lang}': elem.find('event').attrib['status']  # noqa
                }
                feature['properties'][f'warnings_{self.lang}'].append(location)  # noqa

        self.items.append(feature)

        action = {
            '_id': self.filepath.stem.split('_')[0],
            '_index': 'marine_weather_warnings',
            '_op_type': 'update',
            'doc': feature,
            'doc_as_upsert': True,
        }

        yield action

    def generate_regular_forecasts(self):
        """
        Generates and yields a series of marine weather regular forecasts
        for a given marine weather area. Each regular forecast is returned
        as Elasticsearch bulk API upsert actions, with documents in GeoJSON to
        match the Elasticsearch index mappings.
        :returns: Generator of Elasticsearch actions to upsert the marine
                  weather regular forecast.
        """
        regular_forecasts = self.root.findall('regularForecast/')
        feature = {'type': 'Feature', 'geometry': {}, 'properties': {}}

        feature['geometry'] = self.area['geometry']
        feature['properties'][f'area_{self.lang}'] = self.area[
            'name'
        ]
        feature['properties'][f'region_{self.lang}'] = self.area[
            'region'
        ]
        feature['properties'][
            f'sub_region_{self.lang}'
        ] = self.area['subRegion']
        feature['properties'][f'forecasts_{self.lang}'] = []

        if len(regular_forecasts) > 0:
            datetimes = self.create_datetime_dict(
                [
                    element
                    for element in regular_forecasts
                    if element.tag == 'dateTime'
                ]
            )
            feature['properties']['issued_datetime_utc'] = strftime_rfc3339(
                datetimes['utc']
            )
            feature['properties']['issued_datetime_local'] = strftime_rfc3339(
                datetimes['local']
            )

            locations = [
                element
                for element in regular_forecasts
                if element.tag == 'location'
            ]
            for location in locations:
                location = {
                    f'location_{self.lang}': location.attrib['name']
                    if 'name' in location.attrib else self.area['name'],
                    f'period_of_coverage_{self.lang}':
                    location.find('weatherCondition/periodOfCoverage').text
                    if location.find('weatherCondition/periodOfCoverage')
                    is not None else None,
                    f'wind_{self.lang}': location.find(
                        'weatherCondition/wind'
                    ).text
                    if location.find('weatherCondition/wind') is not None
                    else None,
                    f'weather_visibility_{self.lang}':
                    location.find('weatherCondition/weatherVisibility').text
                    if location.find('weatherCondition/weatherVisibility')
                    is not None else None,
                    f'air_temperature_{self.lang}': location.find(
                        'weatherCondition/airTemperature'
                    ).text
                    if location.find('weatherCondition/airTemperature')
                    is not None else None,
                    f'freezing_spray_{self.lang}': location.find(
                        'weatherCondition/freezingSpray'
                    ).text
                    if location.find('weatherCondition/freezingSpray')
                    is not None else None,
                    f'status_statement_{self.lang}': location.find(
                        'statusStatement'
                    ).text
                    if location.find('statusStatement') is not None else None
                }
                feature['properties'][f'forecasts_{self.lang}'].append(location)  # noqa

        self.items.append(feature)

        action = {
            '_id': self.filepath.stem.split('_')[0],
            '_index': 'marine_weather_regular-forecasts',
            '_op_type': 'update',
            'doc': feature,
            'doc_as_upsert': True,
        }

        yield action

    def generate_extended_forecasts(self):
        """
        Generates and yields a series of marine weather extended forecasts
        for a given marine weather area. Each extended forecast is returned
        as Elasticsearch bulk API upsert actions, with documents in GeoJSON to
        match the Elasticsearch index mappings.
        :returns: Generator of Elasticsearch actions to upsert the marine
                  weather extended forecast.
        """
        extended_forecasts = self.root.findall('extendedForecast/')
        feature = {'type': 'Feature', 'geometry': {}, 'properties': {}}

        feature['geometry'] = self.area['geometry']
        feature['properties'][f'area_{self.lang}'] = self.area['name']
        feature['properties'][f'region_{self.lang}'] = self.area['region']
        feature['properties'][f'sub_region_{self.lang}'] = self.area['subRegion']  # noqa
        feature['properties'][f'extended_forecasts_{self.lang}'] = []

        if len(extended_forecasts) > 0:
            datetimes = self.create_datetime_dict(
                [
                    element
                    for element in extended_forecasts
                    if element.tag == 'dateTime'
                ]
            )
            feature['properties']['issued_datetime_utc'] = strftime_rfc3339(
                datetimes['utc']
            )
            feature['properties']['issued_datetime_local'] = strftime_rfc3339(
                datetimes['local']
            )

            locations = [
                element
                for element in extended_forecasts
                if element.tag == 'location'
            ]
            for location in locations:
                location = {
                    f'location_{self.lang}': location.attrib['name']
                    if 'name' in location.attrib
                    else self.area['name'],
                    f'forecast_periods_{self.lang}': [
                        {
                            f'forecast_period_{self.lang}':
                            forecast_period.attrib['name'],
                            f'forecast_{self.lang}':
                            forecast_period.text,
                        }
                        for forecast_period in location.findall(
                            'weatherCondition/'
                        )
                        if location.findall('weatherCondition/') is not None
                    ],
                    f'status_statement_{self.lang}': location.find(
                        'statusStatement'
                    ).text
                    if location.find('statusStatement') is not None
                    else None,
                }
                feature['properties'][
                    f'extended_forecasts_{self.lang}'].append(location)

        self.items.append(feature)

        action = {
            '_id': self.filepath.stem.split('_')[0],
            '_index': 'marine_weather_extended-forecasts',
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
        self.parse_filename()

        LOGGER.debug(f'Received file {self.filepath}')

        self.root = etree.parse(str(self.filepath.resolve())).getroot()

        # set area info for both languages from XML
        self.set_area_info()

        warnings = self.generate_warnings()
        regular_forecasts = self.generate_regular_forecasts()
        extended_forecasts = self.generate_extended_forecasts()

        for package in [warnings, regular_forecasts, extended_forecasts]:
            self.conn.submit_elastic_package(package, request_size=80000)
        return True


@click.group()
def marine_weather():
    """Manages marine weather warnings/forecasts indices"""
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
            for f in [file for file in files if file.endswith('.xml')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = MarineWeatherRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_INDEX_NAME(type=click.Choice(INDICES))
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def delete_index(ctx, index_name, es, username, password, ignore_certs):
    """
    Delete a particular ES index with a given name as argument or all if no
    argument is passed
    """

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if index_name:
        if click.confirm(
            'Are you sure you want to delete ES index named: {}?'.format(
                click.style(index_name, fg='red')
            ),
            abort=True,
        ):
            LOGGER.info(f'Deleting ES index {index_name}')
            conn.delete(index_name)
            return True
    else:
        if click.confirm(
            'Are you sure you want to delete {} marine forecast'
            ' indices ({})?'.format(
                click.style('ALL', fg='red'),
                click.style(", ".join(INDICES), fg='red'),
            ),
            abort=True,
        ):
            conn.delete(",".join(INDICES))
            return True


marine_weather.add_command(add)
marine_weather.add_command(delete_index)
