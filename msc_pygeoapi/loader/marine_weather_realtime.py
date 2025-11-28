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
import re

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
    DATETIME_RFC3339_FMT,
    safe_cast_to_number,
    strftime_rfc3339
)

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'marine_weather_realtime'

FORECAST_POLYGONS_WATER_ES_INDEX = 'forecast_polygons_water_hybrid'

MAPPING = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'area': {
                    'properties': {
                        'countryCode': {
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}},
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}}
                                }
                            }
                        },
                        'region': {
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}},
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}}
                                }
                            }
                        },
                        'subRegion': {
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}},
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'keyword': {'type': 'keyword'}}
                                }
                            }
                        }
                    }
                },
                'regularForecast': {
                    'properties': {
                        'issuedDatetimeUTC': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'issuedDatetimeLocal': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'locations': {
                            'type': 'nested',
                            'properties': {
                                'weatherCondition': {
                                    'type': 'object',
                                    'properties': {
                                        'periodOfCoverage': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'wind': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'weatherVisibility': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'airTemperature': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'freezingSpray': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                'statusStatement': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'extendedForecast': {
                    'properties': {
                        'issuedDatetimeUTC': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'issuedDatetimeLocal': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'locations': {
                            'type': 'nested',
                            'properties': {
                                'weatherCondition': {
                                    'type': 'object',
                                    'properties': {
                                        'forecastPeriods': {
                                            'type': 'nested',
                                            'properties': {
                                                'name': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'keyword': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'keyword': {
                                                                    'type': 'keyword' # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                'value': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'keyword': {
                                                                    'type': 'keyword' # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'keyword': {
                                                                    'type': 'keyword' # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                'statusStatement': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'waveForecast': {
                    'properties': {
                        'issuedDatetimeUTC': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'issuedDatetimeLocal': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'locations': {
                            'type': 'nested',
                            'properties': {
                                'weatherCondition': {
                                    'type': 'object',
                                    'properties': {
                                        'periodOfCoverage': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'textSummary': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'warnings': {
                    'properties': {
                        'locations': {
                            'type': 'nested',
                            'properties': {
                                'events': {
                                    'type': 'nested',
                                    'properties': {
                                        'type': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'category': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'name': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'status': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                'name': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            },
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'keyword': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': MAPPING
}

MAX_XML_DATETIME_DIFF_SECONDS = 10


class MarineWeatherRealtimeLoader(BaseLoader):
    """Marine weather real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filename_pattern = '{datetime}_MSC_MarineWeather_{region_name_code}_{lang}.xml'  # noqa
        self.filepath_en = None
        self.filepath_fr = None
        self.parsed_filename = parse(self.filename_pattern, '')
        self.region_name_code = None
        self.lang = None
        self.xml_roots = {
            'en': etree.Element('root'),
            'fr': etree.Element('root'),
        }
        self.area = {}
        self.items = []
        self.marine_weather_feature = {
            'type': "Feature",
            'properties': {
                'lastUpdated': datetime.now().strftime(DATETIME_RFC3339_FMT)
            }
        }

        # create marine weather indices if it don't exist
        self.conn.create(INDEX_NAME, SETTINGS)

    def _sort_by_datetime_diff(self, file):
        """
        Sort files by absolute datetime difference between filename and
        parsed datetime in active file

        :param file: `Path` object
        :returns: `timedelta` object
        """

        return abs(
            datetime.strptime(
                self.parsed_filename.named['datetime'], '%Y%m%dT%H%M%S.%fZ'
            )
            - datetime.strptime(
                parse(self.filename_pattern, file.name).named['datetime'],
                '%Y%m%dT%H%M%S.%fZ'
            )
        )

    def _node_to_dict(self, node, lang=None):
        """
        Convert an lxml.etree.Element to a dict

        :param node: `lxml.etree.Element` node

        :returns: `dict` representation of xml node
        """

        if node is not None:
            # if node has no attributes, just return the text
            if not node.attrib and node.text:
                if lang:
                    return {lang: safe_cast_to_number(node.text)}
                else:
                    return safe_cast_to_number(node.text)
            else:
                node_dict = {}
                for attrib in node.attrib:
                    if node.attrib[attrib]:
                        # in some case node attributes contain datetime strings
                        # formatted as YYYYMMDDHHMMSS, in this case we
                        # want to convert them to RFC3339
                        regex = r"^(?:[2][0-9]{3})(?:(?:0[1-9]|1[0-2]))(?:(?:0[1-9]|[12]\d|3[01]))(?:(?:[01]\d|2[0-3]))(?:[0-5]\d){2}$"  # noqa
                        if re.match(regex, node.attrib[attrib]):
                            dt = datetime.strptime(
                                node.attrib[attrib], '%Y%m%d%H%M%S'
                            )
                            if lang:
                                node_dict[attrib] = {
                                    lang: dt.strftime(DATETIME_RFC3339_FMT)
                                }
                            else:
                                node_dict[attrib] = dt.strftime(
                                    DATETIME_RFC3339_FMT
                                )
                        elif lang:
                            node_dict[attrib] = {
                                lang: safe_cast_to_number(node.attrib[attrib])
                            }
                        else:
                            node_dict[attrib] = safe_cast_to_number(
                                node.attrib[attrib]
                            )

            if node.text and node.text.strip():
                if lang:
                    node_dict['value'] = {lang: safe_cast_to_number(node.text)}
                else:
                    node_dict['value'] = safe_cast_to_number(node.text)

            return node_dict

        return None

    def _deep_merge(self, d1, d2):
        """
        Deep merge two dictionaries
        :param d1: `dict` to merge into
        :param d2: `dict` to merge from

        :returns: `dict` of merged dictionaries
        """
        for key in d2:
            if key in d1:
                if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                    self._deep_merge(d1[key], d2[key])
                else:
                    d1[key] = d2[key]
            else:
                d1[key] = d2[key]
        return d1

    def _set_nested_value(self, d, keys, value):
        """
        Set nested value in dictionary, and merges dictionaries if they
        already exist at path
        :param d: `dict` to set value in
        :param keys: `list` of keys
        :param value: value to set

        :returns: `dict` of modified dictionary
        """
        for key in keys[:-1]:
            d = d.setdefault(key, {})

        if keys[-1] in d:
            # try to merge dictionaries
            if isinstance(value, dict):
                for k, v in value.items():
                    if k in d[keys[-1]]:
                        if isinstance(v, dict):
                            d[keys[-1]][k] = self._deep_merge(
                                d[keys[-1]][k], v
                            )
                        else:
                            d[keys[-1]][k] = v
                    else:
                        d[keys[-1]][k] = v
            else:
                d[keys[-1]] = value
        else:
            d[keys[-1]] = value

        return d

    def _parse_filename(self):
        """
        Parses a marine weather forecast XML filename to get the
        region name code and language.
        :return: `bool` of parse status
        """
        # parse filepath
        filename = self.filepath.name
        self.parsed_filename = parse(self.filename_pattern, filename)
        # set class variables
        self.region_name_code = self.parsed_filename.named['region_name_code']
        self.lang = self.parsed_filename.named['lang']

        return True

    def _create_datetime_dict(self, datetime_elems):
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

    def _set_area_info(self):
        """
        Gets the area name from the marine weather XML document and
        looks up the equivalent meteocode forecast polygon feature ID to
        query the forecast_polygons_water ES index for the corresponding
        document. If document is found, assigns the self.area class attribute
        that contains region name, subregion name, area name and the
        associated geometry.
        :return: `bool` representing successful setting of self.area attribute
        """
        for lang in self.xml_roots:
            with open(
                os.path.join(
                    MSC_PYGEOAPI_BASEPATH,
                    'resources/meteocode_lookup.json',
                )
            ) as json_file:
                meteocode_lookup = json.load(json_file)
                forecast_id = meteocode_lookup[self.region_name_code]

            try:
                result = self.conn.Elasticsearch.get(
                    index=FORECAST_POLYGONS_WATER_ES_INDEX,
                    id=forecast_id,
                    _source=['geometry']
                )

                geometry = result['_source'].get('geometry')

                area = self._node_to_dict(
                    self.xml_roots[lang].find('area'), lang=lang
                )

                self._set_nested_value(
                    self.marine_weather_feature['properties'],
                    ['area'],
                    area
                )

                self.marine_weather_feature['geometry'] = geometry

            except exceptions.NotFoundError:
                LOGGER.warning(f'Could not get forecast polygon document with id: {forecast_id}')  # noqa
                return False
        return True

    def _set_warning(self):
        """
        Generates warnings for a given marine weather area. Warnings are
        added to the marine weather feature properties.
        :returns: marine weather feature `dict` with warnings added.
        """
        warnings = self.xml_root.findall('warnings/')

        if 'warnings' not in self.marine_weather_feature['properties']:
            self.marine_weather_feature['properties']['warnings'] = {
                "locations": []
            }

        if len(warnings) == 0:
            LOGGER.debug('No warnings found in XML.')
            return self.marine_weather_feature

        locations = [
            element for element in warnings if element.tag == 'location'
        ]
        existing_locations = self.marine_weather_feature['properties'][
            'warnings'
        ]['locations']

        for i, location_elem in enumerate(locations):
            if i < len(existing_locations):
                location_dict = existing_locations[i]
            else:
                location_dict = {'events': []}

            if location_elem.attrib.get('name'):
                self._set_nested_value(
                    location_dict,
                    ['name'],
                    {self.lang: location_elem.attrib['name']}
                )

            # iterate over location events
            events = location_elem.findall('event')
            for event_index, event_elem in enumerate(events):
                if event_index < len(location_dict['events']):
                    event_dict = location_dict['events'][event_index]
                else:
                    event_dict = {}

                for attrib in event_elem.attrib:
                    self._set_nested_value(
                        event_dict,
                        [attrib],
                        {self.lang: event_elem.attrib[attrib]}
                    )

                if event_index < len(location_dict['events']):
                    location_dict['events'][event_index] = event_dict
                else:
                    location_dict['events'].append(event_dict)

            if i < len(existing_locations):
                existing_locations[i] = location_dict
            else:
                existing_locations.append(location_dict)

        return self.marine_weather_feature

    def _set_regular_forecast(self):
        """
        Generates regular forecasts for a given marine weather area. Regular
        forecasts are added to the marine weather feature properties.
        :returns: marine weather feature `dict` with regular forecasts added.
        """
        regular_forecasts = self.xml_root.findall('regularForecast/')

        if 'regularForecast' not in self.marine_weather_feature['properties']:
            self.marine_weather_feature['properties']['regularForecast'] = {
                "locations": []
            }

        if len(regular_forecasts) == 0:
            LOGGER.debug('No regular forecasts found in XML.')
            return self.marine_weather_feature

        # Set issued datetimes
        datetimes = self._create_datetime_dict(
            [elem for elem in regular_forecasts if elem.tag == 'dateTime']
        )
        self.marine_weather_feature['properties']['regularForecast'][
            'issuedDatetimeUTC'
        ] = strftime_rfc3339(datetimes['utc'])
        self.marine_weather_feature['properties']['regularForecast'][
            'issuedDatetimeLocal'
        ] = datetimes['local'].isoformat()

        # iterate over regular forecast location elements
        locations = [
            elem for elem in regular_forecasts if elem.tag == 'location'
        ]
        existing_locations = self.marine_weather_feature['properties'][
            'regularForecast'
        ]['locations']

        for i, location_elem in enumerate(locations):
            if i < len(existing_locations):
                location_dict = existing_locations[i]
            else:
                location_dict = {}

            if location_elem.attrib.get('name'):
                self._set_nested_value(
                    location_dict,
                    ['name'],
                    location_elem.attrib['name']
                )

            # add weather condition elements
            weather_condition_paths = [
                'weatherCondition/periodOfCoverage',
                'weatherCondition/wind',
                'weatherCondition/weatherVisibility',
                'weatherCondition/airTemperature',
                'weatherCondition/freezingSpray'
            ]

            weather_dict = {}
            for path in weather_condition_paths:
                node = location_elem.find(path)
                if node is not None and node.text:
                    weather_dict[path.split('/')[-1]] = self._node_to_dict(
                        node, self.lang
                    )

            if weather_dict:
                self._set_nested_value(
                    location_dict, ['weatherCondition'], weather_dict
                )

            # add status statement
            status_statement_node = location_elem.find('statusStatement')
            if status_statement_node is not None and (
                status_statement_node.attrib or status_statement_node.text
            ):
                self._set_nested_value(
                    location_dict,
                    ['statusStatement'],
                    self._node_to_dict(status_statement_node, self.lang)
                )

            if i < len(existing_locations):
                existing_locations[i] = location_dict
            else:
                existing_locations.append(location_dict)

        return self.marine_weather_feature

    def _set_extended_forecast(self):
        """
        Generates extended forecasts for a given marine weather area. Extended
        forecasts are added to the marine weather feature properties.
        :returns: marine weather feature `dict` with extended forecasts added.
        """
        extended_forecasts = self.xml_root.findall('extendedForecast/')

        if 'extendedForecast' not in self.marine_weather_feature['properties']:
            self.marine_weather_feature['properties']['extendedForecast'] = {
                "locations": []
            }

        if len(extended_forecasts) == 0:
            LOGGER.debug('No extended forecasts found in XML.')
            return self.marine_weather_feature

        # set extended forecast issued datetimes
        datetimes = self._create_datetime_dict(
            [elem for elem in extended_forecasts if elem.tag == 'dateTime']
        )
        self.marine_weather_feature['properties']['extendedForecast'][
            'issuedDatetimeUTC'
        ] = strftime_rfc3339(datetimes['utc'])
        self.marine_weather_feature['properties']['extendedForecast'][
            'issuedDatetimeLocal'
        ] = datetimes['local'].isoformat()

        # iterate over extended forecast location elements
        locations = [
            elem for elem in extended_forecasts if elem.tag == 'location'
        ]

        for i, location_elem in enumerate(locations):
            existing_locations = self.marine_weather_feature['properties'][
                'extendedForecast'
            ]['locations']

            if i < len(existing_locations):
                location_dict = existing_locations[i]
            else:
                location_dict = {}

            if location_elem.attrib.get('name'):
                self._set_nested_value(
                    location_dict,
                    ['name'],
                    location_elem.attrib['name']
                )

            # add weatherCondition tags and forecastPeriod children
            weather_condition = location_elem.find('weatherCondition')
            if weather_condition is not None and len(weather_condition) > 0:
                # Get or initialize forecast periods list
                weather_condition_dict = location_dict.setdefault(
                    'weatherCondition', {}
                )
                forecast_periods = weather_condition_dict.setdefault(
                    'forecastPeriods', []
                )

                forecast_periods_elems = weather_condition.findall(
                    'forecastPeriod'
                )

                for period_index, forecast_period in enumerate(
                    forecast_periods_elems
                ):
                    period_dict = self._node_to_dict(
                        forecast_period, self.lang
                    )

                    if period_index < len(forecast_periods):
                        forecast_periods[period_index] = self._deep_merge(
                            forecast_periods[period_index], period_dict
                        )
                    else:
                        forecast_periods.append(period_dict)
                # update forecast periods dictionary
                self._set_nested_value(
                    location_dict,
                    ['weatherCondition', 'forecastPeriods'],
                    forecast_periods,
                )

            # add statusStatement
            status_statement_node = location_elem.find('statusStatement')
            if (
                status_statement_node is not None
                and len(status_statement_node) > 0
            ):
                self._set_nested_value(
                    location_dict,
                    ['statusStatement'],
                    self._node_to_dict(status_statement_node, self.lang)
                )

            if i < len(existing_locations):
                existing_locations[i] = location_dict
            else:
                existing_locations.append(location_dict)

        return self.marine_weather_feature

    def _set_wave_forecast(self):
        """
        Generates wave forecasts for a given marine weather area. Wave
        forecasts are added to the marine weather feature properties.
        :returns: marine weather feature `dict` with wave forecasts added.
        """
        wave_forecasts = self.xml_root.findall('waveForecast/')

        if len(wave_forecasts) == 0:
            LOGGER.debug('No wave forecast found in XML.')
            return self.marine_weather_feature

        if 'waveForecast' not in self.marine_weather_feature['properties']:
            self.marine_weather_feature['properties']['waveForecast'] = {
                "locations": []
            }

        # set extended forecast issued datetimes
        datetimes = self._create_datetime_dict(
            [elem for elem in wave_forecasts if elem.tag == 'dateTime']
        )
        self.marine_weather_feature['properties']['waveForecast'][
            'issuedDatetimeUTC'
        ] = strftime_rfc3339(datetimes['utc'])
        self.marine_weather_feature['properties']['waveForecast'][
            'issuedDatetimeLocal'
        ] = datetimes['local'].isoformat()

        # iterate over wave forecast location elements
        locations = [elem for elem in wave_forecasts if elem.tag == 'location']

        existing_locations = self.marine_weather_feature['properties'][
            'waveForecast'
        ]['locations']

        for i, location_elem in enumerate(locations):
            if i < len(existing_locations):
                location_dict = existing_locations[i]
            else:
                location_dict = {}

            if location_elem.attrib.get('name'):
                self._set_nested_value(
                    location_dict,
                    ['name'],
                    location_elem.attrib['name']
                )

            # add weather condition elements
            weather_condition_paths = [
                'weatherCondition/periodOfCoverage',
                'weatherCondition/textSummary'
            ]

            weather_dict = {}
            for path in weather_condition_paths:
                node = location_elem.find(path)
                if node is not None and node.text:
                    weather_dict[path.split('/')[-1]] = self._node_to_dict(
                        node, self.lang
                    )
            if weather_dict:
                self._set_nested_value(
                    location_dict, ['weatherCondition'], weather_dict
                )

            if i < len(existing_locations):
                existing_locations[i] = location_dict
            else:
                existing_locations.append(location_dict)

        return self.marine_weather_feature

    def xml2json_marine_weather(self):
        """
        main for generating marine weather json feature

        :returns: `dict` representing marine weather feature
        """

        self._set_area_info()

        for lang, xml_root in self.xml_roots.items():
            self.xml_root = xml_root
            self.lang = lang

            self._set_regular_forecast()
            self._set_extended_forecast()
            self._set_wave_forecast()
            self._set_warning()

        return self.marine_weather_feature

    def load_data(self, filepath):
        """
        loads data from event to target
        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)
        self._parse_filename()
        current_lang = self.parsed_filename.named['lang']
        alt_lang = 'fr' if current_lang == 'en' else 'en'

        # set current file language filepath
        setattr(self, f'filepath_{current_lang}', self.filepath)

        # construct alternate language filepath
        alt_xml_wildcard = self.filename_pattern.format(
            datetime='*', region_name_code=self.region_name_code, lang=alt_lang
        )

        associated_alt_files = sorted(
            self.filepath.parent.glob(alt_xml_wildcard),
            key=self._sort_by_datetime_diff
        )

        if associated_alt_files:
            # set alternate language filepath to closest datetime file
            setattr(self, f'filepath_{alt_lang}', associated_alt_files[0])
        else:
            LOGGER.warning(
                f'No associated {alt_lang} file found for '
                f'{self.filepath.name}'
            )
            return False

        LOGGER.debug(
            f'Processing XML: '
            f'{getattr(self, f"filepath_en")} and '
            f'{getattr(self, f"filepath_fr")}'
        )

        try:
            self.xml_roots = {
                'en': etree.parse(self.filepath_en).getroot(),
                'fr': etree.parse(self.filepath_fr).getroot()
            }
        except Exception as err:
            LOGGER.error(f'ERROR: cannot process data: {err}')
            return False

        xml_creation_dates = [
            datetime.strptime(
                self.xml_roots[key].find('dateTime/timeStamp').text,
                '%Y%m%d%H%M%S'
            )
            for key in self.xml_roots
        ]

        # calculate diff between the two nearest en/fr XML creation dates
        xml_creation_diff_seconds = abs(
            (xml_creation_dates[0] - xml_creation_dates[1]).total_seconds()
        )
        if xml_creation_diff_seconds > MAX_XML_DATETIME_DIFF_SECONDS:
            LOGGER.warning(
                'File creation times differ by more than '
                f'{MAX_XML_DATETIME_DIFF_SECONDS} seconds. '
                'Skipping loading...'
            )
            return False
        else:
            LOGGER.debug(
                f'File creation times differ by {xml_creation_diff_seconds} '
                'seconds. Proceeding...'
            )

        # populate self.marine_weather_feature
        self.xml2json_marine_weather()

        # load self.marine_weather_feature
        action = {
            '_id': self.region_name_code,
            '_index': INDEX_NAME,
            '_op_type': 'update',
            'doc': self.marine_weather_feature,
            'doc_as_upsert': True
        }

        try:
            self.conn.submit_elastic_package([action], refresh=True)
            return True
        except Exception as err:
            LOGGER.error(f'ERROR: cannot process data: {err}')
            return False


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
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def delete_index(ctx, es, username, password, ignore_certs):
    """
    Delete a particular ES index with a given name as argument or all if no
    argument is passed
    """

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if click.confirm(
        'Are you sure you want to delete ES index named: {}?'.format(
            click.style(INDEX_NAME, fg='red')
        ),
        abort=True
    ):
        LOGGER.info(f'Deleting ES index {INDEX_NAME}')
        conn.delete(INDEX_NAME)
        return True
    else:
        if click.confirm(
            'Are you sure you want to delete the marine forecast'
            ' index ({})?'.format(click.style(INDEX_NAME, fg='red')),
            abort=True
        ):
            conn.delete(INDEX_NAME)
            return True


marine_weather.add_command(add)
marine_weather.add_command(delete_index)
