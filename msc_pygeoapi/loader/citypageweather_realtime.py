# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>
#
# Copyright (c) 2020 Louis-Philippe Rousseau-Lambert
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

import click
from datetime import datetime, timedelta
import json
import logging
from lxml import etree
import os

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_BASEPATH
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# Index settings
INDEX_NAME = 'current_conditions'

NATIONAL_CITIES = [
    'Calgary',
    'Charlottetown',
    'Edmonton',
    'Fredericton',
    'Halifax',
    'Iqaluit',
    u'Montréal',
    u'Ottawa (Kanata - Orléans)',
    'Prince George',
    u'Québec',
    'Regina',
    'Saskatoon',
    'St. John\'s',
    'Thunder Bay',
    'Toronto',
    'Vancouver',
    'Victoria',
    'Whitehorse',
    'Winnipeg',
    'Yellowknife',
]

SETTINGS = {
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
                    'identifier': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'name': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'nom': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'station_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'stations_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'icon': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'cond_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'cond_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'temp': {
                        "type": "float"
                    },
                    'dewpoint': {
                        "type": "float"
                    },
                    'windchill': {
                        "type": "integer"
                    },
                    'pres_en': {
                        "type": "float"
                    },
                    'pres_fr': {
                        "type": "float"
                    },
                    'prestnd_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'prestnd_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'rel_hum': {
                        "type": "integer"
                    },
                    'speed': {
                        "type": "integer"
                    },
                    'gust': {
                        "type": "integer"
                    },
                    'direction': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'bearing': {
                        "type": "float"
                    },
                    'timestamp': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'url_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'url_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'national': {
                        'type': 'integer',
                    }
                }
            }
        }
    }
}


class CitypageweatherRealtimeLoader(BaseLoader):
    """Current conditions real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.conn.create(INDEX_NAME, mapping=SETTINGS)

    def load_data(self, filepath):
        """
        fonction from base to load the data in ES

        :param filepath: filepath for parsing the current condition file

        :returns: True/False
        """

        with open(os.path.join(MSC_PYGEOAPI_BASEPATH,
                               'resources/wxo_lookup.json')) as json_file:
            wxo_lookup = json.load(json_file)

        data = self.xml2json_cpw(wxo_lookup, filepath)

        if data:
            try:
                r = self.conn.Elasticsearch.update(
                    index=INDEX_NAME,
                    id=data['properties']['identifier'],
                    doc_as_upsert=True,
                    doc=data
                )
                LOGGER.debug(f'Result: {r}')
                return True
            except Exception as err:
                LOGGER.warning(f'Error indexing: {err}')
                return False

    def _get_element(self, node, path, attrib=None):
        """
        Convenience function to resolve lxml.etree.Element handling

        :param node: xml node
        :param path: path in the xml node
        :param attrib: attribute to get in the node

        returns: attribute as text or None
        """

        val = node.find(path)
        if attrib is not None and val is not None:
            return val.attrib.get(attrib)
        if hasattr(val, 'text') and val.text not in [None, '']:
            return val.text
        return None

    def if_none(self, type_, value):
        """
        Convenience fonction to avoid errors when
        converting to int or float

        :param type_: f for float and i for int
        :param value: value to convert to float/int

        :returns: converted variable
        """

        try:
            if type_ == 'f':
                variable = float(value) if value else 'null'
            elif type_ == 'i':
                variable = int(value) if value else 'null'
        except ValueError:
            variable = value

        return variable

    def xml2json_cpw(self, wxo_lookup, xml):
        """
        main for generating weather data

        :param wxo_lookup: json file to have the city id
        :param xml: xml file to parse and convert to json

        :returns: xml as json object
        """

        feature = {}
        row = {}

        LOGGER.debug(f'Processing XML: {xml}')
        LOGGER.debug('Fetching English elements')

        try:
            root = etree.parse(xml).getroot()
        except Exception as err:
            LOGGER.error(f'ERROR: cannot process data: {err}')
            return None

        if root.findall("currentConditions/"):
            sitecode = os.path.basename(xml)[:-6]
            try:
                citycode = wxo_lookup[sitecode]['citycode']
            except KeyError as err:
                LOGGER.error(f'ERROR: cannot find sitecode {sitecode}: {err}')

            location_name = root.find('location/name')
            x = float(location_name.attrib.get('lon')[:-1])
            y = float(location_name.attrib.get('lat')[:-1])

            if location_name.attrib.get('lat')[-1] == 'S':
                y *= -1  # south means negative latitude
            elif location_name.attrib.get('lon')[-1] in ['W', 'O']:
                x *= -1  # west means negative longitude

            feature['geom'] = [x, y, 0.0]
            icon = self._get_element(root, 'currentConditions/iconCode')

            if icon:
                row['icon'] = f'https://weather.gc.ca/weathericons/{icon}.gif'
            else:
                row['icon'] = None

            for dates in root.findall("currentConditions/dateTime"
                                      "[@zone='UTC'][@name='observation']"):
                timestamp = dates.find('timeStamp')
                if timestamp is not None:
                    dt2 = datetime.strptime(timestamp.text, '%Y%m%d%H%M%S')
                    row['timestamp'] = dt2.strftime('%Y-%m-%dT%H:%M:%SZ')

            row['rel_hum'] = self._get_element(
                              root,
                              'currentConditions/relativeHumidity')
            row['speed'] = self._get_element(root,
                                             'currentConditions/wind/speed')
            row['gust'] = self._get_element(root,
                                            'currentConditions/wind/gust')
            row['direction'] = self._get_element(
                                root,
                                'currentConditions/wind/direction')
            row['bearing'] = self._get_element(
                              root, 'currentConditions/wind/bearing')
            row['temp'] = self._get_element(
                           root, 'currentConditions/temperature')
            row['dewpoint'] = self._get_element(
                               root, 'currentConditions/dewpoint')
            row['windchill'] = self._get_element(
                                root, 'currentConditions/windChill')

            if xml.endswith('e.xml'):
                row['name'] = self._get_element(root, 'location/name')
                row['station_en'] = self._get_element(
                                     root, 'currentConditions/station')
                row['cond_en'] = self._get_element(
                                  root, 'currentConditions/condition')
                row['pres_en'] = self._get_element(
                                  root, 'currentConditions/pressure')
                row['prestnd_en'] = self._get_element(
                                     root,
                                     'currentConditions/pressure',
                                     'tendency')
                row['url_en'] = f'https://weather.gc.ca/city/pages/{citycode}_metric_e.html'  # noqa

                row['national'] = 0
                if row['name'] in NATIONAL_CITIES:
                    row['national'] = 1

                LOGGER.debug('Adding feature')
                LOGGER.debug('Setting geometry')

                conditions = {
                    'type': "Feature",
                    'properties': {
                        'identifier': citycode,
                        'name': row['name'],
                        'station_en': row['station_en'],
                        'icon': row['icon'],
                        'cond_en': row['cond_en'],
                        'temp': self.if_none('f', row['temp']),
                        'dewpoint': self.if_none('f', row['dewpoint']),
                        'windchill': self.if_none('i', row['windchill']),
                        'pres_en': self.if_none('f', row['pres_en']),
                        'prestnd_en': row['prestnd_en'],
                        'rel_hum': self.if_none('i', row['rel_hum']),
                        'speed': self.if_none('i', row['speed']),
                        'gust': self.if_none('i', row['gust']),
                        'direction_en': row['direction'],
                        'bearing': self.if_none('f', row['bearing']),
                        'timestamp': row['timestamp'],
                        'url_en': row['url_en'],
                        'national': int(row['national'])
                    },
                    'geometry': {
                        'type': "Point",
                        'coordinates': feature['geom']
                    }
                }

            elif xml.endswith('f.xml'):
                LOGGER.debug(f'Processing {xml}')

                row['nom'] = self._get_element(root, 'location/name')
                row['station_fr'] = self._get_element(
                                     root, 'currentConditions/station')
                row['cond_fr'] = self._get_element(
                                  root, 'currentConditions/condition')
                row['pres_fr'] = self._get_element(
                                  root, 'currentConditions/pressure')
                row['prestnd_fr'] = self._get_element(
                                     root,
                                     'currentConditions/pressure',
                                     'tendency')
                row['url_fr'] = f'https://meteo.gc.ca/city/pages/{citycode}_metric_f.html'  # noqa

                row['national'] = 0
                if row['nom'] in NATIONAL_CITIES:
                    row['national'] = 1

                LOGGER.debug('Adding feature')
                LOGGER.debug('Setting geometry')

                conditions = {
                    'type': "Feature",
                    'properties': {
                        'identifier': citycode,
                        'nom': row['nom'],
                        'station_fr': row['station_fr'],
                        'icon': row['icon'],
                        'cond_fr': row['cond_fr'],
                        'temp': self.if_none('f', row['temp']),
                        'dewpoint': self.if_none('f', row['dewpoint']),
                        'windchill': self.if_none('i', row['windchill']),
                        'pres_fr': self.if_none('f', row['pres_fr']),
                        'prestnd_fr': row['prestnd_fr'],
                        'rel_hum': self.if_none('i', row['rel_hum']),
                        'speed': self.if_none('i', row['speed']),
                        'gust': self.if_none('i', row['gust']),
                        'direction_fr': row['direction'],
                        'bearing': self.if_none('f', row['bearing']),
                        'timestamp': row['timestamp'],
                        'url_fr': row['url_fr'],
                        'national': int(row['national'])},
                    'geometry': {
                        'type': "Point",
                        'coordinates': feature['geom']
                    }
                }

            conditions['properties'] = {key:val for key, val in conditions['properties'].items() if val != 'null'} # noqa
            return conditions

        else:
            LOGGER.warning(
                f'No current conditions found. Skippping file {xml}.'
            )
            return None


@click.group()
def citypageweather():
    """Manages current conditions index"""
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
    """adds data to system"""

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
        loader = CitypageweatherRealtimeLoader(conn_config)
        loader.load_data(file_to_process)

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help=f'Delete documents older than n days (default={DAYS_TO_KEEP})'
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old documents?'
)
def clean_records(ctx, days, es, username, password, ignore_certs):
    """Delete old citypageweather documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    older_than = (datetime.now() - timedelta(days=days)).strftime(
        '%Y-%m-%d %H:%M')
    click.echo(f'Deleting documents older than {older_than} ({days} days)')

    query = {
        'query': {
            'range': {
                'properties.datetime': {
                    'lte': older_than
                }
            }
        }
    }

    conn.Elasticsearch.delete_by_query(index=INDEX_NAME, body=query)


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs):
    """Delete current conditions index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    conn.delete(INDEX_NAME)


citypageweather.add_command(add)
citypageweather.add_command(clean_records)
citypageweather.add_command(delete_index)
