# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>
#
# Copyright (c) 2020 Louis-Philippe Rousseau-Lambert
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
import logging
from lxml import etree
import os
import re

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    _get_date_format,
    _get_element
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# Alerts by increasing severity
ALERTS_LEVELS = ['advisory', 'statement', 'watch', 'warning']

# Index settings
INDEX_NAME = 'cap_alerts'

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
                    'area': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'reference': {
                        'type': 'keyword',
                        'index': 'true'
                    },
                    'zone': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'headline': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'titre': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'descrip_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'descrip_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'effective': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss'Z'"
                    },
                    'expires': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss'Z'"
                    },
                    'alert_type': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'status': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'references': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'url': {
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
}


class CapAlertsRealtimeLoader(BaseLoader):
    """Cap Alerts real-time loader"""

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

        data = self.weather_warning2geojson(filepath)

        try:
            self.bulk_data = []
            for doc in data:
                op_dict = {
                    'index': {
                        '_index': INDEX_NAME,
                        '_type': '_doc'
                    }
                }
                op_dict['index']['_id'] = doc['properties']['identifier']
                self.bulk_data.append(op_dict)
                self.bulk_data.append(doc)
            r = self.conn.Elasticsearch.bulk(
                index=INDEX_NAME, body=self.bulk_data
            )

            LOGGER.debug('Result: {}'.format(r))

            previous_alerts = self.delete_references_alerts()

            click.echo('done importing in ES')

            if previous_alerts:
                LOGGER.debug('Deleted old warning')
            else:
                LOGGER.debug('New warning, no deletion')
            return True

        except Exception as err:
            LOGGER.warning('Error bulk indexing: {}'.format(err))
            return False

    def delete_references_alerts(self):
        """Delete old alerts documents"""

        if self.references_arr and len(self.references_arr) != 0:

            click.echo('Deleting old alerts')

            query = {
                'query': {
                    'terms': {
                        'properties.reference': self.references_arr
                    }
                }
            }

            self.conn.Elasticsearch.delete_by_query(
                index=INDEX_NAME, body=query
            )

            return True

        else:
            return False

    def weather_warning2geojson(self, filepath):
        """
        Create GeoJSON that will be use to display weather alerts

        :param filepath: filepath to the cap-xml file

        :returns: xml as json object
        """

        # we must define the variable that we'll need
        now = datetime.utcnow()

        french_alert = {}
        english_alert = {}
        english_alert_remove = []

        timeformat = '%Y-%m-%dT%H:%M:%SZ'

        # we want to run a loop on every cap-xml in filepath and add them
        # in the geojson
        # we want to strat by the newest file in the directory
        LOGGER.info('Processing {} CAP documents'.format(len(filepath)))

        LOGGER.debug('Processing {}'.format(filepath))
        # with the lxml library we parse the xml file
        try:
            tree = etree.parse(filepath)
        except Exception as err:
            LOGGER.warning('Cannot parse {}: {}'.format(filepath, err))

        url = 'https://dd.weather.gc.ca/alerts/{}'.\
            format(filepath.split('alerts')[1])

        root = tree.getroot()

        b_xml = '{urn:oasis:names:tc:emergency:cap:1.2}'

        identifier = _get_element(root,
                                  '{}identifier'.format(b_xml))
        references = _get_element(root,
                                  '{}references'.format(b_xml)).split(' ')
        self.references_arr = []
        for ref in references:
            self.references_arr.append(ref.split(',')[1])

        for grandchild in root.iter('{}info'.format(b_xml)):
            expires = _get_date_format(_get_element(grandchild,
                                       '{}expires'.format(b_xml)))\
                      .strftime(timeformat)

            status_alert = _get_element(grandchild,
                                        '{}parameter[last()-4]/'
                                        '{}value'.format(b_xml,
                                                         b_xml))

            if _get_date_format(expires) > now:
                language = _get_element(grandchild,
                                        '{}language'.format(b_xml))
                if language == 'fr-CA':
                    headline = _get_element(grandchild,
                                            '{}headline'.format(b_xml))

                    description_fr = '{}description'.format(b_xml)
                    descript = _get_element(grandchild, description_fr)
                    descript = descript.replace("\n", " ").strip()

                    for i in grandchild.iter('{}area'.format(b_xml)):
                        tag = _get_element(i,
                                           '{}polygon'.format(b_xml))
                        name = _get_element(i,
                                            '{}areaDesc'.format(b_xml))

                        for j in grandchild.iter('{}geocode'.format(b_xml)):
                            str_value_name = '{}valueName'.format(b_xml)
                            valueName = _get_element(j, str_value_name)

                            if valueName == 'layer:EC-MSC-SMC:1.0:CLC':
                                geocode_value = '{}value'.format(b_xml)
                                geocode = _get_element(j, geocode_value)

                        id_warning = '{}_{}'.format(identifier, geocode)

                        if id_warning not in french_alert:
                            french_alert[id_warning] = (id_warning,
                                                        name,
                                                        headline,
                                                        descript)
                else:
                    headline = _get_element(grandchild,
                                            '{}headline'.format(b_xml))

                    description = '{}description'.format(b_xml)
                    descript = _get_element(grandchild, description)
                    descript = descript.replace("\n", " ").strip()

                    effective_date =\
                        _get_element(grandchild,
                                     '{}effective'.format(b_xml))
                    effective = _get_date_format(effective_date)
                    effective = effective.strftime(timeformat)

                    warning = _get_element(grandchild,
                                           '{}parameter[1]/'
                                           '{}value'.format(b_xml,
                                                            b_xml))

                    # There can be many <area> cobvered by one
                    #  <info> so we have to loop through the info
                    for i in grandchild.iter('{}area'.format(b_xml)):
                        tag = _get_element(i, '{}polygon'.format(b_xml))
                        name = _get_element(i, '{}areaDesc'.format(b_xml))

                        for j in grandchild.iter('{}geocode'.format(b_xml)):
                            valueName = \
                                _get_element(j, '{}valueName'.format(b_xml))
                            if valueName == 'layer:EC-MSC-SMC:1.0:CLC':
                                geocode = \
                                    _get_element(j, '{}value'.format(b_xml))

                        split_tag = re.split(' |,', tag)

                        id_warning = '{}_{}'.format(identifier, geocode)

                        if id_warning not in english_alert:
                            english_alert[id_warning] = (split_tag,
                                                         name,
                                                         headline,
                                                         effective,
                                                         expires,
                                                         warning,
                                                         status_alert,
                                                         id_warning,
                                                         descript,
                                                         url)

        LOGGER.info('Done processing')
        for j in english_alert:
            if _get_date_format(english_alert[j][4]) < now:
                english_alert_remove.append(j)
                # We can't remove a element of a dictionary while looping in it
                # So we remove the warning in another step
        for key in english_alert_remove:
            del english_alert[key]
            del french_alert[key]

        # To keep going we want to have the same number of warning
        # in english and in french
        if len(french_alert) == len(english_alert):
            LOGGER.info('Creating %d features', len(english_alert))

            data = []
            for num_poly in english_alert:
                poly = []
                for el in list(reversed(range(0,
                                              len(english_alert[num_poly][0]),
                                              2))):
                    if len(english_alert[num_poly][0]) > 1:
                        poly.append([float(english_alert[num_poly][0][el + 1]),
                                     float(english_alert[num_poly][0][el]),
                                     0.0])

                # for temporary care of the duplicate neighbors coordinate
                # poly = [k for k, g in groupby(poly)]
                no_dup_poly = []
                for k in poly:
                    if k not in no_dup_poly:
                        no_dup_poly.append(k)
                no_dup_poly.append(poly[-1])

                id_ = english_alert[num_poly][7]

                AlertLocation = {
                    'type': "Feature",
                    'properties': {
                        'identifier': id_,
                        'area': english_alert[num_poly][1],
                        'reference': identifier,
                        'zone': french_alert[num_poly][1],
                        'headline': english_alert[num_poly][2],
                        'titre': french_alert[num_poly][2],
                        'descrip_en': english_alert[num_poly][8],
                        'descrip_fr': french_alert[num_poly][3],
                        'effective': english_alert[num_poly][3],
                        'expires': english_alert[num_poly][4],
                        'alert_type': english_alert[num_poly][5],
                        'status': english_alert[num_poly][6],
                        'references': self.references_arr,
                        'url': english_alert[num_poly][9]
                    },
                    'geometry': {
                        'type': "Polygon",
                        'coordinates': [no_dup_poly]
                    }
                }

                data.append(AlertLocation)

        return data


@click.group()
def cap_alerts():
    """Manages cap alerts index"""
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
        loader = CapAlertsRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


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
    """Delete old cap-alerts documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    older_than = (datetime.now() - timedelta(days=days)).strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    click.echo('Deleting documents older than {} ({} days)'.format(
        older_than, days))

    query = {
        'query': {
            'range': {
                'properties.expires': {
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
    """Delete cap-alerts realtime index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    conn.delete(INDEX_NAME)


cap_alerts.add_command(add)
cap_alerts.add_command(clean_records)
cap_alerts.add_command(delete_index)
