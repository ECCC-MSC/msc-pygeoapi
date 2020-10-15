# =================================================================
#
# Author: Alex Hurka <alex.hurka@canada.ca>
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2019 Alex Hurka
# Copyright (c) 2020 Etienne Pelletier
# Copyright (c) 2020 Tom Kralidis

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

import json
import logging

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.env import MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import get_es, submit_elastic_package


LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


class AhccdLoader(BaseLoader):
    """AHCCD Loader"""

    def __init__(self, plugin_def):
        """initializer"""

        super().__init__()

        if plugin_def['es_conn_dict']:
            self.ES = get_es(
                plugin_def['es_conn_dict']['host'],
                plugin_def['es_conn_dict']['auth'],
            )
        else:
            self.ES = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    def create_index(self, index):
        """
        Creates the Elasticsearch index at self.ES. If the index already
        exists, it is deleted and re-created. The mappings for the two types
        are also created.

        :param index: Identifier for the index to be created.
        """

        if index == 'annual':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "identifier__identifiant": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "lat__lat": {"type": "float"},
                                "lon__long": {"type": "float"},
                                "province__province": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_group__groupe_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_value__valeur_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_sea_level__pression_niveau_mer": {
                                    "type": "float"
                                },
                                "pressure_sea_level_units__pression_niveau_mer_unite": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "rain__pluie": {"type": "float"},
                                "rain_units__pluie_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "snow__neige": {"type": "float"},
                                "snow_units__neige_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_id__id_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_max__temp_max": {"type": "float"},
                                "temp_max_units__temp_max_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_mean__temp_moyenne": {"type": "float"},
                                "temp_mean_units__temp_moyenne_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_min__temp_min": {"type": "float"},
                                "temp_min_units__temp_min_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "total_precip__precip_totale": {
                                    "type": "float"
                                },
                                "total_precip_units__precip_totale_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "wind_speed__vitesse_vent": {"type": "float"},
                                "wind_speed_units__vitesse_vent_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "year__annee": {"type": "integer"},
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'ahccd_annual'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the AHCCD annuals index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'monthly':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "date": {
                                    "type": "date",
                                    "format": "yyyy-MM||yyyy",
                                },
                                "identifier__identifiant": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "lat__lat": {"type": "float"},
                                "lon__long": {"type": "float"},
                                "province__province": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_group__groupe_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_value__valeur_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_sea_level__pression_niveau_mer": {
                                    "type": "float"
                                },
                                "pressure_sea_level_units__pression_niveau_mer_unite": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "rain__pluie": {"type": "float"},
                                "rain_units__pluie_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "snow__neige": {"type": "float"},
                                "snow_units__neige_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_id__id_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_max__temp_max": {"type": "float"},
                                "temp_max_units__temp_max_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_mean__temp_moyenne": {"type": "float"},
                                "temp_mean_units__temp_moyenne_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_min__temp_min": {"type": "float"},
                                "temp_min_units__temp_min_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "total_precip__precip_totale": {
                                    "type": "float"
                                },
                                "total_precip_units__precip_totale_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "wind_speed__vitesse_vent": {"type": "float"},
                                "wind_speed_units__vitesse_vent_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "year__annee": {"type": "integer"},
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'ahccd_monthly'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the AHCCD monthlies index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'seasonal':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "identifier__identifiant": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "lat__lat": {"type": "float"},
                                "lon__long": {"type": "float"},
                                "province__province": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_group__groupe_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period_value__valeur_periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_sea_level__pression_niveau_mer": {
                                    "type": "float"
                                },
                                "pressure_sea_level_units__pression_niveau_mer_unite": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": {  # noqa
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "rain__pluie": {"type": "float"},
                                "rain_units__pluie_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "snow__neige": {"type": "float"},
                                "snow_units__neige_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_id__id_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_max__temp_max": {"type": "float"},
                                "temp_max_units__temp_max_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_mean__temp_moyenne": {"type": "float"},
                                "temp_mean_units__temp_moyenne_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "temp_min__temp_min": {"type": "float"},
                                "temp_min_units__temp_min_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "total_precip__precip_totale": {
                                    "type": "float"
                                },
                                "total_precip_units__precip_totale_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "wind_speed__vitesse_vent": {"type": "float"},
                                "wind_speed_units__vitesse_vent_unites": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "year__annee": {"type": "integer"},
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'ahccd_seasonal'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the AHCCD seasonals index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'stations':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "identifier__identifiant": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_id__id_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_name__nom_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "measurement_type__type_mesure": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period__periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "trend_value__valeur_tendance": {
                                    "type": "float"
                                },
                                "elevation__elevation": {"type": "float"},
                                "province__province": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "joined__rejoint": {"type": "integer"},
                                "year_range__annees": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'ahccd_stations'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the AHCCD stations index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'trends':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "identifier__identifiant": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_id__id_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "station_name__nom_station": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "measurement_type__type_mesure": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "period__periode": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "trend_value__valeur_tendance": {
                                    "type": "float"
                                },
                                "elevation__elevation": {"type": "float"},
                                "province__province": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "joined__rejoint": {"type": "integer"},
                                "year_range__annees": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'ahccd_trends'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the AHCCD trends index')
            self.ES.indices.create(index=index_name, body=mapping)

    def generate_docs(self, fp, index):
        """
        Reads AHCCD and CMIP5 data from file(s) at fp and reformats them
        so they can be nserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param fp: the location of the raw data file(s) to load.
        :param index: name of index to load.
        :returns: generator of bulk API upsert actions.
        """

        if index not in [
            'stations',
            'monthly',
            'annual',
            'seasonal',
            'trends',
        ]:
            LOGGER.error('Unrecognized AHCCD data type {}'.format(index))
            return

        try:
            with open(fp, 'r') as f:
                json_source = f.read()
                contents = json.loads(json_source)
        except Exception as err:
            LOGGER.error(f'Could not open JSON file due to: {str(err)}.')
            return

        for record in contents['features']:
            if index == 'annual':
                index_name = 'ahccd_annual'
            elif index == 'seasonal':
                index_name = 'ahccd_seasonal'
            elif index == 'stations':
                index_name = 'ahccd_stations'
                stn_id = record['properties']['station_id__id_station']
                record['properties']['identifier__identifiant'] = stn_id
            elif index == 'monthly':
                index_name = 'ahccd_monthly'
                record['properties']['date'] = '{}-{}'.format(
                    record['properties']['identifier__identifiant'].split('.')[
                        1
                    ],
                    record['properties']['identifier__identifiant'].split('.')[
                        2
                    ],
                )
                del record['properties']['year__annee']
            elif index == 'trends':
                index_name = 'ahccd_trends'
                identifier = '{}.{}.{}'.format(
                    record['properties']['station_id__id_station'],
                    record['properties']['period__periode'],
                    record['properties']['measurement_type__type_mesure'],
                )
                record['properties']['identifier__identifiant'] = identifier

            action = {
                '_id': record['properties']['identifier__identifiant'],
                '_index': index_name,
                '_op_type': 'update',
                'doc': record,
                'doc_as_upsert': True,
            }
            yield action


@click.group()
def ahccd():
    """Manages AHCCD indices"""
    pass


CTL_HELP = '''
    The control file should be a JSON of the form:
    {
        "stations": "/path/to/stations.json",
        "annual": "/path/to/annual.json",
        "monthly": "/path/to/monthly.json",
        "seasonal": "/path/to/seasonal.json",
        "trends": "/path/to/trends.json"
    }
    '''


@click.command()
@click.pass_context
@cli_options.OPTION_FILE('--ctl', help=CTL_HELP)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_DATASET(
    type=click.Choice(
        ['all', 'stations', 'trends', 'annual', 'seasonal', 'monthly']
    )
)
def add(ctx, ctl, es, username, password, dataset):
    """Loads AHCCD data from JSON into Elasticsearch"""

    plugin_def = {
        'es_conn_dict': {'host': es, 'auth': (username, password)}
        if all([es, username, password])
        else None,
        'handler': 'msc_pygeoapi.loader.ahccd.AhccdLoader',
    }

    loader = AhccdLoader(plugin_def)

    try:
        with open(ctl, 'r') as f:
            ctl_dict = json.loads(f.read())
    except Exception as err:
        msg = 'Could not open JSON location file: {}'.format(err)
        click.ClickException(err)

    if dataset == 'all':
        datasets_to_process = [
            'annual',
            'monthly',
            'seasonal',
            'stations',
            'trends'
        ]
    else:
        datasets_to_process = [dataset]

    click.echo('Processing dataset(s): {}'.format(datasets_to_process))

    for dtp in datasets_to_process:
        try:
            click.echo('Populating {} index'.format(dtp))
            loader.create_index(dtp)
            dtp_data = loader.generate_docs(ctl_dict[dtp], dtp)
            submit_elastic_package(loader.ES, dtp_data)
        except Exception as err:
            msg = 'Could not populate {} index: {}'.format(dtp, err)
            raise click.ClickException(msg)


ahccd.add_command(add)
