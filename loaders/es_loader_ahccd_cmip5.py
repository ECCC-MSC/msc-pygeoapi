# Example Usage:
# Load all datasets from scratch:
# python es_loader_ahccd_cmip5.py --path /path/to/json/locations.json --es https://path/to/elasticsearch --username user --password pass --dataset all # noqa
#
# Load a single dataset from scratch:
# python es_loader_ahccd_cmip5.py --path /path/to/json/locations.json --es https://path/to/elasticsearch --username user --password pass --dataset trends # noqa

import requests
import json
import logging
import click

LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False

def create_index(es, index, AUTH):
    """
    Creates the Elasticsearch index at es. If the index already exists,
    it is deleted and re-created. The mappings for the two types are also
    created.

    :param es: the path to the ES index.
    :param index: the index to be created.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    if index == 'annual':
        r = requests.delete('{}/ahccd_annual'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete annual index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the annual index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "identifier__identifiant": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "lat__lat": {
                                        "type": "float"
                                    },
                                    "lon__long": {
                                        "type": "float"
                                    },
                                    "province__province": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_group__groupe_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_value__valeur_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_sea_level__pression_niveau_mer": {
                                        "type": "float"
                                    },
                                    "pressure_sea_level_units__pression_niveau_mer_unite": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_station__pression_station": {
                                        "type": "float"
                                    },
                                    "pressure_station_units__pression_station_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "rain__pluie": {
                                        "type": "float"
                                    },
                                    "rain_units__pluie_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "snow__neige": {
                                        "type": "float"
                                    },
                                    "snow_units__neige_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_id__id_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_max__temp_max": {
                                        "type": "float"
                                    },
                                    "temp_max_units__temp_max_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_mean__temp_moyenne": {
                                        "type": "float"
                                    },
                                    "temp_mean_units__temp_moyenne_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_min__temp_min": {
                                        "type": "float"
                                    },
                                    "temp_min_units__temp_min_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "total_precip__precip_totale": {
                                        "type": "float"
                                    },
                                    "total_precip_units__precip_totale_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "wind_speed__vitesse_vent": {
                                        "type": "float"
                                    },
                                    "wind_speed_units__vitesse_vent_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "year__annee": {
                                        "type": "integer"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put('{}/ahccd_annual'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create annual index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the annual index')

    if index == 'monthly':
        r = requests.delete('{}/ahccd_monthly'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete monthly index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the monthly index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "date": {
                                       "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        } 
                                    },
                                    "identifier__identifiant": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "lat__lat": {
                                        "type": "float"
                                    },
                                    "lon__long": {
                                        "type": "float"
                                    },
                                    "province__province": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_group__groupe_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_value__valeur_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_sea_level__pression_niveau_mer": {
                                        "type": "float"
                                    },
                                    "pressure_sea_level_units__pression_niveau_mer_unite": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_station__pression_station": {
                                        "type": "float"
                                    },
                                    "pressure_station_units__pression_station_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "rain__pluie": {
                                        "type": "float"
                                    },
                                    "rain_units__pluie_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "snow__neige": {
                                        "type": "float"
                                    },
                                    "snow_units__neige_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_id__id_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_max__temp_max": {
                                        "type": "float"
                                    },
                                    "temp_max_units__temp_max_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_mean__temp_moyenne": {
                                        "type": "float"
                                    },
                                    "temp_mean_units__temp_moyenne_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_min__temp_min": {
                                        "type": "float"
                                    },
                                    "temp_min_units__temp_min_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "total_precip__precip_totale": {
                                        "type": "float"
                                    },
                                    "total_precip_units__precip_totale_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "wind_speed__vitesse_vent": {
                                        "type": "float"
                                    },
                                    "wind_speed_units__vitesse_vent_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "year__annee": {
                                        "type": "integer"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }
        r = requests.put('{}/ahccd_monthly'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create monthly index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the monthly index')

    if index == 'seasonal':
        r = requests.delete('{}/ahccd_seasonal'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete seasonal index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the seasonal index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "identifier__identifiant": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "lat__lat": {
                                        "type": "float"
                                    },
                                    "lon__long": {
                                        "type": "float"
                                    },
                                    "province__province": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_group__groupe_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period_value__valeur_periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_sea_level__pression_niveau_mer": {
                                        "type": "float"
                                    },
                                    "pressure_sea_level_units__pression_niveau_mer_unite": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "pressure_station__pression_station": {
                                        "type": "float"
                                    },
                                    "pressure_station_units__pression_station_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "rain__pluie": {
                                        "type": "float"
                                    },
                                    "rain_units__pluie_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "snow__neige": {
                                        "type": "float"
                                    },
                                    "snow_units__neige_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_id__id_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_max__temp_max": {
                                        "type": "float"
                                    },
                                    "temp_max_units__temp_max_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_mean__temp_moyenne": {
                                        "type": "float"
                                    },
                                    "temp_mean_units__temp_moyenne_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "temp_min__temp_min": {
                                        "type": "float"
                                    },
                                    "temp_min_units__temp_min_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "total_precip__precip_totale": {
                                        "type": "float"
                                    },
                                    "total_precip_units__precip_totale_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "wind_speed__vitesse_vent": {
                                        "type": "float"
                                    },
                                    "wind_speed_units__vitesse_vent_unites": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "year__annee": {
                                        "type": "integer"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }
        r = requests.put('{}/ahccd_seasonal'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create seasonal index due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the seasonal index')

    if index == 'stations':
        r = requests.delete('{}/ahccd_stations'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the stations index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "identifier__identifiant": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_id__id_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_name__nom_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "measurement_type__type_mesure": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period__periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "trend_value__valeur_tendance": {
                                        "type": "float"
                                    },
                                    "elevation__elevation": {
                                        "type": "float"
                                    },
                                    "province__province": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "joined__rejoint": {
                                        "type": "integer"
                                    },
                                    "year_range__annees": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }
        r = requests.put('{}/ahccd_stations'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the stations index')

    if index == 'trends':
        r = requests.delete('{}/ahccd_trends'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete trends due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the trends index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "identifier__identifiant": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_id__id_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "station_name__nom_station": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "measurement_type__type_mesure": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period__periode": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "trend_value__valeur_tendance": {
                                        "type": "float"
                                    },
                                    "elevation__elevation": {
                                        "type": "float"
                                    },
                                    "province__province": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "joined__rejoint": {
                                        "type": "integer"
                                    },
                                    "year_range__annees": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }
        r = requests.put('{}/ahccd_trends'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create trends due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the trends index')

    if index == 'cmip5':
        r = requests.delete('{}/cmip5'.format(es),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete cmip5 due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the cmip5 index')
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "FeatureCollection": {
                        "_meta": {
                            "geomfields": {
                                "geometry": "POINT"
                            }
                        },
                        "properties": {
                            "type": {"type": "text"},
                            "properties": {
                                "properties": {
                                    "rcp": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "table": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "identifier": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "variable": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "percentile": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "time": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "grid_id": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "value": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "period": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "lat": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "lon": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "year": {
                                        "type": "date",
                                        "format": "yyyy"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }
        r = requests.put('{}/cmip5'.format(es),
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create cmip5 due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the cmip5 index')


def generic_loader(fp, es, index, AUTH):
    """
    Loads AHCCD and CMIP5 data from file(s) at fp into the elasticsearch
    index specified by index at es.

    :param fp: the location of the raw data file(s) to load.
    :param es: path to Elasticsearch.
    :param index: name of index to load.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    if index == 'cmip5':
        # CMIP5 data is stored in multiple files, so each file needs to
        # be read in separately. As a result, CMIP5 paths are stored
        # in a list in the JSON locations file.
        for file in fp:
            try:
                with open(file, 'r') as f:
                    json_source = f.read()
                    data = json.loads(json_source)
                for record in data['features']:
                    r = requests.put('{}/cmip5/FeatureCollection/{}'.format(es, record['properties']['identifier']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into cmip5 due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the cmip5 index') # noqa
            except Exception as err:
                LOGGER.error('Could not open JSON file due to: {}.'.format(str(err))) # noqa
    else:
        try:
            with open(fp, 'r') as f:
                json_source = f.read()
                data = json.loads(json_source)

            for record in data['features']:
                if index == 'monthly':
                    record['properties']['date'] = '{}-{}'.format(record['properties']['identifier__identifiant'].split('.')[1], record['properties']['identifier__identifiant'].split('.')[2]) # noqa
                    del record['properties']['year__annee']
                    r = requests.put('{}/ahccd_monthly/FeatureCollection/{}'.format(es, record['properties']['identifier__identifiant']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into monthly due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the monthly index') # noqa
                if index == 'stations':
                    record['properties']['identifier__identifiant'] = record['properties']['station_id__id_station'] # noqa
                    r = requests.put('{}/ahccd_stations/FeatureCollection/{}'.format(es, record['properties']['identifier__identifiant']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into stations due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the stations index') # noqa
                if index == 'annual':
                    r = requests.put('{}/ahccd_annual/FeatureCollection/{}'.format(es, record['properties']['identifier__identifiant']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into annual due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the annual index') # noqa
                if index == 'seasonal':
                    r = requests.put('{}/ahccd_seasonal/FeatureCollection/{}'.format(es, record['properties']['identifier__identifiant']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into seasonal due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the seasonal index') # noqa
                if index == 'trends':
                    record['properties']['identifier__identifiant'] = '{}.{}.{}'.format(record['properties']['station_id__id_station'], record['properties']['period__periode'], record['properties']['measurement_type__type_mesure']) # noqa
                    r = requests.put('{}/ahccd_trends/FeatureCollection/{}'.format(es, record['properties']['identifier__identifiant']), data=json.dumps(record), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                    if r.status_code != POST_OK and r.status_code != HTTP_OK:
                        LOGGER.error('Could not insert into trends due to: {}'.format(r.text)) # noqa
                    else:
                        LOGGER.info('Successfully inserted a record into the trends index') # noqa
        except Exception as err:
            LOGGER.error('Could not open JSON file due to: {}.'.format(str(err))) # noqa


@click.command()
@click.option('--path', type=click.Path(exists=True, resolve_path=True),
              help='Path to file with raw JSON locations')
@click.option('--es', help='URL to Elasticsearch.')
@click.option('--username', help='Username to connect to HTTPS')
@click.option('--password', help='Password to connect to HTTPS')
@click.option('--dataset', help='ES dataset to load, or all\
                                 if loading everything')
def cli(path, es, username, password, dataset):
    """
    Controls transformation from oracle to Elasticsearch.

    The JSON locations file should be a JSON of the form:
    {
    "stations": "/path/to/stations.json",
    "annual": "/path/to/annual.json",
    "monthly": "/path/to/monthly.json",
    "seasonal": "/path/to/seasonal.json",
    "trends": "/path/to/trends.json",
    "cmip5": ["/path/to/cmip5one.json", /path/to/cmip5two.json, ...]
    }

    :param path: path to file with raw JSON locations
    :param es: path to Elasticsearch index.
    :param username: username for HTTP authentication.
    :param password: password for HTTP authentication.
    :param dataset: name of dataset to load, or all for all datasets.
    """
    AUTH = (username, password)
    try:
        with open(path, 'r') as f:
            path_dict = json.loads(f.read())
    except Exception as err:
        LOGGER.error('Could not open JSON location file due to: {}.'.format(str(err))) # noqa

    if dataset == 'all':
        try:
            LOGGER.info('Populating stations...')
            create_index(es, 'stations', AUTH)
            generic_loader(path_dict['stations'], es, 'stations', AUTH)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating trends...')
            create_index(es, 'trends', AUTH)
            generic_loader(path_dict['trends'], es, 'trends', AUTH)
            LOGGER.info('Trends populated.')
        except Exception as err:
            LOGGER.error('Could not populate trends due to: {}.'.format(str(err))) # noqa    

        try:
            LOGGER.info('Populating annual...')
            create_index(es, 'annual', AUTH)
            generic_loader(path_dict['annual'], es, 'annual', AUTH)
            LOGGER.info('Annual populated.')
        except Exception as err:
            LOGGER.error('Could not populate annual due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating seasonal...')
            create_index(es, 'seasonal', AUTH)
            generic_loader(path_dict['seasonal'], es, 'seasonal', AUTH)
            LOGGER.info('Seasonal populated.')
        except Exception as err:
            LOGGER.error('Could not populate seasonal due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating monthly...')
            create_index(es, 'monthly', AUTH)
            generic_loader(path_dict['monthly'], es, 'monthly', AUTH)
            LOGGER.info('Monthly populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating cmip5...')
            create_index(es, 'cmip5', AUTH)
            generic_loader(path_dict['cmip5'], es, 'cmip5', AUTH)
            LOGGER.info('Cmip5 populated.')
        except Exception as err:
            LOGGER.error('Could not populate cmip5 due to: {}.'.format(str(err))) # noqa

    elif dataset == 'stations':
        try:
            LOGGER.info('Populating stations...')
            create_index(es, 'stations', AUTH)
            generic_loader(path_dict['stations'], es, 'stations', AUTH)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

    elif dataset == 'trends':
        try:
            LOGGER.info('Populating trends...')
            create_index(es, 'trends', AUTH)
            generic_loader(path_dict['trends'], es, 'trends', AUTH)
            LOGGER.info('Trends populated.')
        except Exception as err:
            LOGGER.error('Could not populate trends due to: {}.'.format(str(err))) # noqa    

    elif dataset == 'annual':
        try:
            LOGGER.info('Populating annual...')
            create_index(es, 'annual', AUTH)
            generic_loader(path_dict['annual'], es, 'annual', AUTH)
            LOGGER.info('Annual populated.')
        except Exception as err:
            LOGGER.error('Could not populate annual due to: {}.'.format(str(err))) # noqa

    elif dataset == 'seasonal':
        try:
            LOGGER.info('Populating seasonal...')
            create_index(es, 'seasonal', AUTH)
            generic_loader(path_dict['seasonal'], es, 'seasonal', AUTH)
            LOGGER.info('Seasonal populated.')
        except Exception as err:
            LOGGER.error('Could not populate seasonal due to: {}.'.format(str(err))) # noqa

    elif dataset == 'monthly':
        try:
            LOGGER.info('Populating monthly...')
            create_index(es, 'monthly', AUTH)
            generic_loader(path_dict['monthly'], es, 'monthly', AUTH)
            LOGGER.info('Monthly populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly due to: {}.'.format(str(err))) # noqa

    elif dataset == 'cmip5':
        try:
            LOGGER.info('Populating cmip5...')
            create_index(es, 'cmip5', AUTH)
            generic_loader(path_dict['cmip5'], es, 'cmip5', AUTH)
            LOGGER.info('Cmip5 populated.')
        except Exception as err:
            LOGGER.error('Could not populate cmip5 due to: {}.'.format(str(err))) # noqa

    else:
        LOGGER.critical('Unknown dataset parameter {}, skipping index population.'.format(dataset)) # noqa

    LOGGER.info('Finished populating indices.')


cli()
