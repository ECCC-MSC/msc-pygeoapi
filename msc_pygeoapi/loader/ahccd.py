# Example Usage:
# Load all datasets from scratch:
# msc-pygeoapi data load ahccd --path /path/to/json/locations.json --es https://path/to/elasticsearch --username user --password pass --dataset all # noqa
#
# Load a single dataset from scratch:
# msc-pygeoapi data load ahccd --path /path/to/json/locations.json --es https://path/to/elasticsearch --username user --password pass --dataset trends # noqa

import json
import logging
import click

from msc_pygeoapi import util


LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


def create_index(es, index):
    """
    Creates the Elasticsearch index at es. If the index already exists,
    it is deleted and re-created. The mappings for the two types are also
    created.

    :param es: elasticsearch.Elasticsearch object connected to ES cluster.
    :param index: Identifier for the index to be created.
    """

    if index == 'annual':
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
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
                                "pressure_sea_level_units__pression_niveau_mer_unite": { # noqa
                                    "type": "text",
                                    "fields": {
                                        "raw": {"type": "keyword"}
                                    }
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": { # noqa
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

        index_name = 'ahccd_annual'
        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the AHCCD annuals index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'monthly':
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
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
                                   "type": "date",
                                   "format": "yyyy-MM||yyyy"
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
                                "pressure_sea_level_units__pression_niveau_mer_unite": { # noqa
                                    "type": "text",
                                    "fields": {
                                        "raw": {"type": "keyword"}
                                    }
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": { # noqa
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

        index_name = 'ahccd_monthly'
        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the AHCCD monthlies index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'seasonal':
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
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
                                "pressure_sea_level_units__pression_niveau_mer_unite": { # noqa
                                    "type": "text",
                                    "fields": {
                                        "raw": {"type": "keyword"}
                                    }
                                },
                                "pressure_station__pression_station": {
                                    "type": "float"
                                },
                                "pressure_station_units__pression_station_unites": { # noqa
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

        index_name = 'ahccd_seasonal'
        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the AHCCD seasonals index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'stations':
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
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

        index_name = 'ahccd_stations'
        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the AHCCD stations index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'trends':
        mapping =\
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
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

        index_name = 'ahccd_trends'
        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the AHCCD trends index')
        es.indices.create(index=index_name, body=mapping)


def generate_docs(fp, index):
    """
    Reads AHCCD and CMIP5 data from file(s) at fp and reformats them
    so they can be nserted into Elasticsearch.

    Returns a generator of dictionaries that represent upsert actions
    into Elasticsearch's bulk API.

    :param fp: the location of the raw data file(s) to load.
    :param index: name of index to load.
    :returns: generator of bulk API upsert actions.
    """

    if index not in ['stations', 'monthly', 'annual', 'seasonal', 'trends']:
        LOGGER.error('Unrecognized AHCCD data type {}'.format(index))
        return

    try:
        with open(fp, 'r') as f:
            json_source = f.read()
            contents = json.loads(json_source)
    except Exception as err:
        LOGGER.error('Could not open JSON file due to: {}.'
                     .format(str(err)))
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
                record['properties']['identifier__identifiant'].split('.')[1],
                record['properties']['identifier__identifiant'].split('.')[2])
            del record['properties']['year__annee']
        elif index == 'trends':
            index_name = 'ahccd_trends'
            identifier = '{}.{}.{}'.format(
                record['properties']['station_id__id_station'],
                record['properties']['period__periode'],
                record['properties']['measurement_type__type_mesure'])
            record['properties']['identifier__identifiant'] = identifier

        action = {
            '_id': record['properties']['identifier__identifiant'],
            '_index': index_name,
            '_op_type': 'update',
            'doc': record,
            'doc_as_upsert': True
        }
        yield action


@click.command()
@click.pass_context
@click.option('--path', type=click.Path(exists=True, resolve_path=True),
              help='Path to file with raw JSON locations')
@click.option('--es', help='URL to Elasticsearch.')
@click.option('--username', help='Username to connect to HTTPS')
@click.option('--password', help='Password to connect to HTTPS')
@click.option('--dataset', help='ES dataset to load, or all\
                                 if loading everything')
def ahccd(ctx, path, es, username, password, dataset):
    """
    Loads AHCCD and CMIP5 data into Elasticsearch

    Controls transformation from oracle to Elasticsearch.

    The JSON locations file should be a JSON of the form:
    {
        "stations": "/path/to/stations.json",
        "annual": "/path/to/annual.json",
        "monthly": "/path/to/monthly.json",
        "seasonal": "/path/to/seasonal.json",
        "trends": "/path/to/trends.json"
    }

    :param path: path to file with raw JSON locations
    :param es: path to Elasticsearch index.
    :param username: username for HTTP authentication.
    :param password: password for HTTP authentication.
    :param dataset: name of dataset to load, or all for all datasets.
    """

    auth = (username, password)
    es_client = util.get_es(es, auth)

    try:
        with open(path, 'r') as f:
            path_dict = json.loads(f.read())
    except Exception as err:
        LOGGER.error('Could not open JSON location file due to: {}.'
                     .format(str(err)))

    if dataset == 'all':
        try:
            LOGGER.info('Populating stations...')
            create_index(es_client, 'stations')
            stations = generate_docs(path_dict['stations'], 'stations')

            util.submit_elastic_package(es_client, stations)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'
                         .format(str(err)))

        try:
            LOGGER.info('Populating trends...')
            create_index(es_client, 'trends')
            trends = generate_docs(path_dict['trends'], 'trends')

            util.submit_elastic_package(es_client, trends)
            LOGGER.info('Trends populated.')
        except Exception as err:
            LOGGER.error('Could not populate trends due to: {}.'
                         .format(str(err)))

        try:
            LOGGER.info('Populating annual...')
            create_index(es_client, 'annual')
            annuals = generate_docs(path_dict['annual'], 'annual')

            util.submit_elastic_package(es_client, annuals)
            LOGGER.info('Annual populated.')
        except Exception as err:
            LOGGER.error('Could not populate annual due to: {}.'
                         .format(str(err)))

        try:
            LOGGER.info('Populating seasonal...')
            create_index(es_client, 'seasonal')
            seasonals = generate_docs(path_dict['seasonal'], 'seasonal')

            util.submit_elastic_package(es_client, seasonals)
            LOGGER.info('Seasonal populated.')
        except Exception as err:
            LOGGER.error('Could not populate seasonal due to: {}.'
                         .format(str(err)))

        try:
            LOGGER.info('Populating monthly...')
            create_index(es_client, 'monthly')
            monthlies = generate_docs(path_dict['monthly'], 'monthly')

            util.submit_elastic_package(es_client, monthlies)
            LOGGER.info('Monthly populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly due to: {}.'
                         .format(str(err)))

    elif dataset == 'stations':
        try:
            LOGGER.info('Populating stations...')
            create_index(es_client, 'stations')
            stations = generate_docs(path_dict['stations'], 'stations')

            util.submit_elastic_package(es_client, stations)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'
                         .format(str(err)))

    elif dataset == 'trends':
        try:
            LOGGER.info('Populating trends...')
            create_index(es_client, 'trends')
            trends = generate_docs(path_dict['trends'], 'trends')

            util.submit_elastic_package(es_client, trends)
            LOGGER.info('Trends populated.')
        except Exception as err:
            LOGGER.error('Could not populate trends due to: {}.'
                         .format(str(err)))

    elif dataset == 'annual':
        try:
            LOGGER.info('Populating annual...')
            create_index(es_client, 'annual')
            annuals = generate_docs(path_dict['annual'], 'annual')

            util.submit_elastic_package(es_client, annuals)
            LOGGER.info('Annual populated.')
        except Exception as err:
            LOGGER.error('Could not populate annual due to: {}.'
                         .format(str(err)))

    elif dataset == 'seasonal':
        try:
            LOGGER.info('Populating seasonal...')
            create_index(es_client, 'seasonal')
            seasonals = generate_docs(path_dict['seasonal'], 'seasonal')

            util.submit_elastic_package(es_client, seasonals)
            LOGGER.info('Seasonal populated.')
        except Exception as err:
            LOGGER.error('Could not populate seasonal due to: {}.'
                         .format(str(err)))

    elif dataset == 'monthly':
        try:
            LOGGER.info('Populating monthly...')
            create_index(es_client, 'monthly')
            monthlies = generate_docs(path_dict['monthly'], 'monthly')

            util.submit_elastic_package(es_client, monthlies)
            LOGGER.info('Monthly populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly due to: {}.'
                         .format(str(err)))

    else:
        LOGGER.critical('Unknown dataset parameter {}, skipping index population.'.format(dataset)) # noqa

    LOGGER.info('Finished populating indices.')
