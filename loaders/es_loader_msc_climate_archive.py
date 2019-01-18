# Example Usage:
# Load all datasets from scratch:
# python es_loader_msc_climate_archive.py --db <oracle db connection string> --es https://path/to/elasticsearch --username user --password pass --dataset all # noqa
#
# Load a single dataset from scratch:
# python es_loader_msc_climate_archive.py --db <oracle db connection string> --es https://path/to/elasticsearch --username user --password pass --dataset daily # noqa
#
# Update a dataset starting from a point in time
# python es_loader_msc_climate_archive.py --db <oracle db connection string> --es https://path/to/elasticsearch --username user --password pass --dataset daily --date 2018-08-22 # noqa
#
# Update a dataset on a regular basis (e.g. update based on new data in last 7 days)  # noqa
# python es_loader_msc_climate_archive.py --db <oracle db connection string> --es https://path/to/elasticsearch --username user --password pass --dataset daily --date $(date -d '-7day' +"%Y-%m-%d") # noqa


import logging
import cx_Oracle
import requests
import json
import click
import collections

LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


def create_index(path, index, AUTH):
    """
    Creates the ElasticSearch index at path. If the index already exists,
    it is deleted and re-created. The mappings for the two types are also
    created.

    :param path: the path to ElasticSearch.
    :param index: the index to be created.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    if index == 'stations':
        r = requests.delete('{}/climate_station_information'.format(path),
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
                                    "PROV_STATE_TERR_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STN_ID": {
                                        "type": "integer"
                                    },
                                    "STATION_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ENG_PROV_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FRE_PROV_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "COUNTRY": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "LATITUDE": {
                                        "type": "integer"
                                    },
                                    "LONGITUDE": {
                                        "type": "integer"
                                    },
                                    "TIMEZONE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ELEVATION": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "CLIMATE_IDENTIFIER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "TC_IDENTIFIER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "WMO_IDENTIFIER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATION_TYPE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "NORMAL_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PUBLICATION_CODE": {
                                        "type": "integer"
                                    },
                                    "DISPLAY_CODE": {
                                        "type": "integer"
                                    },
                                    "ENG_STN_OPERATOR_ACRONYM": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FRE_STN_OPERATOR_ACRONYM": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ENG_STN_OPERATOR_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FRE_STN_OPERATOR_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "HAS_MONTHLY_SUMMARY": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "HAS_NORMALS_DATA": {
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

        r = requests.put('{}/climate_station_information'.format(path),
                         data=json.dumps(mapping), auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the stations index')

    if index == 'normals':
        r = requests.delete('{}/climate_normals_data'.format(path),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete normals due to: {}'.format(r.text))
        else:
            LOGGER.info('Deleted the normals index')

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
                                    "STN_ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MONTH": {
                                        "type": "integer"
                                    },
                                    "VALUE": {
                                        "type": "integer"
                                    },
                                    "OCCURRENCE_COUNT": {
                                        "type": "integer"
                                    },
                                    "PUBLICATION_CODE": {
                                        "type": "integer"
                                    },
                                    "NORMAL_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "NORMAL_ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PROVINCE_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ENG_PUB_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FRE_PUB_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PERIOD": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PERIOD_BEGIN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PERIOD_END": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATION_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "YEAR_COUNT_NORMAL_PERIOD": {
                                        "type": "integer"
                                    },
                                    "MAX_DURATION_MISSING_YEARS": {
                                        "type": "integer"
                                    },
                                    "FIRST_YEAR_NORMAL_PERIOD": {
                                        "type": "integer"
                                    },
                                    "LAST_YEAR_NORMAL_PERIOD": {
                                        "type": "integer"
                                    },
                                    "FIRST_YEAR": {
                                        "type": "integer"
                                    },
                                    "LAST_YEAR": {
                                        "type": "integer"
                                    },
                                    "TOTAL_OBS_COUNT": {
                                        "type": "integer"
                                    },
                                    "PERCENT_OF_POSSIBLE_OBS": {
                                        "type": "integer"
                                    },
                                    "CURRENT_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FIRST_OCCURRENCE_DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd HH:mm:ss"
                                    },
                                    "DATE_CALCULATED": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd HH:mm:ss"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put('{}/climate_normals_data'.format(path),
                         data=json.dumps(mapping), auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create normals due to: {}'.format(r.text))
        else:
            LOGGER.info('Created the normals index')

    if index == 'monthly_summary':
        r = requests.delete('{}/climate_public_climate_summary'.format(path),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create monthly summary due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the monthly summary index')

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
                                    "CLIMATE_IDENTIFIER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STN_ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATION_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PROVINCE_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "LATITUDE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "LONGITUDE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MEAN_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "NORMAL_MEAN_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "MAX_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "MIN_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "TOTAL_SNOWFALL": {
                                        "type": "float"
                                    },
                                    "NORMAL_SNOWFALL": {
                                        "type": "float"
                                    },
                                    "TOTAL_PRECIPITATION": {
                                        "type": "float"
                                    },
                                    "NORMAL_PRECIPITATION": {
                                        "type": "float"
                                    },
                                    "BRIGHT_SUNSHINE": {
                                        "type": "float"
                                    },
                                    "NORMAL_SUNSHINE": {
                                        "type": "float"
                                    },
                                    "SNOW_ON_GROUND_LAST_DAY": {
                                        "type": "float"
                                    },
                                    "DAYS_WITH_VALID_MIN_TEMP": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_VALID_MEAN_TEMP": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_VALID_MAX_TEMP": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_VALID_SNOWFALL": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_VALID_PRECIP": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_VALID_SUNSHINE": {
                                        "type": "integer"
                                    },
                                    "DAYS_WITH_PRECIP_GE_1MM": {
                                        "type": "integer"
                                    },
                                    "HEATING_DEGREE_DAYS": {
                                        "type": "integer"
                                    },
                                    "COOLING_DEGREE_DAYS": {
                                        "type": "integer"
                                    },
                                    "LOCAL_YEAR": {
                                        "type": "integer"
                                    },
                                    "LOCAL_MONTH": {
                                        "type": "integer"
                                    },
                                    "LAST_UPDATED": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd HH:mm:ss"
                                    },
                                    "LOCAL_DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put('{}/climate_public_climate_summary'.format(path),
                         data=json.dumps(mapping), auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create monthly summary due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the monthly summary index')

    if index == 'daily_summary':
        r = requests.delete('{}/climate_public_daily_data'.format(path),
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create daily summary due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the daily summary index')

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
                                    "CLIMATE_IDENTIFIER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STN_ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATION_NAME": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "SOURCE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "ID": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MAX_TEMPERATURE_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MIN_TEMPERATURE_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MEAN_TEMPERATURE_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PROVINCE_CODE": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MAX_REL_HUMIDITY_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MIN_REL_HUMIDITY_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "TOTAL_RAIN_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "TOTAL_SNOW_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "TOTAL_PRECIPITATION_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "SNOW_ON_GROUND_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DIRECTION_MAX_GUST_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "SPEED_MAX_GUST_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "HEATING_DEGREE_DAYS_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "COOLING_DEGREE_DAYS_FLAG": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MEAN_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "TOTAL_RAIN": {
                                        "type": "float"
                                    },
                                    "MAX_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "MIN_TEMPERATURE": {
                                        "type": "float"
                                    },
                                    "MAX_REL_HUMIDITY": {
                                        "type": "float"
                                    },
                                    "MIN_REL_HUMIDITY": {
                                        "type": "float"
                                    },
                                    "TOTAL_SNOW": {
                                        "type": "float"
                                    },
                                    "SNOW_ON_GROUND": {
                                        "type": "float"
                                    },
                                    "TOTAL_PRECIPITATION": {
                                        "type": "float"
                                    },
                                    "DIRECTION_MAX_GUST": {
                                        "type": "float"
                                    },
                                    "SPEED_MAX_GUST": {
                                        "type": "float"
                                    },
                                    "HEATING_DEGREE_DAYS": {
                                        "type": "integer"
                                    },
                                    "COOLING_DEGREE_DAYS": {
                                        "type": "integer"
                                    },
                                    "LOCAL_YEAR": {
                                        "type": "integer"
                                    },
                                    "LOCAL_MONTH": {
                                        "type": "integer"
                                    },
                                    "LOCAL_DAY": {
                                        "type": "integer"
                                    },
                                    "LOCAL_DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd HH:mm:ss"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put('{}/climate_public_daily_data'.format(path),
                         data=json.dumps(mapping), auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create daily summary due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the daily summary index')


def load_stations(path, cur, AUTH):
    """
    Queries stations data from the db, and reformats
    data so it can be inserted into ElasticSearch.

    :param path: path to ElasticSearch.
    :param cur: oracle cursor to perform queries against.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    try:
        cur.execute('select * from CCCS_PORTAL.STATION_INFORMATION')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        insert_dict = dict(zip([x[0] for x in cur.description], row))
        for key in insert_dict:
            # This is a quick fix for trailing spaces and should not be here.
            # Data should be fixed on db side.
            try:
                insert_dict[key] = insert_dict[key].strip()
            except Exception as err:
                LOGGER.debug('Could not strip value {} due to {}, skipping'.format(insert_dict[key], str(err))) # noqa

            # Transform Date fields from datetime to string.
            if 'DATE' in key:
                insert_dict[key] = str(insert_dict[key]) if insert_dict[key] is not None else insert_dict[key] # noqa

        coords = [float(insert_dict['LONGITUDE_DECIMAL_DEGREES']),
                  float(insert_dict['LATITUDE_DECIMAL_DEGREES'])]
        del insert_dict['LONGITUDE_DECIMAL_DEGREES']
        del insert_dict['LATITUDE_DECIMAL_DEGREES']
        wrapper = {'type': 'Feature', 'properties': insert_dict,
                   'geometry': {'type': 'Point', 'coordinates': coords}}
        r = requests.put('{}/climate_station_information/FeatureCollection/{}'.format(path, wrapper['properties']['STN_ID']), data=json.dumps(wrapper), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
        if r.status_code != POST_OK and r.status_code != HTTP_OK:
            LOGGER.error('Could not insert into stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully inserted a record into the stations index') # noqa


def load_normals(path, cur, stn_dict, normals_dict, periods_dict, AUTH):
    """
    Queries normals data from the db, and reformats
    data so it can be inserted into ElasticSearch.

    :param path: path to ElasticSearch.
    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param normals_dict: mapping of normal IDs to normals information.
    :param periods_dict: mapping of normal period IDs to
                         normal period information.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    try:
        cur.execute('select * from CCCS_PORTAL.NORMALS_DATA')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        insert_dict = dict(zip([x[0] for x in cur.description], row))

        for key in insert_dict:
            # Transform Date fields from datetime to string.
            if 'DATE' in key:
                insert_dict[key] = str(insert_dict[key]) if insert_dict[key] is not None else insert_dict[key] # noqa
        insert_dict['ID'] = '{}.{}.{}'.format(insert_dict['STN_ID'],
                                              insert_dict['NORMAL_ID'],
                                              insert_dict['MONTH'])
        if insert_dict['STN_ID'] in stn_dict:
            coords = stn_dict[insert_dict['STN_ID']]['coordinates']
            insert_dict['STATION_NAME'] = stn_dict[insert_dict['STN_ID']]['STATION_NAME'] # noqa
            insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']]['PROVINCE_CODE'] # noqa
            insert_dict['ENG_PUB_NAME'] = normals_dict[insert_dict['NORMAL_ID']]['ENG_PUB_NAME'] # noqa
            insert_dict['FRE_PUB_NAME'] = normals_dict[insert_dict['NORMAL_ID']]['FRE_PUB_NAME'] # noqa
            insert_dict['PERIOD'] = normals_dict[insert_dict['NORMAL_ID']]['PERIOD'] # noqa
            insert_dict['PERIOD_BEGIN'] = periods_dict[insert_dict['NORMAL_PERIOD_ID']]['PERIOD_BEGIN'] # noqa
            insert_dict['PERIOD_END'] = periods_dict[insert_dict['NORMAL_PERIOD_ID']]['PERIOD_END'] # noqa

            del insert_dict['NORMAL_PERIOD_ID']
            wrapper = {'type': 'Feature', 'properties': insert_dict,
                       'geometry': {'type': 'Point', 'coordinates': coords}}
            r = requests.put('{}/climate_normals_data/FeatureCollection/{}'.format(path, insert_dict['ID']), data=json.dumps(wrapper), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
            if r.status_code != POST_OK and r.status_code != HTTP_OK:
                LOGGER.error('Could not insert into normals due to: {}'.format(r.text)) # noqa
            else:
                LOGGER.info('Successfully inserted a record into the normals index') # noqa
        else:
            LOGGER.error('Bad STN ID: {}, skipping records for this station'.format(insert_dict['STN_ID'])) # noqa


def load_monthly_data(path, cur, stn_dict, AUTH, date=None):
    """
    Queries monthly data from the db, and reformats
    data so it can be inserted into ElasticSearch.

    :param path: path to ElasticSearch.
    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    :param date: date to start fetching data from.
    """

    if not date:
        try:
            cur.execute('select * from CCCS_PORTAL.PUBLIC_CLIMATE_SUMMARY')
        except Exception as err:
            LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa
    else:
        try:
            cur.execute("select * from CCCS_PORTAL.PUBLIC_CLIMATE_SUMMARY WHERE LAST_UPDATED > TO_TIMESTAMP('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')".format(date)) # noqa
        except Exception as err:
            LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        insert_dict = dict(zip([x[0] for x in cur.description], row))
        # Transform Date fields from datetime to string.
        insert_dict['LAST_UPDATED'] = str(insert_dict['LAST_UPDATED']) if insert_dict['LAST_UPDATED'] is not None else insert_dict['LAST_UPDATED'] # noqa

        insert_dict['ID'] = '{}.{}.{}'.format(insert_dict['STN_ID'],
                                              insert_dict['LOCAL_YEAR'],
                                              insert_dict['LOCAL_MONTH'])
        if insert_dict['STN_ID'] in stn_dict:
            coords = stn_dict[insert_dict['STN_ID']]['coordinates']
            insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']]['PROVINCE_CODE'] # noqa
            wrapper = {'type': 'Feature', 'properties': insert_dict,
                       'geometry': {'type': 'Point', 'coordinates': coords}}
            r = requests.put('{}/climate_public_climate_summary/FeatureCollection/{}'.format(path, insert_dict['ID']), data=json.dumps(wrapper), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
            if r.status_code != POST_OK and r.status_code != HTTP_OK:
                LOGGER.error('Could not insert into monthly summary due to: {}'.format(r.text)) # noqa
            else:
                LOGGER.info('Successfully inserted a record into the monthly summary index') # noqa
        else:
            LOGGER.error('Bad STN ID: {}, skipping records for this station'.format(insert_dict['STN_ID'])) # noqa


def load_daily_data(path, cur, stn_dict, AUTH, date=None):
    """
    Queries daily data from the db, and reformats
    data so it can be inserted into ElasticSearch.

    :param path: path to ElasticSearch.
    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    :param date: date to start fetching data from.
    """
    for station in stn_dict:
        if not date:
            try:
                cur.execute('select * from CCCS_PORTAL.PUBLIC_DAILY_DATA where STN_ID={}'.format(station)) # noqa
            except Exception as err:
                LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa
        else:
            try:
                cur.execute("select * from CCCS_PORTAL.PUBLIC_DAILY_DATA where STN_ID={} and LOCAL_DATE > TO_TIMESTAMP('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')".format(station, date)) # noqa
            except Exception as err:
                LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

        for row in cur:
            insert_dict = dict(zip([x[0] for x in cur.description], row))
            # Transform Date fields from datetime to string.
            insert_dict['LOCAL_DATE'] = str(insert_dict['LOCAL_DATE']) if insert_dict['LOCAL_DATE'] is not None else insert_dict['LOCAL_DATE'] # noqa

            insert_dict['ID'] = '{}.{}.{}.{}'.format(
                insert_dict['CLIMATE_IDENTIFIER'],
                insert_dict['LOCAL_YEAR'],
                insert_dict['LOCAL_MONTH'], # noqa
                insert_dict['LOCAL_DAY'])
            if insert_dict['STN_ID'] in stn_dict:
                coords = stn_dict[insert_dict['STN_ID']]['coordinates']
                insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']]['PROVINCE_CODE'] # noqa
                insert_dict['STATION_NAME'] = stn_dict[insert_dict['STN_ID']]['STATION_NAME'] # noqa
                wrapper = {'type': 'Feature', 'properties': insert_dict,
                           'geometry': {'type': 'Point',
                                        'coordinates': coords}}
                r = requests.put('{}/climate_public_daily_data/FeatureCollection/{}'.format(path, insert_dict['ID']), data=json.dumps(wrapper), auth=AUTH, verify=VERIFY, headers=HEADERS) # noqa
                if r.status_code != POST_OK and r.status_code != HTTP_OK:
                    LOGGER.error('Could not insert into daily summary due to: {}'.format(r.text)) # noqa
                else:
                    LOGGER.info('Successfully inserted a record into the daily summary index') # noqa
            else:
                LOGGER.error('Bad STN ID: {}, skipping records for this station'.format(insert_dict['STN_ID'])) # noqa


def get_station_data(cur, station, starting_from):
    """
    Creates a mapping of station ID to station coordinates and province name.

    :param cur: oracle cursor to perform queries against.

    :returns: A dictionary of dictionaries containing
              station coordinates and province name keyed by station ID.
    """
    stn_dict = collections.OrderedDict()
    try:
        if station:
            if starting_from:
                cur.execute('select STN_ID, LONGITUDE_DECIMAL_DEGREES,\
                             LATITUDE_DECIMAL_DEGREES,\
                             ENG_PROV_NAME, FRE_PROV_NAME,\
                             PROV_STATE_TERR_CODE,\
                             STATION_NAME from\
                             CCCS_PORTAL.STATION_INFORMATION\
                             where STN_ID >= {}\
                             order by STN_ID'.format(station))
            else:
                cur.execute('select STN_ID, LONGITUDE_DECIMAL_DEGREES,\
                             LATITUDE_DECIMAL_DEGREES,\
                             ENG_PROV_NAME, FRE_PROV_NAME,\
                             PROV_STATE_TERR_CODE,\
                             STATION_NAME from\
                             CCCS_PORTAL.STATION_INFORMATION\
                             where STN_ID = {}\
                             order by STN_ID'.format(station))
        else:
            cur.execute('select STN_ID, LONGITUDE_DECIMAL_DEGREES,\
                         LATITUDE_DECIMAL_DEGREES,\
                         ENG_PROV_NAME, FRE_PROV_NAME,\
                         PROV_STATE_TERR_CODE,\
                         STATION_NAME from\
                         CCCS_PORTAL.STATION_INFORMATION\
                         order by STN_ID')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        stn_dict[row[0]] = {'coordinates': [row[1], row[2]],
                            'ENG_PROV_NAME': row[3],
                            'FRE_PROV_NAME': row[4],
                            'PROVINCE_CODE': row[5].strip(),  # remove the strip # noqa
                            'STATION_NAME': row[6]
                            }
    return stn_dict


def get_normals_data(cur):
    """
    Creates a mapping of normal ID to pub_name and period.

    :param cur: oracle cursor to perform queries against.

    :returns: A dictionary of dictionaries containing
              pub_name and period keyed by normal ID.
    """
    normals_dict = {}
    try:
        cur.execute('select NORMAL_ID, ENG_PUB_NAME,\
                     FRE_PUB_NAME, PERIOD from\
                     CCCS_PORTAL.VALID_NORMALS_ELEMENTS')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        normals_dict[row[0]] = {'ENG_PUB_NAME': row[1],
                                'FRE_PUB_NAME': row[2],
                                'PERIOD': row[3]
                                }
    return normals_dict


def get_normals_periods(cur):
    """
    Creates a mapping of normal period ID to period begin and end.

    :param cur: oracle cursor to perform queries against.

    :returns: A dictionary of dictionaries containing
              period begin and end keyed by normal period ID.
    """
    period_dict = {}
    try:
        cur.execute('select NORMAL_PERIOD_ID, PERIOD_BEGIN,\
                     PERIOD_END from\
                     CCCS_PORTAL.NORMAL_PERIODS')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        period_dict[row[0]] = {'PERIOD_BEGIN': row[1],
                               'PERIOD_END': row[2]
                               }
    return period_dict


@click.command()
@click.option('--db', help='Oracle database connection string.')
@click.option('--es', help='URL to ElasticSearch.')
@click.option('--username', help='Username to connect to HTTPS')
@click.option('--password', help='Password to connect to HTTPS')
@click.option('--dataset', help='ES dataset to load, or all\
                                 if loading everything')
@click.option('--station', help='station ID (STN_ID) of\
                                 station to load', required=False)
@click.option('--starting_from',
              help=' Load all stations starting from specified station',
              required=False)
@click.option('--date', help='Start date to fetch updates', required=False)
def cli(db, es, username, password, dataset, station=None,
        starting_from=False, date=None):
    """
    Controls transformation from oracle to ElasticSearch.

    :param db: database connection string.
    :param es: path to ElasticSearch.
    :param username: username for HTTP authentication.
    :param password: password for HTTP authentication.
    :param dataset: name of dataset to load, or all for all datasets.
    :param station: STN_ID of station to index for daily.
    :param starting_from: load all stations after specified station
    :param date: date to start fetching daily and monthly data from.
    """
    AUTH = (username, password)
    try:
        con = cx_Oracle.connect(db)
    except Exception as err:
        msg = 'Could not connect to Oracle: {}'.format(err)
        LOGGER.critical(msg)
        raise click.ClickException(msg)

    cur = con.cursor()

    if dataset == 'all':
        stn_dict = get_station_data(cur, station, starting_from)
        normals_dict = get_normals_data(cur)
        periods_dict = get_normals_periods(cur)

        try:
            LOGGER.info('Populating stations...')
            create_index(es, 'stations', AUTH)
            load_stations(es, cur, AUTH)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating normals...')
            create_index(es, 'normals', AUTH)
            load_normals(es, cur, stn_dict, normals_dict, periods_dict, AUTH)
            LOGGER.info('Normals populated.')
        except Exception as err:
            LOGGER.error('Could not populate normals due to: {}.'.format(str(err))) # noqa    

        try:
            LOGGER.info('Populating monthly summary...')
            if not date:
                create_index(es, 'monthly_summary', AUTH)
            load_monthly_data(es, cur, stn_dict, AUTH, date)
            LOGGER.info('Monthly Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly summary due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating daily summary...')
            if not date:
                create_index(es, 'daily_summary', AUTH)
            load_daily_data(es, cur, stn_dict, AUTH, date)
            LOGGER.info('Daily Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate daily summary due to: {}.'.format(str(err))) # noqa

    elif dataset == 'stations':
        try:
            LOGGER.info('Populating stations...')
            create_index(es, 'stations', AUTH)
            load_stations(es, cur, AUTH)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

    elif dataset == 'normals':
        try:
            LOGGER.info('Populating normals...')
            stn_dict = get_station_data(cur, station, starting_from)
            normals_dict = get_normals_data(cur)
            periods_dict = get_normals_periods(cur)
            create_index(es, 'normals', AUTH)
            load_normals(es, cur, stn_dict, normals_dict, periods_dict, AUTH)
            LOGGER.info('Normals populated.')
        except Exception as err:
            LOGGER.error('Could not populate normals due to: {}.'.format(str(err))) # noqa

    elif dataset == 'monthly':
        try:
            LOGGER.info('Populating monthly summary...')
            stn_dict = get_station_data(cur, station, starting_from)
            if not (date or station or starting_from):
                create_index(es, 'monthly_summary', AUTH)
            load_monthly_data(es, cur, stn_dict, AUTH, date)
            LOGGER.info('Monthly Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly summary due to: {}.'.format(str(err))) # noqa

    elif dataset == 'daily':
        try:
            LOGGER.info('Populating daily summary...')
            stn_dict = get_station_data(cur, station, starting_from)
            normals_dict = get_normals_data(cur)
            periods_dict = get_normals_periods(cur)
            if not (date or station or starting_from):
                create_index(es, 'daily_summary', AUTH)
            load_daily_data(es, cur, stn_dict, AUTH, date)
            LOGGER.info('Daily Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate daily summary due to: {}.'.format(str(err))) # noqa

    else:
        LOGGER.critical('Unknown dataset parameter {}, skipping index population.'.format(dataset)) # noqa

    LOGGER.info('Finished populating indices.')

    con.close()


cli()
