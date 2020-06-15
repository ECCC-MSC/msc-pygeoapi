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
import click
import collections

from msc_pygeoapi import util
from msc_pygeoapi.util import strftime_rfc3339, DATETIME_RFC3339_MAPPING


logging.basicConfig()
LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


def create_index(es, index):
    """
    Creates the Elasticsearch index at path. If the index already exists,
    it is deleted and re-created. The mappings for the two types are also
    created.

    :param path: the path to Elasticsearch.
    :param index: the index to be created.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """

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
                                },
                                "DLY_FIRST_DATE": DATETIME_RFC3339_MAPPING,
                                "DLY_LAST_DATE": DATETIME_RFC3339_MAPPING,
                                "FIRST_DATE": DATETIME_RFC3339_MAPPING,
                                "LAST_DATE": DATETIME_RFC3339_MAPPING
                            }
                        },
                        "geometry": {"type": "geo_shape"}
                    }
                }
            }

        index_name = 'climate_station_information'

        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the stations index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'normals':
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
                                "CLIMATE_IDENTIFIER": {
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
                                "E_NORMAL_ELEMENT_NAME": {
                                    "type": "text",
                                    "fields": {
                                        "raw": {"type": "keyword"}
                                    }
                                },
                                "F_NORMAL_ELEMENT_NAME": {
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
                                "FIRST_OCCURRENCE_DATE": DATETIME_RFC3339_MAPPING,  # noqa
                                "DATE_CALCULATED": DATETIME_RFC3339_MAPPING
                            }
                        },
                        "geometry": {"type": "geo_shape"}
                    }
                }
            }

        index_name = 'climate_normals_data'

        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the climate normals index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'monthly_summary':
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
                                "LAST_UPDATED": DATETIME_RFC3339_MAPPING,
                                "LOCAL_DATE": DATETIME_RFC3339_MAPPING
                            }
                        },
                        "geometry": {"type": "geo_shape"}
                    }
                }
            }

        index_name = 'climate_public_climate_summary'

        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the climate monthly summaries index')
        es.indices.create(index=index_name, body=mapping)

    if index == 'daily_summary':
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
                                "LOCAL_DATE": DATETIME_RFC3339_MAPPING
                            }
                        },
                        "geometry": {"type": "geo_shape"}
                    }
                }
            }

        index_name = 'climate_public_daily_data'

        if es.indices.exists(index_name):
            es.indices.delete(index_name)
            LOGGER.info('Deleted the climate daily summaries index')
        es.indices.create(index=index_name, body=mapping)


def generate_stations(cur):
    """
    Queries stations data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    Returns a generator of dictionaries that represent upsert actions
    into Elasticsearch's bulk API.

    :param cur: oracle cursor to perform queries against.
    :returns: generator of bulk API upsert actions.
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
                insert_dict[key] = strftime_rfc3339(insert_dict[key])

        coords = [float(insert_dict['LONGITUDE_DECIMAL_DEGREES']),
                  float(insert_dict['LATITUDE_DECIMAL_DEGREES'])]
        del insert_dict['LONGITUDE_DECIMAL_DEGREES']
        del insert_dict['LATITUDE_DECIMAL_DEGREES']
        climate_identifier = insert_dict['CLIMATE_IDENTIFIER']
        wrapper = {
            'type': 'Feature',
            'properties': insert_dict,
            'geometry': {'type': 'Point', 'coordinates': coords}
        }

        action = {
            '_id': climate_identifier,
            '_index': 'climate_station_information',
            '_op_type': 'update',
            'doc': wrapper,
            'doc_as_upsert': True
        }
        yield action


def generate_normals(cur, stn_dict, normals_dict, periods_dict):
    """
    Queries normals data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    Returns a generator of dictionaries that represent upsert actions
    into Elasticsearch's bulk API.

    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param normals_dict: mapping of normal IDs to normals information.
    :param periods_dict: mapping of normal period IDs to
                         normal period information.
    :returns: generator of bulk API upsert actions.
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
                insert_dict[key] = strftime_rfc3339(insert_dict[key])
        insert_dict['ID'] = '{}.{}.{}'.format(insert_dict['STN_ID'],
                                              insert_dict['NORMAL_ID'],
                                              insert_dict['MONTH'])
        if insert_dict['STN_ID'] in stn_dict:
            coords = stn_dict[insert_dict['STN_ID']]['coordinates']
            insert_dict['STATION_NAME'] = stn_dict[insert_dict['STN_ID']]['STATION_NAME'] # noqa
            insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']]['PROVINCE_CODE'] # noqa
            insert_dict['E_NORMAL_ELEMENT_NAME'] = normals_dict[insert_dict['NORMAL_ID']]['E_NORMAL_ELEMENT_NAME'] # noqa
            insert_dict['F_NORMAL_ELEMENT_NAME'] = normals_dict[insert_dict['NORMAL_ID']]['F_NORMAL_ELEMENT_NAME'] # noqa
            insert_dict['PERIOD'] = normals_dict[insert_dict['NORMAL_ID']]['PERIOD'] # noqa
            insert_dict['PERIOD_BEGIN'] = periods_dict[insert_dict['NORMAL_PERIOD_ID']]['PERIOD_BEGIN'] # noqa
            insert_dict['PERIOD_END'] = periods_dict[insert_dict['NORMAL_PERIOD_ID']]['PERIOD_END'] # noqa
            insert_dict['CLIMATE_IDENTIFIER'] = stn_dict[insert_dict['STN_ID']]['CLIMATE_IDENTIFIER'] # noqa

            del insert_dict['NORMAL_PERIOD_ID']
            wrapper = {'type': 'Feature', 'properties': insert_dict,
                       'geometry': {'type': 'Point', 'coordinates': coords}}
            action = {
                '_id': insert_dict['ID'],
                '_index': 'climate_normals_data',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True
            }
            yield action
        else:
            LOGGER.error('Bad STN ID: {}, skipping records for this station'.format(insert_dict['STN_ID'])) # noqa


def generate_monthly_data(cur, stn_dict, date=None):
    """
    Queries monthly data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    Returns a generator of dictionaries that represent upsert actions
    into Elasticsearch's bulk API.

    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param date: date to start fetching data from.
    :returns: generator of bulk API upsert actions.
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
        insert_dict['LAST_UPDATED'] = strftime_rfc3339(insert_dict['LAST_UPDATED'])  # noqa

        insert_dict['ID'] = '{}.{}.{}'.format(insert_dict['STN_ID'],
                                              insert_dict['LOCAL_YEAR'],
                                              insert_dict['LOCAL_MONTH'])
        if insert_dict['STN_ID'] in stn_dict:
            coords = stn_dict[insert_dict['STN_ID']]['coordinates']
            insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']]['PROVINCE_CODE'] # noqa
            wrapper = {'type': 'Feature', 'properties': insert_dict,
                       'geometry': {'type': 'Point', 'coordinates': coords}}
            action = {
                '_id': insert_dict['ID'],
                '_index': 'climate_public_climate_summary',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True
            }
            yield action
        else:
            LOGGER.error('Bad STN ID: {}, skipping records for this station'.format(insert_dict['STN_ID'])) # noqa


def generate_daily_data(cur, stn_dict, date=None):
    """
    Queries daily data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    Returns a generator of dictionaries that represent upsert actions
    into Elasticsearch's bulk API.

    :param cur: oracle cursor to perform queries against.
    :param stn_dict: mapping of station IDs to station information.
    :param date: date to start fetching data from.
    :returns: generator of bulk API upsert actions.
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
            insert_dict['LOCAL_DATE'] = strftime_rfc3339(insert_dict['LOCAL_DATE']) # noqa

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
                action = {
                    '_id': insert_dict['ID'],
                    '_index': 'climate_public_daily_data',
                    '_op_type': 'update',
                    'doc': wrapper,
                    'doc_as_upsert': True
                }
                yield action
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
                             STATION_NAME, CLIMATE_IDENTIFIER from\
                             CCCS_PORTAL.STATION_INFORMATION\
                             where STN_ID >= {}\
                             order by STN_ID'.format(station))
            else:
                cur.execute('select STN_ID, LONGITUDE_DECIMAL_DEGREES,\
                             LATITUDE_DECIMAL_DEGREES,\
                             ENG_PROV_NAME, FRE_PROV_NAME,\
                             PROV_STATE_TERR_CODE,\
                             STATION_NAME, CLIMATE_IDENTIFIER from\
                             CCCS_PORTAL.STATION_INFORMATION\
                             where STN_ID = {}\
                             order by STN_ID'.format(station))
        else:
            cur.execute('select STN_ID, LONGITUDE_DECIMAL_DEGREES,\
                         LATITUDE_DECIMAL_DEGREES,\
                         ENG_PROV_NAME, FRE_PROV_NAME,\
                         PROV_STATE_TERR_CODE,\
                         STATION_NAME, CLIMATE_IDENTIFIER from\
                         CCCS_PORTAL.STATION_INFORMATION\
                         order by STN_ID')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        stn_dict[row[0]] = {'coordinates': [row[1], row[2]],
                            'ENG_PROV_NAME': row[3],
                            'FRE_PROV_NAME': row[4],
                            'PROVINCE_CODE': row[5].strip(),  # remove the strip # noqa
                            'STATION_NAME': row[6],
                            'CLIMATE_IDENTIFIER': row[7].strip()
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
        cur.execute('select NORMAL_ID, E_NORMAL_ELEMENT_NAME,\
                     F_NORMAL_ELEMENT_NAME, PERIOD from\
                     CCCS_PORTAL.VALID_NORMALS_ELEMENTS')
    except Exception as err:
        LOGGER.error('Could not fetch records from oracle due to: {}.'.format(str(err))) # noqa

    for row in cur:
        normals_dict[row[0]] = {'E_NORMAL_ELEMENT_NAME': row[1],
                                'F_NORMAL_ELEMENT_NAME': row[2],
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
@click.pass_context
@click.option('--db', help='Oracle database connection string.')
@click.option('--es', help='URL to Elasticsearch.')
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
def climate_archive(ctx, db, es, username, password, dataset, station=None,
                    starting_from=False, date=None):
    """
    Loads MSC Climate Archive data into Elasticsearch

    Controls transformation from oracle to Elasticsearch.

    :param db: database connection string.
    :param es: path to Elasticsearch.
    :param username: username for HTTP authentication.
    :param password: password for HTTP authentication.
    :param dataset: name of dataset to load, or all for all datasets.
    :param station: STN_ID of station to index for daily.
    :param starting_from: load all stations after specified station
    :param date: date to start fetching daily and monthly data from.
    """

    auth = (username, password)
    es_client = util.get_es(es, auth)

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
            create_index(es_client, 'stations')
            stations = generate_stations(cur)

            util.submit_elastic_package(es_client, stations)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating normals...')
            create_index(es_client, 'normals')
            normals = generate_normals(cur, stn_dict, normals_dict,
                                       periods_dict)

            util.submit_elastic_package(es_client, normals)
            LOGGER.info('Normals populated.')
        except Exception as err:
            LOGGER.error('Could not populate normals due to: {}.'.format(str(err))) # noqa    

        try:
            LOGGER.info('Populating monthly summary...')
            if not date:
                create_index(es_client, 'monthly_summary')
            monthlies = generate_monthly_data(cur, stn_dict, date)

            util.submit_elastic_package(es_client, monthlies)
            LOGGER.info('Monthly Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly summary due to: {}.'.format(str(err))) # noqa

        try:
            LOGGER.info('Populating daily summary...')
            if not date:
                create_index(es_client, 'daily_summary')
            dailies = generate_daily_data(cur, stn_dict, date)

            util.submit_elastic_package(es_client, dailies)
            LOGGER.info('Daily Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate daily summary due to: {}.'.format(str(err))) # noqa

    elif dataset == 'stations':
        try:
            LOGGER.info('Populating stations...')
            create_index(es_client, 'stations')
            stations = generate_stations(cur)

            util.submit_elastic_package(es_client, stations)
            LOGGER.info('Stations populated.')
        except Exception as err:
            LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa

    elif dataset == 'normals':
        try:
            LOGGER.info('Populating normals...')
            stn_dict = get_station_data(cur, station, starting_from)
            normals_dict = get_normals_data(cur)
            periods_dict = get_normals_periods(cur)
            create_index(es_client, 'normals')
            normals = generate_normals(cur, stn_dict, normals_dict,
                                       periods_dict)

            util.submit_elastic_package(es_client, normals)
            LOGGER.info('Normals populated.')
        except Exception as err:
            LOGGER.error('Could not populate normals due to: {}.'.format(str(err))) # noqa

    elif dataset == 'monthly':
        try:
            LOGGER.info('Populating monthly summary...')
            stn_dict = get_station_data(cur, station, starting_from)
            if not (date or station or starting_from):
                create_index(es_client, 'monthly_summary')
            monthlies = generate_monthly_data(cur, stn_dict, date)

            util.submit_elastic_package(es_client, monthlies)
            LOGGER.info('Monthly Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate monthly summary due to: {}.'.format(str(err))) # noqa

    elif dataset == 'daily':
        try:
            LOGGER.info('Populating daily summary...')
            stn_dict = get_station_data(cur, station, starting_from)
            if not (date or station or starting_from):
                create_index(es_client, 'daily_summary')
            dailies = generate_daily_data(cur, stn_dict, date)

            util.submit_elastic_package(es_client, dailies)
            LOGGER.info('Daily Summary populated.')
        except Exception as err:
            LOGGER.error('Could not populate daily summary due to: {}.'.format(str(err))) # noqa

    else:
        LOGGER.critical('Unknown dataset parameter {}, skipping index population.'.format(dataset)) # noqa

    LOGGER.info('Finished populating indices.')

    con.close()
