# Example Usage:
# python es_loader_hydat.py --db path/to/hydat.sqlite3 --es https://path/to/elasticsearch --username user --password pass # noqa


import logging
import json
import requests
import click
from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.sql import distinct
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import sessionmaker

LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


def zero_pad(val):
    """
    If val is one character long, returns val left padded with a zero.
    Otherwise, returns the string representation of val.

    :param val: the value to be zero-padded.

    :returns: the value with a leading zero if it is one character,
              the string representation of the value otherwise.
    """
    if len(str(val)) == 1:
        return '0{}'.format(val)
    else:
        return str(val)


def create_index(path, index, AUTH):
    """
    Creates the Elasticsearch index at path. If the index already exists,
    it is deleted and re-created. The mappings for the two types are also
    created.

    :param path: path to Elasticsearch.
    :param index: the index(es) to be created.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    if index == 'observations':
        r = requests.delete(path + '/hydrometric_daily_mean',
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete daily means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the daily means index')
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
                                    "STATION_NUMBER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "IDENTIFIER": {
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
                                    "PROV_TERR_STATE_LOC": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "LEVEL_SYMBOL_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "LEVEL_SYMBOL_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FLOW_SYMBOL_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FLOW_SYMBOL_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "FLOW": {
                                        "type": "float"
                                    },
                                    "LEVEL": {
                                        "type": "float"
                                    },
                                    "DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put(path + '/hydrometric_daily_mean',
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create daily means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the daily means index')

        r = requests.delete(path + '/hydrometric_monthly_mean',
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete monthly means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the monthly means index')
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
                                    "STATION_NUMBER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "IDENTIFIER": {
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
                                    "PROV_TERR_STATE_LOC": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM"
                                    },
                                    "MONTHLY_MEAN_FLOW": {
                                        "type": "float"
                                    },
                                    "MONTHLY_MEAN_LEVEL": {
                                        "type": "float"
                                    }
                                }
                            },
                            "geometry": {"type": "geo_shape"}
                        }
                    }
                }
            }

        r = requests.put(path + '/hydrometric_monthly_mean',
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create monthly means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the monthly means index')

    if index == 'annual_statistics':
        r = requests.delete(path + '/hydrometric_annual_statistics',
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete annual statistics due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the annual statistics index')
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
                                    "STATION_NUMBER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "IDENTIFIER": {
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
                                    "PROV_TERR_STATE_LOC": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MIN_DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd"
                                    },
                                    "MAX_DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd"
                                    },
                                    "MIN_VALUE": {
                                        "type": "float"
                                    },
                                    "MAX_VALUE": {
                                        "type": "float"
                                    },
                                    "MIN_SYMBOL_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MIN_SYMBOL_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MAX_SYMBOL_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "MAX_SYMBOL_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DATA_TYPE_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DATA_TYPE_FR": {
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

        r = requests.put(path + '/hydrometric_annual_statistics',
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create annual stats due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the annual stats index')

    if index == 'stations':
        r = requests.delete(path + '/hydrometric_stations',
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
                                    "STATION_NUMBER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "IDENTIFIER": {
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
                                    "PROV_TERR_STATE_LOC": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATUS_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "STATUS_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "CONTRIBUTOR_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "CONTRIBUTOR_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "VERTICAL_DATUM": {
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

        r = requests.put(path + '/hydrometric_stations',
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the stations index')

    if index == 'annual_peaks':
        r = requests.delete(path + '/hydrometric_annual_peaks',
                            auth=AUTH, verify=VERIFY)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not delete annual peaks due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Deleted the annual peaks index')
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
                                    "STATION_NUMBER": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "IDENTIFIER": {
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
                                    "DATE": {
                                        "type": "date",
                                        "format": "yyyy-MM-dd'T'HH:mm||yyy-MM-dd" # noqa
                                    },
                                    "TIMEZONE_OFFSET": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PROV_TERR_STATE_LOC": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DATA_TYPE_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "DATA_TYPE_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PEAK_CODE_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "PEAK_CODE_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "UNITS_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "UNITS_FR": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "SYMBOL_EN": {
                                        "type": "text",
                                        "fields": {
                                            "raw": {"type": "keyword"}
                                        }
                                    },
                                    "SYMBOL_FR": {
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

        r = requests.put(path + '/hydrometric_annual_peaks',
                         data=json.dumps(mapping),
                         auth=AUTH, verify=VERIFY,
                         headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not create annual peaks due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Created the annual peaks index')


def connect_db(db_string):
    """
    Connects to the database.

    :param db_string: the connection information for the database.

    :returns: a tuple containing the engine, SQLAlchemy session and metadata.
    """
    LOGGER.info('Connecting to database {}.'.format(db_string))
    LOGGER.info('Creating engine...')
    engine = create_engine(db_string)
    LOGGER.info('Success. Database engine created.')
    LOGGER.info('Reflecting database schema...')
    metadata = MetaData()
    metadata.reflect(bind=engine)
    LOGGER.info('Success. Schema reflection complete.')
    LOGGER.info('Establishing session...')
    Session = sessionmaker(bind=engine)
    session = Session()
    LOGGER.info('Success. Database session created.')
    return (engine, session, metadata)


def get_table_var(metadata, table_name):
    """
    Gets table object corresponding to table_name.

    :param metadata: db metadata returned by connect_db.
    :param table_name: the name of the db table to find.

    :returns: the table object if table_name is found, nothing otherwise.
    """
    for t in metadata.sorted_tables:
        if t.name == table_name:
            return t


def generate_obs(session, station, var, symbol_table, flow=True):
    """
    Generates a list of flow or level obs for station.
    Each observation in the list is of the form:
    {'Station_number' : 'foo', 'Date' : yyyy-MM-dd, 'Flow'/'Level' : 'X'}.

    :param session: SQLAlchemy session object.
    :param station: the station to generate_obs for.
    :param var: table object to query flow or level data from.
    :param symbol_table: table object to query symbol data from.
    :param flow: boolean to determine whether flow or level data is returned.

    :returns: tuple of lists of dictionaries containing daily obs
              and monthly means for the station passed in in station.
    """
    keys = var.columns.keys()
    symbol_keys = symbol_table.columns.keys()
    args = {'STATION_NUMBER': station}
    # get all obs for station
    obs = session.query(var).filter_by(**args).all()
    lst = []
    mean_lst = []
    if flow:
        word = 'FLOW'
    else:
        word = 'LEVEL'
    for row in obs:
        if flow:
            no_days = row[4]
        else:
            no_days = row[5]
        # get a month's worth of obs for station
        for i in range(1, no_days + 1):
            insert_dict = {'STATION_NUMBER': row[0], 'DATE': '', word: '',
                           'IDENTIFIER': ''}
            date = '{}-{}-{}'.format(str(row[1]),
                                     zero_pad(row[2]),
                                     zero_pad(i))
            insert_dict['DATE'] = date
            insert_dict['IDENTIFIER'] = '{}.{}'.format(row[0], date)
            value = row[keys.index(word.upper() + str(i))]
            symbol = row[keys.index(word.upper() + '_SYMBOL' + str(i))]
            if symbol is not None and symbol.strip():
                args = {'SYMBOL_ID': symbol}
                symbol_data = list(session.query(symbol_table).filter_by(**args).all()[0]) # noqa
                symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
                symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
                insert_dict[word + '_SYMBOL_EN'] = symbol_en
                insert_dict[word + '_SYMBOL_FR'] = symbol_fr
            else:
                insert_dict[word + '_SYMBOL_EN'] = None
                insert_dict[word + '_SYMBOL_FR'] = None
            if value is None:
                insert_dict[word] = None
            else:
                insert_dict[word] = float(value)
            lst.append(insert_dict)
            LOGGER.debug('Generated a daily mean value for date {} and station {}'.format(insert_dict['DATE'], insert_dict['STATION_NUMBER'])) # noqa

            mean_dict = {}
            date = '{}-{}'.format(str(row[1]),
                                  zero_pad(row[2]))
            mean_dict['DATE'] = date
            mean_dict['IDENTIFIER'] = '{}.{}'.format(row[0], date)

            if row[keys.index('MONTHLY_MEAN')]:
                mean_dict['MONTHLY_MEAN_' + word] = float(row[keys.index('MONTHLY_MEAN')]) # noqa
            else:
                mean_dict['MONTHLY_MEAN_' + word] = None
            mean_lst.append(mean_dict)
            LOGGER.debug('Generated a monthly mean value for date {} and station {}'.format(mean_dict['DATE'], insert_dict['STATION_NUMBER'])) # noqa
    return (lst, mean_lst)


def unpivot(session, flow_var, level_var, path, station_table,
            symbol_table, AUTH):
    """
    Unpivots db observations one station at a time, and reformats
    observations so they can be bulk inserted to Elasticsearch.

    :param session: SQLAlchemy session object.
    :param flow_var: table object to query flow data from.
    :param level_var: table object to query level data from.
    :param path: path to Elasticsearch.
    :param station_table: table object to query station data from.
    :param symbol_table: table object to query symbol data from.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    flow_station_codes = [x[0] for x in session.query(distinct(flow_var.c['STATION_NUMBER'])).all()] # noqa
    level_station_codes = [x[0] for x in session.query(distinct(level_var.c['STATION_NUMBER'])).all()] # noqa
    station_codes = list(set(flow_station_codes).union(level_station_codes))
    for station in station_codes:
        LOGGER.debug('Generating flow and level values for station {}'.format(station)) # noqa
        flow_lst, flow_means = generate_obs(session, station, flow_var,
                                            symbol_table)
        level_lst, level_means = generate_obs(session, station, level_var,
                                              symbol_table, False)
        station_keys = station_table.columns.keys()
        args = {'STATION_NUMBER': station}
        # Gather station metadata from the stations table.
        station_metadata = list(session.query(station_table).filter_by(**args).all()[0]) # noqa
        station_name = station_metadata[station_keys.index('STATION_NAME')]
        province = station_metadata[station_keys.index('PROV_TERR_STATE_LOC')]
        station_coords = [float(station_metadata[station_keys.index('LONGITUDE')]), # noqa
                          float(station_metadata[station_keys.index('LATITUDE')])] # noqa
        # combine dictionaries with dates in common
        d = defaultdict(dict)
        for l in (flow_lst, level_lst):
            for elem in l:
                d[elem['DATE']].update(elem)
        comb_list = d.values()
        # add missing flow/level key to any dicts that were
        # not combined (i.e. full outer join)
        wrapper_lst = []
        for item in comb_list:
            if 'LEVEL' not in item:
                item['LEVEL'] = None
                item['LEVEL_SYMBOL_EN'] = None
                item['LEVEL_SYMBOL_FR'] = None
            if 'FLOW' not in item:
                item['FLOW'] = None
                item['FLOW_SYMBOL_EN'] = None
                item['FLOW_SYMBOL_FR'] = None
            wrapper = {'type': 'Feature', 'properties': item,
                       'geometry': {'type': 'Point'}}
            wrapper['properties']['STATION_NAME'] = station_name
            wrapper['properties']['PROV_TERR_STATE_LOC'] = province
            wrapper['geometry']['coordinates'] = station_coords
            wrapper_lst.append(wrapper)
        # turn list into a format suitable for bulk insert
        insert_lst = [x for item in wrapper_lst for x in ({'index': {'_id': item['properties']['IDENTIFIER']}}, item)] # noqa
        ES_str = '\n'.join([json.dumps(x) for x in insert_lst]) + '\n'
        r = requests.post(path + '/hydrometric_daily_mean/FeatureCollection/_bulk', data = ES_str, auth=AUTH, verify=VERIFY) # noqa
        if r.status_code != POST_OK and r.status_code != HTTP_OK:
            LOGGER.error('Could not insert into daily means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully bulk inserted {} records into ES'.format(len(wrapper_lst))) # noqa

        # Insert all monthly means for this station
        d = defaultdict(dict)
        for l in (flow_means, level_means):
            for elem in l:
                d[elem['DATE']].update(elem)
        comb_list = d.values()
        # add missing mean flow/level key to any dicts that were
        # not combined (i.e. full outer join)
        wrapper_lst = []
        for item in comb_list:
            if 'MONTHLY_MEAN_LEVEL' not in item:
                item['MONTHLY_MEAN_LEVEL'] = None
            if 'MONTHLY_MEAN_FLOW' not in item:
                item['MONTHLY_MEAN_FLOW'] = None
            wrapper = {'type': 'Feature', 'properties': item,
                       'geometry': {'type': 'Point'}}
            wrapper['properties']['STATION_NAME'] = station_name
            wrapper['properties']['STATION_NUMBER'] = station
            wrapper['properties']['PROV_TERR_STATE_LOC'] = province
            wrapper['geometry']['coordinates'] = station_coords
            wrapper_lst.append(wrapper)
        # turn list into a format suitable for bulk insert
        insert_lst = [x for item in wrapper_lst for x in ({'index': {'_id': item['properties']['IDENTIFIER']}}, item)] # noqa
        ES_str = '\n'.join([json.dumps(x) for x in insert_lst]) + '\n'
        r = requests.post(path + '/hydrometric_monthly_mean/FeatureCollection/_bulk', data = ES_str, auth=AUTH, verify=VERIFY) # noqa
        if r.status_code != POST_OK and r.status_code != HTTP_OK:
            LOGGER.error('Could not insert into monthly means due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully bulk inserted {} records into ES'.format(len(wrapper_lst))) # noqa


def load_stations(session, metadata, path, station_table, AUTH):
    """
    Queries station data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    :param session: SQLAlchemy session object.
    :param metadata: db metadata returned by connect_db.
    :param path: path to Elasticsearch.
    :param station_table: table object to query station data from.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    station_codes = [x[0] for x in session.query(distinct(station_table.c['STATION_NUMBER'])).all()] # noqa
    for station in station_codes:
        station_keys = station_table.columns.keys()
        args = {'STATION_NUMBER': station}
        # Gather station metadata from the stations table.
        station_metadata = list(session.query(station_table).filter_by(**args).all()[0]) # noqa
        station_name = station_metadata[station_keys.index('STATION_NAME')]
        station_loc = station_metadata[station_keys.index('PROV_TERR_STATE_LOC')] # noqa
        station_status = station_metadata[station_keys.index('HYD_STATUS')]
        station_coords = [float(station_metadata[station_keys.index('LONGITUDE')]), # noqa
                          float(station_metadata[station_keys.index('LATITUDE')])] # noqa
        agency_id = station_metadata[station_keys.index('CONTRIBUTOR_ID')]
        datum_id = station_metadata[station_keys.index('DATUM_ID')]
        if agency_id is not None:
            agency_args = {'AGENCY_ID': agency_id}
            agency_table = get_table_var(metadata, 'AGENCY_LIST')
            agency_keys = agency_table.columns.keys()
            agency_metadata = list(session.query(agency_table).filter_by(**agency_args).all()[0]) # noqa
            agency_en = agency_metadata[agency_keys.index('AGENCY_EN')]
            agency_fr = agency_metadata[agency_keys.index('AGENCY_FR')]
        else:
            agency_en = agency_fr = ''
            LOGGER.warning('Could not find agency information for station {}'.format(station)) # noqa
        if datum_id is not None:
            datum_args = {'DATUM_ID': datum_id}
            datum_table = get_table_var(metadata, 'DATUM_LIST')
            datum_keys = datum_table.columns.keys()
            datum_metadata = list(session.query(datum_table).filter_by(**datum_args).all()[0]) # noqa
            datum_en = datum_metadata[datum_keys.index('DATUM_EN')]
        else:
            datum_en = ''
            LOGGER.warning('Could not find datum information for station {}'.format(station)) # noqa
        if station_status is not None:
            status_args = {'STATUS_CODE': station_status}
            status_table = get_table_var(metadata, 'STN_STATUS_CODES')
            status_keys = status_table.columns.keys()
            status_metadata = list(session.query(status_table).filter_by(**status_args).all()[0]) # noqa
            status_en = status_metadata[status_keys.index('STATUS_EN')]
            status_fr = status_metadata[status_keys.index('STATUS_FR')]
        else:
            status_en = status_fr = ''
            LOGGER.warning('Could not find status information for station {}'.format(station)) # noqa
        metadata_dict = {'type': 'Feature', 'properties':
                         {'STATION_NAME': station_name,
                          'IDENTIFIER': station,
                          'STATION_NUMBER': station,
                          'PROV_TERR_STATE_LOC': station_loc,
                          'STATUS_EN': status_en,
                          'STATUS_FR': status_fr,
                          'CONTRIBUTOR_EN': agency_en,
                          'CONTRIBUTOR_FR': agency_fr,
                          'VERTICAL_DATUM': datum_en},
                         'geometry': {'type': 'Point',
                                      'coordinates': station_coords}
                         }
        r = requests.put(path + '/hydrometric_stations/FeatureCollection/{}'.format(station), # noqa
                                data=json.dumps(metadata_dict),
                                auth=AUTH, verify=VERIFY,
                                headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not insert into stations due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully inserted one station record into ES') # noqa   


def load_annual_stats(session, path, annual_stats_table, data_types_table,
                      station_table, symbol_table, AUTH):
    """
    Queries annual statistics data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    :param session: SQLAlchemy session object.
    :param path: path to Elasticsearch.
    :param annual_stats_table: table object to query annual stats data from.
    :param data_types_table: table object to query data types data from.
    :param station_table: table object to query station data from.
    :param symbol_table: table object to query symbol data from.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    results = session.query(annual_stats_table).group_by(annual_stats_table.c['STATION_NUMBER'], annual_stats_table.c['DATA_TYPE'], annual_stats_table.c['YEAR']).all() # noqa
    results = [list(x) for x in results]
    annual_stats_keys = annual_stats_table.columns.keys()
    station_keys = station_table.columns.keys()
    data_types_keys = data_types_table.columns.keys()
    for result in results:
        station_number = result[annual_stats_keys.index('STATION_NUMBER')]
        data_type = result[annual_stats_keys.index('DATA_TYPE')]
        year = result[annual_stats_keys.index('YEAR')]
        min_month = result[annual_stats_keys.index('MIN_MONTH')]
        min_day = result[annual_stats_keys.index('MIN_DAY')]
        min_value = result[annual_stats_keys.index('MIN')]
        min_symbol = result[annual_stats_keys.index('MIN_SYMBOL')]
        max_month = result[annual_stats_keys.index('MAX_MONTH')]
        max_day = result[annual_stats_keys.index('MAX_DAY')]
        max_value = result[annual_stats_keys.index('MAX')]
        max_symbol = result[annual_stats_keys.index('MAX_SYMBOL')]
        args = {'STATION_NUMBER': station_number}
        station_metadata = list(session.query(station_table).filter_by(**args).all()[0]) # noqa
        args = {'DATA_TYPE': data_type}
        data_type_metadata = list(session.query(data_types_table).filter_by(**args).all()[0]) # noqa
        station_name = station_metadata[station_keys.index('STATION_NAME')]
        province = station_metadata[station_keys.index('PROV_TERR_STATE_LOC')]
        station_coords = [float(station_metadata[station_keys.index('LONGITUDE')]), # noqa
                          float(station_metadata[station_keys.index('LATITUDE')])] # noqa
        data_type_en = data_type_metadata[data_types_keys.index('DATA_TYPE_EN')] # noqa
        data_type_fr = data_type_metadata[data_types_keys.index('DATA_TYPE_FR')] # noqa
        if min_month is None or min_day is None:
            min_date = None
            LOGGER.warning('Could not find min date for station {}'.format(station_number)) # noqa
        else:
            min_date = '{}-{}-{}'.format(year, zero_pad(min_month),
                                         zero_pad(min_day))
        if max_month is None or max_day is None:
            max_date = None
            LOGGER.warning('Could not find max date for station {}'.format(station_number)) # noqa
        else:
            max_date = '{}-{}-{}'.format(year, zero_pad(max_month),
                                         zero_pad(max_day))
        symbol_keys = symbol_table.columns.keys()
        if min_symbol is not None and min_symbol.strip():
            args = {'SYMBOL_ID': min_symbol}
            symbol_data = list(session.query(symbol_table).filter_by(**args).all()[0]) # noqa
            min_symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
            min_symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
        else:
            min_symbol_en = min_symbol_fr = ''
            LOGGER.warning('Could not find min symbol for station {}'.format(station_number)) # noqa
        if max_symbol is not None and max_symbol.strip():
            args = {'SYMBOL_ID': max_symbol}
            symbol_data = list(session.query(symbol_table).filter_by(**args).all()[0]) # noqa
            max_symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
            max_symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
        else:
            max_symbol_en = max_symbol_fr = ''
            LOGGER.warning('Could not find max symbol for station {}'.format(station_number)) # noqa
        if data_type_en == 'Water Level':
            es_id = '{}.{}.level-niveaux'.format(station_number, year)
        elif data_type_en == 'Flow':
            es_id = '{}.{}.flow-debit'.format(station_number, year)
        elif data_type_en == 'Sediment in mg/L':
            es_id = '{}.{}.sediment-sediment'.format(station_number, year)
        elif data_type_en == 'Daily Mean Tonnes':
            es_id = '{}.{}.tonnes-tonnes'.format(station_number, year)
        else:
            es_id = '{}.{}.None'.format(station_number, year)
        metadata_dict = {'type': 'Feature', 'properties':
                         {'STATION_NAME': station_name,
                          'IDENTIFIER': es_id,
                          'STATION_NUMBER': station_number,
                          'PROV_TERR_STATE_LOC': province,
                          'DATA_TYPE_EN': data_type_en,
                          'DATA_TYPE_FR': data_type_fr,
                          'MIN_DATE': min_date,
                          'MIN_VALUE': float(min_value) if min_value else None,
                          'MIN_SYMBOL_EN': min_symbol_en,
                          'MIN_SYMBOL_FR': min_symbol_fr,
                          'MAX_DATE': max_date,
                          'MAX_VALUE': float(max_value) if max_value else None,
                          'MAX_SYMBOL_EN': max_symbol_en,
                          'MAX_SYMBOL_FR': max_symbol_fr},
                         'geometry': {'type': 'Point',
                                      'coordinates': station_coords}
                         }
        r = requests.put(path + '/hydrometric_annual_statistics/FeatureCollection/{}'.format(es_id), # noqa
                                data=json.dumps(metadata_dict),
                                auth=AUTH, verify=VERIFY,
                                headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not insert into annual stats due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully inserted one annual stats record into ES') # noqa 


def load_annual_peaks(session, metadata, path, annual_peaks_table,
                      data_types_table, symbol_table, station_table, AUTH):
    """
    Queries annual peaks data from the db, and reformats
    data so it can be inserted into Elasticsearch.

    :param session: SQLAlchemy session object.
    :param metadata: db metadata returned by connect_db.
    :param path: path to Elasticsearch.
    :param annual_peaks_table: table object to query annual peaks data from.
    :param data_types_table: table object to query data types data from.
    :param symbol_table: table object to query symbol data from.
    :param station_table: table object to query station data from.
    :param AUTH: tuple of username and password used to authorize the
                 HTTP request.
    """
    tz_map = {None: None, '*': None, '0': None, 'AKST': '-9', 'AST': '-4',
              'CST': '-6', 'EST': '-5', 'MDT': '-6', 'MST': '-7',
              'NST': '-3.5', 'PST': '-8', 'YST': '-9'}
    annual_peaks_keys = annual_peaks_table.columns.keys()
    station_keys = station_table.columns.keys()
    data_types_keys = data_types_table.columns.keys()
    results = session.query(annual_peaks_table).group_by(annual_peaks_table.c['STATION_NUMBER'], annual_peaks_table.c['DATA_TYPE'], annual_peaks_table.c['YEAR'], annual_peaks_table.c['PEAK_CODE']).all() # noqa
    results = [list(x) for x in results]
    for result in results:
        station_number = result[annual_peaks_keys.index('STATION_NUMBER')]
        data_type = result[annual_peaks_keys.index('DATA_TYPE')]
        year = result[annual_peaks_keys.index('YEAR')]
        peak_id = result[annual_peaks_keys.index('PEAK_CODE')]
        unit_id = result[annual_peaks_keys.index('PRECISION_CODE')]
        month = result[annual_peaks_keys.index('MONTH')]
        day = result[annual_peaks_keys.index('DAY')]
        hour = result[annual_peaks_keys.index('HOUR')]
        minute = result[annual_peaks_keys.index('MINUTE')]
        time_zone = tz_map[result[annual_peaks_keys.index('TIME_ZONE')]]
        symbol_id = result[annual_peaks_keys.index('SYMBOL')]
        if month is None or day is None:
            date = None
            LOGGER.warning('Could not find date for station {}'.format(station_number)) # noqa
        elif hour is None or minute is None:
            date = '{}-{}-{}'.format(year, zero_pad(month), zero_pad(day))
        else:
            date = '{}-{}-{}T{}:{}'.format(year, zero_pad(month),
                                           zero_pad(day), zero_pad(hour),
                                           zero_pad(minute))
        args = {'STATION_NUMBER': station_number}
        try:
            station_metadata = list(session.query(station_table).filter_by(**args).all()[0]) # noqa
            station_name = station_metadata[station_keys.index('STATION_NAME')]
            province = station_metadata[station_keys.index('PROV_TERR_STATE_LOC')]
            station_coords = [float(station_metadata[station_keys.index('LONGITUDE')]), # noqa
                          float(station_metadata[station_keys.index('LATITUDE')])] # noqa
        except Exception:
            station_name = None
            province = None
            station_coords = [None, None]
            LOGGER.warning('Could not find station information for station {}'.format(station_number)) # noqa
        args = {'DATA_TYPE': data_type}
        data_type_metadata = list(session.query(data_types_table).filter_by(**args).all()[0]) # noqa
        data_type_en = data_type_metadata[data_types_keys.index('DATA_TYPE_EN')] # noqa
        data_type_fr = data_type_metadata[data_types_keys.index('DATA_TYPE_FR')] # noqa
        if unit_id:
            unit_codes = get_table_var(metadata, 'PRECISION_CODES')
            unit_keys = unit_codes.columns.keys()
            args = {'PRECISION_CODE': unit_id}
            unit_data = list(session.query(unit_codes).filter_by(**args).all()[0]) # noqa
            unit_en = unit_data[unit_keys.index('PRECISION_EN')]
            unit_fr = unit_data[unit_keys.index('PRECISION_FR')]
        else:
            unit_en = unit_fr = None
            LOGGER.warning('Could not find units for station {}'.format(station_number)) # noqa
        if peak_id:
            peak_codes = get_table_var(metadata, 'PEAK_CODES')
            peak_keys = peak_codes.columns.keys()
            args = {'PEAK_CODE': peak_id}
            peak_data = list(session.query(peak_codes).filter_by(**args).all()[0]) # noqa
            peak_en = peak_data[peak_keys.index('PEAK_EN')]
            peak_fr = peak_data[peak_keys.index('PEAK_FR')]
        else:
            peak_en = peak_fr = None
            LOGGER.warning('Could not find peaks for station {}'.format(station_number)) # noqa
        if symbol_id and symbol_id.strip():
            symbol_keys = symbol_table.columns.keys()
            args = {'SYMBOL_ID': symbol_id}
            symbol_data = list(session.query(symbol_table).filter_by(**args).all()[0]) # noqa
            symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
            symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
        else:
            symbol_en = symbol_fr = None
            LOGGER.warning('Could not find symbol for station {}'.format(station_number)) # noqa
        if peak_en == 'Maximum':
            peak = 'maximum-maximale'
        elif peak_en == 'Minimum':
            peak = 'minimum-minimale'
        else:
            peak = None

        if data_type_en == 'Water Level':
            es_id = '{}.{}.level-niveaux.{}'.format(station_number, year, peak)
        elif data_type_en == 'Flow':
            es_id = '{}.{}.flow-debit.{}'.format(station_number, year, peak)
        elif data_type_en == 'Sediment in mg/L':
            es_id = '{}.{}.sediment-sediment.{}'.format(station_number,
                                                        year, peak)
        elif data_type_en == 'Daily Mean Tonnes':
            es_id = '{}.{}.tonnes-tonnes.{}'.format(station_number, year, peak)
        else:
            es_id = '{}.{}.None'.format(station_number, year)
        metadata_dict = {'type': 'Feature', 'properties':
                         {'STATION_NAME': station_name,
                          'STATION_NUMBER': station_number,
                          'PROV_TERR_STATE_LOC': province,
                          'IDENTIFIER': es_id,
                          'DATA_TYPE_EN': data_type_en,
                          'DATA_TYPE_FR': data_type_fr,
                          'DATE': date,
                          'TIMEZONE_OFFSET': time_zone,
                          'PEAK_CODE_EN': peak_en,
                          'PEAK_CODE_FR': peak_fr,
                          'UNITS_EN': unit_en,
                          'UNITS_FR': unit_fr,
                          'SYMBOL_EN': symbol_en,
                          'SYMBOL_FR': symbol_fr},
                         'geometry': {'type': 'Point',
                                      'coordinates': station_coords}
                         }
        r = requests.put(path + '/hydrometric_annual_peaks/FeatureCollection/{}'.format(es_id), # noqa
                     data=json.dumps(metadata_dict),
                     auth=AUTH, verify=VERIFY, headers=HEADERS)
        if r.status_code != HTTP_OK and r.status_code != POST_OK:
            LOGGER.error('Could not insert into annual peaks due to: {}'.format(r.text)) # noqa
        else:
            LOGGER.info('Successfully inserted one annual peaks record into ES') # noqa 


@click.command()
@click.option('--db', type=click.Path(exists=True, resolve_path=True),
              help='Path to Hydat sqlite database')
@click.option('--es', help='URL to Elasticsearch')
@click.option('--username', help='Username to connect to HTTPS')
@click.option('--password', help='Password to connect to HTTPS')
def cli(db, es, username, password):
    """
    Controls transformation from sqlite to Elasticsearch.

    :param db: database connection string.
    :param es: path to Elasticsearch.
    :param username: username for HTTP authentication.
    :param password: password for HTTP authentication.
    """
    AUTH = (username, password)
    try:
        engine, session, metadata = connect_db('sqlite:///{}'.format(db))
    except Exception as err:
        LOGGER.critical('Could not connect to database due to: {}. Exiting.').format(str(err)) # noqa
        return None
    try:
        LOGGER.info('Accessing SQLite tables...')
        flow_var = level_var = station_table = None
        level_var = get_table_var(metadata, 'DLY_LEVELS')
        flow_var = get_table_var(metadata, 'DLY_FLOWS')
        station_table = get_table_var(metadata, 'STATIONS')
        data_types_table = get_table_var(metadata, 'DATA_TYPES')
        annual_stats_table = get_table_var(metadata, 'ANNUAL_STATISTICS')
        symbol_table = get_table_var(metadata, 'DATA_SYMBOLS')
        annual_peaks_table = get_table_var(metadata, 'ANNUAL_INSTANT_PEAKS')
        LOGGER.info('Success. Created table variables.')
    except Exception as err:
        LOGGER.critical('Could not create table variables due to: {}. Exiting.').format(str(err)) # noqa
        return None
    try:
        LOGGER.info('Populating stations index...')
        create_index(es, 'stations', AUTH)
        load_stations(session, metadata, es, station_table, AUTH)
        LOGGER.info('Stations index populated.')
    except Exception as err:
        LOGGER.error('Could not populate stations due to: {}.'.format(str(err))) # noqa
    try:
        LOGGER.info('Populating observations...')
        create_index(es, 'observations', AUTH)
        unpivot(session, flow_var, level_var, es, station_table, symbol_table, AUTH) # noqa
        LOGGER.info('Observations populated.')
    except Exception as err:
        LOGGER.error('Could not populate observations due to: {}.'.format(str(err))) # noqa
    try:
        LOGGER.info('Populating annual statistics index...')
        create_index(es, 'annual_statistics', AUTH)
        load_annual_stats(session, es, annual_stats_table, data_types_table, station_table, symbol_table, AUTH) # noqa
        LOGGER.info('Annual stastistics index populated.')
    except Exception as err:
        LOGGER.error('Could not populate annual statistics due to: {}.'.format(str(err))) # noqa
    try:
        LOGGER.info('Populating peaks index...')
        create_index(es, 'annual_peaks', AUTH)
        load_annual_peaks(session, metadata, es, annual_peaks_table, data_types_table, symbol_table, station_table, AUTH) # noqa
        LOGGER.info('Annual peaks index populated.')
    except Exception as err:
        LOGGER.error('Could not populate annual peaks due to: {}.'.format(str(err))) # noqa
    LOGGER.info('Finished populating indices.')


cli()
