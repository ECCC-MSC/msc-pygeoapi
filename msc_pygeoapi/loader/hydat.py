# =================================================================
#
# Author: Alex Hurka <alex.hurka@canada.ca>
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2019 Alex Hurka
# Copyright (c) 2020 Etienne Pelletier
# Copyright (c) 2021 Tom Kralidis

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

from collections import defaultdict
import logging

import click

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import distinct

from msc_pygeoapi import cli_options
from msc_pygeoapi.env import (MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH,
                              MSC_PYGEOAPI_LOGGING_LOGLEVEL,
                              MSC_PYGEOAPI_OGC_API_URL)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import get_es, submit_elastic_package


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}
# Needs to be fixed.
VERIFY = False


class HydatLoader(BaseLoader):
    """Climat Archive Loader"""

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

        self.db_string = 'sqlite:///{}'.format(plugin_def['db_string'])

        self.engine, self.session, self.metadata = self.connect_db()

    def zero_pad(self, val):
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

    def create_index(self, index):
        """
        Creates the Elasticsearch index named <index>. If the index already
        exists, it is deleted and re-created. The mappings for the two types
        are also created.

        :param es: elasticsearch.Elasticsearch client.
        :param index: name for the index(es) to be created.
        """
        if index == 'observations':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STATION_NUMBER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROV_TERR_STATE_LOC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LEVEL_SYMBOL_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LEVEL_SYMBOL_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DISCHARGE_SYMBOL_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DISCHARGE_SYMBOL_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DISCHARGE": {"type": "float"},
                                "LEVEL": {"type": "float"},
                                "DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd",
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'hydrometric_daily_mean'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the daily observations index')
            self.ES.indices.create(index=index_name, body=mapping)

            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STATION_NUMBER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROV_TERR_STATE_LOC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATE": {"type": "date", "format": "yyyy-MM"},
                                "MONTHLY_MEAN_DISCHARGE": {"type": "float"},
                                "MONTHLY_MEAN_LEVEL": {"type": "float"},
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'hydrometric_monthly_mean'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the monthly observations index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'annual_statistics':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STATION_NUMBER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROV_TERR_STATE_LOC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MIN_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd",
                                },
                                "MAX_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd",
                                },
                                "MIN_VALUE": {"type": "float"},
                                "MAX_VALUE": {"type": "float"},
                                "MIN_SYMBOL_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MIN_SYMBOL_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MAX_SYMBOL_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MAX_SYMBOL_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATA_TYPE_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATA_TYPE_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'hydrometric_annual_statistics'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the annual statistics index')
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
                                "STATION_NUMBER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROV_TERR_STATE_LOC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATUS_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATUS_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "CONTRIBUTOR_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "CONTRIBUTOR_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "VERTICAL_DATUM": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'hydrometric_stations'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the stations index')
            self.ES.indices.create(index=index_name, body=mapping)

        if index == 'annual_peaks':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STATION_NUMBER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd'T'HH:mm||yyy-MM-dd",
                                },
                                "TIMEZONE_OFFSET": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROV_TERR_STATE_LOC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATA_TYPE_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DATA_TYPE_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PEAK_CODE_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PEAK_CODE_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PEAK": {"type": "float"},
                                "UNITS_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "UNITS_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "SYMBOL_EN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "SYMBOL_FR": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'hydrometric_annual_peaks'
            if self.ES.indices.exists(index_name):
                self.ES.indices.delete(index_name)
                LOGGER.info('Deleted the annual peaks index')
            self.ES.indices.create(index=index_name, body=mapping)

    def connect_db(self):
        """
        Connects to the database.

        :param db_string: the connection information for the database.

        :returns: a tuple containing the engine, SQLAlchemy session and
                  metadata.
        """

        try:
            LOGGER.info('Connecting to database {}.'.format(self.db_string))
            LOGGER.info('Creating engine...')
            engine = create_engine(self.db_string)
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
        except Exception as err:
            LOGGER.critical(
                f'Could not connect to database due to: {str(err)}. Exiting.'
            )
            raise err

    def get_table_var(self, table_name):
        """
        Gets table object corresponding to table_name.

        :param table_name: the name of the db table to find.

        :returns: the table object if table_name is found, nothing otherwise.
        """
        for t in self.metadata.sorted_tables:
            if t.name == table_name:
                return t

    def generate_obs(self, station, var, symbol_table, discharge=True):
        """
        Generates a list of discharge or level obs for station.
        Each observation in the list is of the form:
        {'Station_number' : 'foo', 'Date' : yyyy-MM-dd,
        'Discharge'/'Level' : 'X'}.

        :param station: the station to generate_obs for.
        :param var: table object to query discharge or level data from.
        :param symbol_table: table object to query symbol data from.
        :param discharge: boolean to determine whether discharge or level
                        data is returned.

        :returns: tuple of lists of dictionaries containing daily obs
                and monthly means for the station passed in as <station>.
        """
        keys = var.columns.keys()
        symbol_keys = symbol_table.columns.keys()
        args = {'STATION_NUMBER': station}
        # get all obs for station
        obs = self.session.query(var).filter_by(**args).all()
        lst = []
        mean_lst = []
        if discharge:
            word_in = 'FLOW'
            word_out = 'DISCHARGE'
        else:
            word_in = 'LEVEL'
            word_out = 'LEVEL'
        for row in obs:
            if discharge:
                no_days = row[4]
            else:
                no_days = row[5]
            # get a month's worth of obs for station
            for i in range(1, no_days + 1):
                insert_dict = {
                    'STATION_NUMBER': row[0],
                    'DATE': '',
                    word_out: '',
                    'IDENTIFIER': '',
                }
                date = '{}-{}-{}'.format(
                    str(row[1]), self.zero_pad(row[2]), self.zero_pad(i)
                )
                insert_dict['DATE'] = date
                insert_dict['IDENTIFIER'] = '{}.{}'.format(row[0], date)
                value = row[keys.index(word_in.upper() + str(i))]
                symbol = row[keys.index(word_in.upper() + '_SYMBOL' + str(i))]
                if symbol is not None and symbol.strip():
                    args = {'SYMBOL_ID': symbol}
                    symbol_data = list(
                        self.session.query(symbol_table)
                        .filter_by(**args)
                        .all()[0]
                    )
                    symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
                    symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
                    insert_dict[word_out + '_SYMBOL_EN'] = symbol_en
                    insert_dict[word_out + '_SYMBOL_FR'] = symbol_fr
                else:
                    insert_dict[word_out + '_SYMBOL_EN'] = None
                    insert_dict[word_out + '_SYMBOL_FR'] = None
                if value is None:
                    insert_dict[word_out] = None
                else:
                    insert_dict[word_out] = float(value)
                lst.append(insert_dict)
                LOGGER.debug(
                    f'Generated a daily mean value for date '
                    f'{insert_dict["DATE"]} and station '
                    f'{insert_dict["STATION_NUMBER"]}'
                )

                mean_dict = {}
                date = '{}-{}'.format(str(row[1]), self.zero_pad(row[2]))
                mean_dict['DATE'] = date
                mean_dict['IDENTIFIER'] = '{}.{}'.format(row[0], date)

                if row[keys.index('MONTHLY_MEAN')]:
                    mean_dict['MONTHLY_MEAN_' + word_out] = float(
                        row[keys.index('MONTHLY_MEAN')]
                    )
                else:
                    mean_dict['MONTHLY_MEAN_' + word_out] = None
                mean_lst.append(mean_dict)
                LOGGER.debug(
                    f'Generated a monthly mean value for date '
                    f'{mean_dict["DATE"]} and station '
                    f'{insert_dict["STATION_NUMBER"]}'
                )
        return (lst, mean_lst)

    def generate_means(
        self, discharge_var, level_var, station_table, symbol_table
    ):
        """
        Unpivots db observations one station at a time, and reformats
        observations so they can be bulk inserted to Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        Generates documents for both daily and monthly means indexes.

        :param discharge_var: table object to query discharge data from.
        :param level_var: table object to query level data from.
        :param station_table: table object to query station data from.
        :param symbol_table: table object to query symbol data from.
        :returns: generator of bulk API upsert actions.
        """
        discharge_station_codes = [
            x[0]
            for x in self.session.query(
                distinct(discharge_var.c['STATION_NUMBER'])
            ).all()
        ]
        level_station_codes = [
            x[0]
            for x in self.session.query(
                distinct(level_var.c['STATION_NUMBER'])
            ).all()
        ]
        station_codes = list(
            set(discharge_station_codes).union(level_station_codes)
        )
        for station in station_codes:
            LOGGER.debug(
                'Generating discharge and level values for station {}'.format(
                    station
                )
            )
            discharge_lst, discharge_means = self.generate_obs(
                station, discharge_var, symbol_table, True
            )
            level_lst, level_means = self.generate_obs(
                station, level_var, symbol_table, False
            )
            station_keys = station_table.columns.keys()
            args = {'STATION_NUMBER': station}
            # Gather station metadata from the stations table.
            station_metadata = list(
                self.session.query(station_table).filter_by(**args).all()[0]
            )
            station_name = station_metadata[station_keys.index('STATION_NAME')]
            province = station_metadata[
                station_keys.index('PROV_TERR_STATE_LOC')
            ]
            station_coords = [
                float(station_metadata[station_keys.index('LONGITUDE')]),
                float(station_metadata[station_keys.index('LATITUDE')]),
            ]
            # combine dictionaries with dates in common
            d = defaultdict(dict)
            for el in (discharge_lst, level_lst):
                for elem in el:
                    d[elem['DATE']].update(elem)
            comb_list = d.values()
            # add missing discharge/level key to any dicts that were
            # not combined (i.e. full outer join)
            for item in comb_list:
                if 'LEVEL' not in item:
                    item['LEVEL'] = None
                    item['LEVEL_SYMBOL_EN'] = None
                    item['LEVEL_SYMBOL_FR'] = None
                if 'DISCHARGE' not in item:
                    item['DISCHARGE'] = None
                    item['DISCHARGE_SYMBOL_EN'] = None
                    item['DISCHARGE_SYMBOL_FR'] = None
                wrapper = {
                    'type': 'Feature',
                    'properties': item,
                    'geometry': {'type': 'Point'},
                }
                wrapper['properties']['STATION_NAME'] = station_name
                wrapper['properties']['PROV_TERR_STATE_LOC'] = province
                wrapper['geometry']['coordinates'] = station_coords
                action = {
                    '_id': item['IDENTIFIER'],
                    '_index': 'hydrometric_daily_mean',
                    '_op_type': 'update',
                    'doc': wrapper,
                    'doc_as_upsert': True,
                }
                yield action

            # Insert all monthly means for this station
            d = defaultdict(dict)
            for el in (discharge_means, level_means):
                for elem in el:
                    d[elem['DATE']].update(elem)
            comb_list = d.values()
            # add missing mean discharge/level key to any dicts that were
            # not combined (i.e. full outer join)
            for item in comb_list:
                if 'MONTHLY_MEAN_LEVEL' not in item:
                    item['MONTHLY_MEAN_LEVEL'] = None
                if 'MONTHLY_MEAN_DISCHARGE' not in item:
                    item['MONTHLY_MEAN_DISCHARGE'] = None
                wrapper = {
                    'type': 'Feature',
                    'properties': item,
                    'geometry': {'type': 'Point'},
                }
                wrapper['properties']['STATION_NAME'] = station_name
                wrapper['properties']['STATION_NUMBER'] = station
                wrapper['properties']['PROV_TERR_STATE_LOC'] = province
                wrapper['geometry']['coordinates'] = station_coords
                action = {
                    '_id': item['IDENTIFIER'],
                    '_index': 'hydrometric_monthly_mean',
                    '_op_type': 'update',
                    'doc': wrapper,
                    'doc_as_upsert': True,
                }
                yield action

    def generate_stations(self, station_table):
        """
        Queries station data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param station_table: table object to query station data from.
        :returns: generator of bulk API upsert actions.
        """

        url = MSC_PYGEOAPI_OGC_API_URL

        station_codes = [
            x[0]
            for x in self.session.query(
                distinct(station_table.c['STATION_NUMBER'])
            ).all()
        ]
        for station in station_codes:
            station_keys = station_table.columns.keys()
            args = {'STATION_NUMBER': station}
            # Gather station metadata from the stations table.
            station_metadata = list(
                self.session.query(station_table).filter_by(**args).all()[0]
            )
            station_name = station_metadata[station_keys.index('STATION_NAME')]
            station_loc = station_metadata[
                station_keys.index('PROV_TERR_STATE_LOC')
            ]
            station_status = station_metadata[station_keys.index('HYD_STATUS')]
            station_coords = [
                float(station_metadata[station_keys.index('LONGITUDE')]),
                float(station_metadata[station_keys.index('LATITUDE')]),
            ]
            agency_id = station_metadata[station_keys.index('CONTRIBUTOR_ID')]
            datum_id = station_metadata[station_keys.index('DATUM_ID')]
            if agency_id is not None:
                agency_args = {'AGENCY_ID': agency_id}
                agency_table = self.get_table_var('AGENCY_LIST')
                agency_keys = agency_table.columns.keys()
                agency_metadata = list(
                    self.session.query(agency_table)
                    .filter_by(**agency_args)
                    .all()[0]
                )
                agency_en = agency_metadata[agency_keys.index('AGENCY_EN')]
                agency_fr = agency_metadata[agency_keys.index('AGENCY_FR')]
            else:
                agency_en = agency_fr = ''
                LOGGER.warning(
                    'Could not find agency information for station {}'.format(
                        station
                    )
                )
            if datum_id is not None:
                datum_args = {'DATUM_ID': datum_id}
                datum_table = self.get_table_var('DATUM_LIST')
                datum_keys = datum_table.columns.keys()
                datum_metadata = list(
                    self.session.query(datum_table)
                    .filter_by(**datum_args)
                    .all()[0]
                )
                datum_en = datum_metadata[datum_keys.index('DATUM_EN')]
            else:
                datum_en = ''
                LOGGER.warning(
                    'Could not find datum information for station {}'.format(
                        station
                    )
                )
            if station_status is not None:
                status_args = {'STATUS_CODE': station_status}
                status_table = self.get_table_var('STN_STATUS_CODES')
                status_keys = status_table.columns.keys()
                status_metadata = list(
                    self.session.query(status_table)
                    .filter_by(**status_args)
                    .all()[0]
                )
                status_en = status_metadata[status_keys.index('STATUS_EN')]
                status_fr = status_metadata[status_keys.index('STATUS_FR')]
            else:
                status_en = status_fr = ''
                LOGGER.warning(
                    'Could not find status information for station {}'.format(
                        station
                    )
                )

            insert_dict = {
                'type': 'Feature',
                'properties': {
                    'STATION_NAME': station_name,
                    'IDENTIFIER': station,
                    'STATION_NUMBER': station,
                    'PROV_TERR_STATE_LOC': station_loc,
                    'STATUS_EN': status_en,
                    'STATUS_FR': status_fr,
                    'CONTRIBUTOR_EN': agency_en,
                    'CONTRIBUTOR_FR': agency_fr,
                    'VERTICAL_DATUM': datum_en,
                    'links': [
                        {
                            'type': 'text/html',
                            'rel': 'related',
                            'title': f'Station Information for {station_name} ({station})',  # noqa
                            'href': f'{url}/collections/hydrometric-stations/items?STATION_NUMBER={station}&f=html',  # noqa
                        },
                        {
                            'type': 'application/json',
                            'rel': 'related',
                            'title': f'Daily Mean of Water Level or Discharge for {station_name} ({station})',  # noqa
                            'href': f'{url}/collections/hydrometric-daily-mean/items?STATION_NUMBER={station}',  # noqa
                        },
                        {
                            'type': 'application/json',
                            'rel': 'related',
                            'title': f'Monthly Mean of Water Level or Discharge for {station_name} ({station})',  # noqa
                            'href': f'{url}/collections/hydrometric-monthly-mean/items?STATION_NUMBER={station}',  # noqa
                        },
                        {
                            'type': 'application/json',
                            'rel': 'related',
                            'title': f'Annual Maximum and Minimum Instantaneous Water Level or Discharge for {station_name} ({station})',  # noqa
                            'href': f'{url}/collections/hydrometric-annual-peaks/items?STATION_NUMBER={station}',  # noqa
                        },
                        {
                            'type': 'application/json',
                            'rel': 'related',
                            'title': f'Annual Maximum and Minimum Daily Water Level or Discharge for {station_name} ({station})',  # noqa
                            'href': f'{url}/collections/hydrometric-annual-statistics/items?STATION_NUMBER={station}',  # noqa
                        },
                        {
                            'type': 'text/html',
                            'rel': 'alternate',
                            'title': 'Station Information for {} ({})'.format(
                                station_name, station
                            ),
                            'href': f'https://wateroffice.ec.gc.ca/report/historical_e.html?stn={station}',  # noqa
                            'hreflang': 'en-CA',
                        },
                        {
                            'type': 'text/html',
                            'rel': 'alternate',
                            'title': f'Informations pour la station {station_name} ({station})',  # noqa
                            'href': f'https://wateroffice.ec.gc.ca/report/historical_f.html?stn={station}',  # noqa
                            'hreflang': 'fr-CA',
                        },
                    ],
                },
                'geometry': {'type': 'Point', 'coordinates': station_coords},
            }

            action = {
                '_id': station,
                '_index': 'hydrometric_stations',
                '_op_type': 'update',
                'doc': insert_dict,
                'doc_as_upsert': True,
            }
            yield action

    def generate_annual_stats(
        self, annual_stats_table, data_types_table, station_table, symbol_table
    ):
        """
        Queries annual statistics data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param session: SQLAlchemy session object.
        :param annual_stats_table: table object to query annual stats data
                                   from.
        :param data_types_table: table object to query data types data from.
        :param station_table: table object to query station data from.
        :param symbol_table: table object to query symbol data from.
        :returns: generator of bulk API upsert actions.
        """
        results = (
            self.session.query(annual_stats_table)
            .group_by(
                annual_stats_table.c['STATION_NUMBER'],
                annual_stats_table.c['DATA_TYPE'],
                annual_stats_table.c['YEAR'],
            )
            .all()
        )
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

            if min_value is not None:
                min_value = float(min_value)
            if max_value is not None:
                max_value = float(max_value)

            args = {'STATION_NUMBER': station_number}
            station_metadata = list(
                self.session.query(station_table).filter_by(**args).all()[0]
            )
            args = {'DATA_TYPE': data_type}
            data_type_metadata = list(
                self.session.query(data_types_table).filter_by(**args).all()[0]
            )
            station_name = station_metadata[station_keys.index('STATION_NAME')]
            province = station_metadata[
                station_keys.index('PROV_TERR_STATE_LOC')
            ]
            station_coords = [
                float(station_metadata[station_keys.index('LONGITUDE')]),
                float(station_metadata[station_keys.index('LATITUDE')]),
            ]
            data_type_en = data_type_metadata[
                data_types_keys.index('DATA_TYPE_EN')
            ]
            data_type_fr = data_type_metadata[
                data_types_keys.index('DATA_TYPE_FR')
            ]
            if data_type_en == 'Flow':
                data_type_en = 'Discharge'
            if min_month is None or min_day is None:
                min_date = None
                LOGGER.warning(
                    f'Could not find min date for station {station_number}'
                )
            else:
                min_date = '{}-{}-{}'.format(
                    year, self.zero_pad(min_month), self.zero_pad(min_day)
                )
            if max_month is None or max_day is None:
                max_date = None
                LOGGER.warning(
                    f'Could not find max date for station {station_number}'
                )
            else:
                max_date = '{}-{}-{}'.format(
                    year, self.zero_pad(max_month), self.zero_pad(max_day)
                )
            symbol_keys = symbol_table.columns.keys()
            if min_symbol is not None and min_symbol.strip():
                args = {'SYMBOL_ID': min_symbol}
                symbol_data = list(
                    self.session.query(symbol_table).filter_by(**args).all()[0]
                )
                min_symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
                min_symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
            else:
                min_symbol_en = min_symbol_fr = ''
                LOGGER.warning(
                    f'Could not find min symbol for station {station_number}'
                )
            if max_symbol is not None and max_symbol.strip():
                args = {'SYMBOL_ID': max_symbol}
                symbol_data = list(
                    self.session.query(symbol_table).filter_by(**args).all()[0]
                )
                max_symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
                max_symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
            else:
                max_symbol_en = max_symbol_fr = ''
                LOGGER.warning(
                    f'Could not find max symbol for station {station_number}'
                )
            if data_type_en == 'Water Level':
                es_id = '{}.{}.level-niveaux'.format(station_number, year)
            elif data_type_en == 'Discharge':
                es_id = '{}.{}.discharge-debit'.format(station_number, year)
            elif data_type_en == 'Sediment in mg/L':
                es_id = '{}.{}.sediment-sediment'.format(station_number, year)
            elif data_type_en == 'Daily Mean Tonnes':
                es_id = '{}.{}.tonnes-tonnes'.format(station_number, year)
            else:
                es_id = '{}.{}.None'.format(station_number, year)
            insert_dict = {
                'type': 'Feature',
                'properties': {
                    'STATION_NAME': station_name,
                    'IDENTIFIER': es_id,
                    'STATION_NUMBER': station_number,
                    'PROV_TERR_STATE_LOC': province,
                    'DATA_TYPE_EN': data_type_en,
                    'DATA_TYPE_FR': data_type_fr,
                    'MIN_DATE': min_date,
                    'MIN_VALUE': min_value,
                    'MIN_SYMBOL_EN': min_symbol_en,
                    'MIN_SYMBOL_FR': min_symbol_fr,
                    'MAX_DATE': max_date,
                    'MAX_VALUE': max_value,
                    'MAX_SYMBOL_EN': max_symbol_en,
                    'MAX_SYMBOL_FR': max_symbol_fr,
                },
                'geometry': {'type': 'Point', 'coordinates': station_coords},
            }
            action = {
                '_id': es_id,
                '_index': 'hydrometric_annual_statistics',
                '_op_type': 'update',
                'doc': insert_dict,
                'doc_as_upsert': True,
            }
            yield action

    def generate_annual_peaks(
        self,
        annual_peaks_table,
        data_types_table,
        symbol_table,
        station_table,
    ):
        """
        Queries annual peaks data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param annual_peaks_table: table object to query annual peaks data
                                   from.
        :param data_types_table: table object to query data types data from.
        :param symbol_table: table object to query symbol data from.
        :param station_table: table object to query station data from.
        :returns: generator of bulk API upsert actions.
        """
        tz_map = {
            None: None,
            '*': None,
            '0': None,
            'AKST': '-9',
            'AST': '-4',
            'CST': '-6',
            'EST': '-5',
            'MDT': '-6',
            'MST': '-7',
            'NST': '-3.5',
            'PST': '-8',
            'YST': '-9',
        }
        annual_peaks_keys = annual_peaks_table.columns.keys()
        station_keys = station_table.columns.keys()
        data_types_keys = data_types_table.columns.keys()
        results = (
            self.session.query(annual_peaks_table)
            .group_by(
                annual_peaks_table.c['STATION_NUMBER'],
                annual_peaks_table.c['DATA_TYPE'],
                annual_peaks_table.c['YEAR'],
                annual_peaks_table.c['PEAK_CODE'],
            )
            .all()
        )
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
            peak_value = result[annual_peaks_keys.index('PEAK')]
            symbol_id = result[annual_peaks_keys.index('SYMBOL')]
            if month is None or day is None:
                date = None
                LOGGER.warning(
                    f'Could not find date for station {station_number}'
                )
            elif hour is None or minute is None:
                date = '{}-{}-{}'.format(
                    year, self.zero_pad(month), self.zero_pad(day)
                )
            else:
                date = '{}-{}-{}T{}:{}'.format(
                    year,
                    self.zero_pad(month),
                    self.zero_pad(day),
                    self.zero_pad(hour),
                    self.zero_pad(minute),
                )
            args = {'STATION_NUMBER': station_number}
            try:
                station_metadata = list(
                    self.session.query(station_table)
                    .filter_by(**args)
                    .all()[0]
                )
                station_name = station_metadata[
                    station_keys.index('STATION_NAME')
                ]
                province = station_metadata[
                    station_keys.index('PROV_TERR_STATE_LOC')
                ]
                station_coords = [
                    float(station_metadata[station_keys.index('LONGITUDE')]),
                    float(station_metadata[station_keys.index('LATITUDE')]),
                ]
            except Exception:
                station_name = None
                province = None
                station_coords = [None, None]
                LOGGER.warning(
                    f'Could not find station information for station '
                    f'{station_number}'
                )
            args = {'DATA_TYPE': data_type}
            data_type_metadata = list(
                self.session.query(data_types_table).filter_by(**args).all()[0]
            )
            data_type_en = data_type_metadata[
                data_types_keys.index('DATA_TYPE_EN')
            ]
            data_type_fr = data_type_metadata[
                data_types_keys.index('DATA_TYPE_FR')
            ]
            if data_type_en == 'Flow':
                data_type_en = 'Discharge'
            if unit_id:
                unit_codes = self.get_table_var('PRECISION_CODES')
                unit_keys = unit_codes.columns.keys()
                args = {'PRECISION_CODE': unit_id}
                unit_data = list(
                    self.session.query(unit_codes).filter_by(**args).all()[0]
                )
                unit_en = unit_data[unit_keys.index('PRECISION_EN')]
                unit_fr = unit_data[unit_keys.index('PRECISION_FR')]
            else:
                unit_en = unit_fr = None
                LOGGER.warning(
                    'Could not find units for station {}'.format(
                        station_number
                    )
                )
            if peak_id:
                peak_codes = self.get_table_var('PEAK_CODES')
                peak_keys = peak_codes.columns.keys()
                args = {'PEAK_CODE': peak_id}
                peak_data = list(
                    self.session.query(peak_codes).filter_by(**args).all()[0]
                )
                peak_en = peak_data[peak_keys.index('PEAK_EN')]
                peak_fr = peak_data[peak_keys.index('PEAK_FR')]
            else:
                peak_en = peak_fr = None
                LOGGER.warning(
                    f'Could not find peaks for station {station_number}'
                )
            if symbol_id and symbol_id.strip():
                symbol_keys = symbol_table.columns.keys()
                args = {'SYMBOL_ID': symbol_id}
                symbol_data = list(
                    self.session.query(symbol_table).filter_by(**args).all()[0]
                )
                symbol_en = symbol_data[symbol_keys.index('SYMBOL_EN')]
                symbol_fr = symbol_data[symbol_keys.index('SYMBOL_FR')]
            else:
                symbol_en = symbol_fr = None
                LOGGER.warning(
                    f'Could not find symbol for station {station_number}'
                )
            if peak_en == 'Maximum':
                peak = 'maximum-maximale'
            elif peak_en == 'Minimum':
                peak = 'minimum-minimale'
            else:
                peak = None

            if data_type_en == 'Water Level':
                es_id = '{}.{}.level-niveaux.{}'.format(
                    station_number, year, peak
                )
            elif data_type_en == 'Discharge':
                es_id = '{}.{}.discharge-debit.{}'.format(
                    station_number, year, peak
                )
            elif data_type_en == 'Sediment in mg/L':
                es_id = '{}.{}.sediment-sediment.{}'.format(
                    station_number, year, peak
                )
            elif data_type_en == 'Daily Mean Tonnes':
                es_id = '{}.{}.tonnes-tonnes.{}'.format(
                    station_number, year, peak
                )
            else:
                es_id = '{}.{}.None'.format(station_number, year)
            insert_dict = {
                'type': 'Feature',
                'properties': {
                    'STATION_NAME': station_name,
                    'STATION_NUMBER': station_number,
                    'PROV_TERR_STATE_LOC': province,
                    'IDENTIFIER': es_id,
                    'DATA_TYPE_EN': data_type_en,
                    'DATA_TYPE_FR': data_type_fr,
                    'DATE': date,
                    'TIMEZONE_OFFSET': time_zone,
                    'PEAK_CODE_EN': peak_en,
                    'PEAK_CODE_FR': peak_fr,
                    'PEAK': peak_value,
                    'UNITS_EN': unit_en,
                    'UNITS_FR': unit_fr,
                    'SYMBOL_EN': symbol_en,
                    'SYMBOL_FR': symbol_fr,
                },
                'geometry': {'type': 'Point', 'coordinates': station_coords},
            }
            action = {
                '_id': es_id,
                '_index': 'hydrometric_annual_peaks',
                '_op_type': 'update',
                'doc': insert_dict,
                'doc_as_upsert': True,
            }
            yield action


@click.group()
def hydat():
    """Manages HYDAT indices"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_DB(help='Path to HYDAT SQLite database')
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_DATASET(
    type=click.Choice(
        [
            'all',
            'stations',
            'observations',
            'annual-statistics',
            'annual-peaks',
        ]
    )
)
def add(ctx, db, es, username, password, dataset):
    """Loads HYDAT data into Elasticsearch"""

    plugin_def = {
        'db_string': db,
        'es_conn_dict': {'host': es, 'auth': (username, password)}
        if all([es, username, password])
        else None,
        'handler': 'msc_pygeoapi.loader.hydat.HydatLoader',
    }

    loader = HydatLoader(plugin_def)

    click.echo('Accessing SQLite database {}'.format(db))
    try:
        click.echo('Accessing SQLite database {}'.format(db))
        discharge_var = level_var = station_table = None

        level_var = loader.get_table_var('DLY_LEVELS')
        discharge_var = loader.get_table_var('DLY_FLOWS')
        station_table = loader.get_table_var('STATIONS')
        data_types_table = loader.get_table_var('DATA_TYPES')
        annual_stats_table = loader.get_table_var('ANNUAL_STATISTICS')
        symbol_table = loader.get_table_var('DATA_SYMBOLS')
        annual_peaks_table = loader.get_table_var('ANNUAL_INSTANT_PEAKS')
    except Exception as err:
        msg = 'Could not create table variables: {}'.format(err)
        raise click.ClickException(msg)

    if dataset == 'all':
        datasets_to_process = [
            'annual-peaks',
            'annual-statistics',
            'observations',
            'stations'
        ]
    else:
        datasets_to_process = [dataset]

    click.echo('Processing dataset(s): {}'.format(datasets_to_process))

    if 'stations' in datasets_to_process:
        if MSC_PYGEOAPI_OGC_API_URL is None:
            msg = 'MSC_PYGEOAPI_OGC_API_URL environment variable not set'
            raise click.ClickException(msg)
        try:
            click.echo('Populating stations index')
            loader.create_index('stations')
            stations = loader.generate_stations(station_table)
            submit_elastic_package(loader.ES, stations)
        except Exception as err:
            msg = 'Could not populate stations index: {}'.format(err)
            raise click.ClickException(msg)

    if 'observations' in datasets_to_process:
        try:
            click.echo('Populating observations indexes')
            loader.create_index('observations')
            means = loader.generate_means(discharge_var, level_var,
                                          station_table, symbol_table)
            submit_elastic_package(loader.ES, means)
        except Exception as err:
            msg = 'Could not populate observations indexes: {}'.format(err)
            raise click.ClickException(msg)

    if 'annual-statistics' in datasets_to_process:
        try:
            click.echo('Populating annual statistics index')
            loader.create_index('annual_statistics')
            stats = loader.generate_annual_stats(annual_stats_table,
                                                 data_types_table,
                                                 station_table, symbol_table)
            submit_elastic_package(loader.ES, stats)
        except Exception as err:
            msg = 'Could not populate annual statistics index: {}'.format(err)
            raise click.ClickException(msg)

    if 'annual-peaks' in datasets_to_process:
        try:
            click.echo('Populating annual peaks index')
            loader.create_index('annual_peaks')
            peaks = loader.generate_annual_peaks(annual_peaks_table,
                                                 data_types_table,
                                                 symbol_table, station_table)
            submit_elastic_package(loader.ES, peaks)
        except Exception as err:
            msg = 'Could not populate annual peaks index: {}'.format(err)
            raise click.ClickException(msg)


hydat.add_command(add)
