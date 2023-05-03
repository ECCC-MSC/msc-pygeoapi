# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
#
# Copyright (c) 2020 Etienne Pelletier
# Copyright (c) 2022 Tom Kralidis
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

from datetime import datetime
import logging

import cx_Oracle
import click
from slugify import slugify

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_LOGGING_LOGLEVEL
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    check_es_indexes_to_delete,
    configure_es_connection,
    strftime_rfc3339,
    DATETIME_RFC3339_FMT,
)

LOGGER = logging.getLogger(__name__)

LOGGER.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

DAYS_TO_KEEP = 7

INDEX_BASENAME = 'ltce_{}.'
INDEX_PATTERN = '{index_name}.{year:d}-{month:d}-{day:d}.{hour:2d}{minute:2d}{second:2d}'  # noqa

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {'properties': None},
        }
    },
}

MAPPINGS = {
    'stations': {
        'VIRTUAL_CLIMATE_ID': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_E': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_F': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'WXO_CITY_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'ELEMENT_NAME_E': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'CLIMATE_IDENTIFIER': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'START_DATE': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'END_DATE': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'DATA_SOURCE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'ENG_STN_NAME': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'FRE_STN_NAME': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'PROVINCE_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'IDENTIFIER': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
    },
    'temp_extremes': {
        'WXO_CITY_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_E': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_F': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_CLIMATE_ID': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'LOCAL_DAY': {'type': 'short'},
        'LOCAL_MONTH': {'type': 'byte'},
        'RECORD_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'RECORD_HIGH_MAX_TEMP': {'type': 'half_float'},
        'PREV_RECORD_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'PREV_RECORD_HIGH_MAX_TEMP': {'type': 'half_float'},
        'RECORD_LOW_MAX_TEMP_YR': {'type': 'short'},
        'RECORD_LOW_MAX_TEMP': {'type': 'half_float'},
        'PREV_RECORD_LOW_MAX_TEMP_YR': {'type': 'short'},
        'PREV_RECORD_LOW_MAX_TEMP': {'type': 'half_float'},
        'RECORD_LOW_MIN_TEMP_YR': {'type': 'short'},
        'RECORD_LOW_MIN_TEMP': {'type': 'half_float'},
        'PREV_RECORD_LOW_MIN_TEMP_YR': {'type': 'short'},
        'PREV_RECORD_LOW_MIN_TEMP': {'type': 'half_float'},
        'RECORD_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'RECORD_HIGH_MIN_TEMP': {'type': 'half_float'},
        'PREV_RECORD_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'PREV_RECORD_HIGH_MIN_TEMP': {'type': 'half_float'},
        'FIRST_HIGH_MAX_TEMP': {'type': 'half_float'},
        'FIRST_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'SECOND_HIGH_MAX_TEMP': {'type': 'half_float'},
        'SECOND_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'THIRD_HIGH_MAX_TEMP': {'type': 'half_float'},
        'THIRD_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'FOURTH_HIGH_MAX_TEMP': {'type': 'half_float'},
        'FOURTH_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'FIFTH_HIGH_MAX_TEMP': {'type': 'half_float'},
        'FIFTH_HIGH_MAX_TEMP_YR': {'type': 'short'},
        'LAST_UPDATED': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'FIRST_LOW_MAX_TEMP': {'type': 'half_float'},
        'FIRST_LOW_MAX_TEMP_YR': {'type': 'short'},
        'SECOND_LOW_MAX_TEMP': {'type': 'half_float'},
        'SECOND_LOW_MAX_TEMP_YR': {'type': 'short'},
        'THIRD_LOW_MAX_TEMP': {'type': 'half_float'},
        'THIRD_LOW_MAX_TEMP_YR': {'type': 'short'},
        'FOURTH_LOW_MAX_TEMP': {'type': 'half_float'},
        'FOURTH_LOW_MAX_TEMP_YR': {'type': 'short'},
        'FIFTH_LOW_MAX_TEMP': {'type': 'half_float'},
        'FIFTH_LOW_MAX_TEMP_YR': {'type': 'short'},
        'FIRST_HIGH_MIN_TEMP': {'type': 'half_float'},
        'FIRST_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'SECOND_HIGH_MIN_TEMP': {'type': 'half_float'},
        'SECOND_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'THIRD_HIGH_MIN_TEMP': {'type': 'half_float'},
        'THIRD_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'FOURTH_HIGH_MIN_TEMP': {'type': 'half_float'},
        'FOURTH_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'FIFTH_HIGH_MIN_TEMP': {'type': 'half_float'},
        'FIFTH_HIGH_MIN_TEMP_YR': {'type': 'short'},
        'FIRST_LOW_MIN_TEMP': {'type': 'half_float'},
        'FIRST_LOW_MIN_TEMP_YR': {'type': 'short'},
        'SECOND_LOW_MIN_TEMP': {'type': 'half_float'},
        'SECOND_LOW_MIN_TEMP_YR': {'type': 'short'},
        'THIRD_LOW_MIN_TEMP': {'type': 'half_float'},
        'THIRD_LOW_MIN_TEMP_YR': {'type': 'short'},
        'FOURTH_LOW_MIN_TEMP': {'type': 'half_float'},
        'FOURTH_LOW_MIN_TEMP_YR': {'type': 'short'},
        'FIFTH_LOW_MIN_TEMP': {'type': 'half_float'},
        'FIFTH_LOW_MIN_TEMP_YR': {'type': 'short'},
        'MIN_TEMP_RECORD_BEGIN': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'MIN_TEMP_RECORD_END': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'MAX_TEMP_RECORD_BEGIN': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'MAX_TEMP_RECORD_END': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'IDENTIFIER': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'PROVINCE_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
    },
    'precip_extremes': {
        'WXO_CITY_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_E': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_F': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_CLIMATE_ID': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'LOCAL_DAY': {'type': 'short'},
        'LOCAL_MONTH': {'type': 'byte'},
        'RECORD_PRECIPITATION_YR': {'type': 'short'},
        'RECORD_PRECIPITATION': {'type': 'half_float'},
        'PREV_RECORD_PRECIPITATION_YR': {'type': 'short'},
        'PREV_RECORD_PRECIPITATION': {'type': 'half_float'},
        'FIRST_PRECIPITATION': {'type': 'half_float'},
        'FIRST_PRECIPITATION_YEAR': {'type': 'short'},
        'SECOND_PRECIPITATION': {'type': 'half_float'},
        'SECOND_PRECIPITATION_YEAR': {'type': 'short'},
        'THIRD_PRECIPITATION': {'type': 'half_float'},
        'THIRD_PRECIPITATION_YEAR': {'type': 'short'},
        'FOURTH_PRECIPITATION': {'type': 'half_float'},
        'FOURTH_PRECIPITATION_YEAR': {'type': 'short'},
        'FIFTH_PRECIPITATION': {'type': 'half_float'},
        'FIFTH_PRECIPITATION_YEAR': {'type': 'short'},
        'LAST_UPDATED': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'RECORD_BEGIN': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'RECORD_END': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'IDENTIFIER': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'PROVINCE_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
    },
    'snow_extremes': {
        'WXO_CITY_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_E': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_STATION_NAME_F': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'VIRTUAL_CLIMATE_ID': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'LOCAL_DAY': {'type': 'short'},
        'LOCAL_MONTH': {'type': 'byte'},
        'RECORD_SNOWFALL_YR': {'type': 'short'},
        'RECORD_SNOWFALL': {'type': 'half_float'},
        'PREV_RECORD_SNOWFALL_YR': {'type': 'short'},
        'PREV_RECORD_SNOWFALL': {'type': 'half_float'},
        'FIRST_SNOWFALL': {'type': 'half_float'},
        'FIRST_SNOWFALL_YEAR': {'type': 'short'},
        'SECOND_SNOWFALL': {'type': 'half_float'},
        'SECOND_SNOWFALL_YEAR': {'type': 'short'},
        'THIRD_SNOWFALL': {'type': 'half_float'},
        'THIRD_SNOWFALL_YEAR': {'type': 'short'},
        'FOURTH_SNOWFALL': {'type': 'half_float'},
        'FOURTH_SNOWFALL_YEAR': {'type': 'short'},
        'FIFTH_SNOWFALL': {'type': 'half_float'},
        'FIFTH_SNOWFALL_YEAR': {'type': 'short'},
        'LAST_UPDATED': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'RECORD_BEGIN': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'RECORD_END': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'IDENTIFIER': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
        'PROVINCE_CODE': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}},
        },
    },
}

INDICES = [INDEX_BASENAME.format(index) for index in MAPPINGS]


class LtceLoader(BaseLoader):
    """LTCE data loader"""

    def __init__(self, db_string=None, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)
        self.conn = ElasticsearchConnector(conn_config)
        self.db_conn = None
        self.date = datetime.utcnow().strftime('%Y-%m-%d.%H%M%S')

        # setup DB connection
        if db_string is not None:
            try:
                self.db_conn = cx_Oracle.connect(db_string)
                self.cur = self.db_conn.cursor()
            except Exception as err:
                msg = f'Could not connect to Oracle: {err}'
                LOGGER.critical(msg)
                raise click.ClickException(msg)
        else:
            LOGGER.debug("No DB connection string passed. Indexing disabled.")
            self.db_conn = self.cur = None

        for item in MAPPINGS:
            SETTINGS['mappings']['properties']['properties'][
                'properties'
            ] = MAPPINGS[item]
            SETTINGS['index_patterns'] = [f'ltce_{item}.*']
            self.conn.create_template(INDEX_BASENAME.format(item), SETTINGS)

    def get_stations_info(self, element_name, station_id):
        """
        Queries LTCE station data for a given element name (DAILY MINIMUM
        TEMPERATURE, DAILY MAXIMUM TEMPERATURE, etc.), and virtual station ID.
        Returns the ealiest start date of all returned stations and the end
        date climate identifier, and coordinates of the most recently threaded
        station.
        :param element_name: `str` of element name
        :param station_id: `str` of virtual climate station id
        :return: `dict` of stations information
        """
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "properties.VIRTUAL_CLIMATE_ID.raw": station_id  # noqa
                                    }
                                },
                                {
                                    "term": {
                                        "properties.ELEMENT_NAME_E.raw": element_name  # noqa
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        }

        results = self.conn.Elasticsearch.search(
            body=query,
            index='ltce_stations',
            _source=[
                'properties.CLIMATE_IDENTIFIER',
                'properties.ENG_STN_NAME',
                'properties.FRE_STN_NAME',
                'properties.START_DATE',
                'properties.END_DATE',
                'properties.PROVINCE_CODE',
                'geometry.coordinates',
            ],
        )

        results = [result['_source'] for result in results['hits']['hits']]

        if not results:
            LOGGER.error(
                f'Received no results from Elasticsearch for {station_id} '
                f'and {element_name}.'
            )

        oldest_station = None
        most_recent_station = None

        for index, station in enumerate(results):
            # retrieve station start and end date
            dates = (
                station['properties']['START_DATE'],
                station['properties']['END_DATE'],
            )

            # convert station dates to datetime objects
            (
                station['properties']['START_DATE'],
                station['properties']['END_DATE'],
            ) = (start_date, end_date) = [
                datetime.strptime(date, DATETIME_RFC3339_FMT)
                if date is not None
                else None
                for date in dates
            ]

            # assign first station as oldest and most recent
            if index == 0:
                oldest_station = station
                most_recent_station = station
                continue

            # then compare all remaining stations and replace as necessary
            if start_date < oldest_station['properties']['START_DATE']:
                oldest_station = station
            if most_recent_station['properties']['END_DATE'] is not None and (
                end_date is None
                or end_date > most_recent_station['properties']['END_DATE']
            ):
                most_recent_station = station

        stations_info = {
            'record_begin': strftime_rfc3339(
                oldest_station['properties']['START_DATE']
            ),
            'record_end': strftime_rfc3339(
                most_recent_station['properties']['END_DATE']
            )
            if most_recent_station['properties']['END_DATE']
            else None,
            'climate_identifier': most_recent_station['properties'][
                'CLIMATE_IDENTIFIER'
            ],
            'eng_stn_name': most_recent_station['properties']['ENG_STN_NAME'],
            'fre_stn_name': most_recent_station['properties']['FRE_STN_NAME'],
            'coords': [
                most_recent_station['geometry']['coordinates'][0],
                most_recent_station['geometry']['coordinates'][1],
            ],
            'province_code': most_recent_station['properties'][
                'PROVINCE_CODE'
            ],
        }

        return stations_info

    def generate_stations(self):
        """
        Queries stations data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param cur: oracle cursor to perform queries against.
        :returns: generator of bulk API upsert actions.
        """

        try:
            self.cur.execute(
                (
                    "SELECT ARKEON2DWH.VIRTUAL_STATION_INFO_F_MVW.*,"
                    "ARKEON2DWH.STATION_INFORMATION.ENG_STN_NAME,"
                    "ARKEON2DWH.STATION_INFORMATION.FRE_STN_NAME,"
                    "ARKEON2DWH.WXO_CITY_INFORMATION_MVW.LAT,"
                    "ARKEON2DWH.WXO_CITY_INFORMATION_MVW.LON,"
                    "ARKEON2DWH.WXO_CITY_INFORMATION_MVW.PROVINCECODE "
                    "FROM ARKEON2DWH.VIRTUAL_STATION_INFO_F_MVW "
                    "LEFT JOIN ARKEON2DWH.STATION_INFORMATION "
                    "ON ARKEON2DWH.VIRTUAL_STATION_INFO_F_MVW.STN_ID = "
                    "ARKEON2DWH.STATION_INFORMATION.STN_ID "
                    "LEFT JOIN ARKEON2DWH.WXO_CITY_INFORMATION_MVW "
                    "ON ARKEON2DWH.VIRTUAL_STATION_INFO_F_MVW.WXO_CITY_CODE = "
                    "ARKEON2DWH.WXO_CITY_INFORMATION_MVW.CITYCODE "
                    "WHERE "
                    "ARKEON2DWH.VIRTUAL_STATION_INFO_F_MVW.ELEMENT_NAME_E IN "
                    "('DAILY MINIMUM TEMPERATURE', 'DAILY MAXIMUM TEMPERATURE',"  # noqa
                    "'DAILY TOTAL PRECIPITATION', 'DAILY TOTAL SNOWFALL')"
                )
            )
        except Exception as err:
            LOGGER.error(f'Could not fetch records from oracle due to: {err}.')

        # fetch records and ensure that some records were retrieved
        records = self.cur.fetchall()
        if self.cur.rowcount == 0:
            LOGGER.error('Stations SQL query returned 0 records.')
            return False

        for row in records:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))
            for key in insert_dict:
                if key in ['START_DATE', 'END_DATE']:
                    insert_dict[key] = (
                        strftime_rfc3339(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )

            es_id = slugify(
                '{}-{}-{}-{}-{}'.format(
                    insert_dict['VIRTUAL_CLIMATE_ID'],
                    insert_dict["ELEMENT_NAME_E"],
                    insert_dict["CLIMATE_IDENTIFIER"],
                    insert_dict["START_DATE"],
                    insert_dict["END_DATE"],
                )
            )

            coords = [
                float(insert_dict['LON']),
                float(insert_dict['LAT']),
            ]

            # rename PROVINCECODE field to PROVINCE_CODE
            insert_dict['PROVINCE_CODE'] = insert_dict['PROVINCECODE']

            # cleanup unwanted fields retained from SQL join
            fields_to_delete = [
                'STN_ID',
                'ENG_PROV_NAME',
                'FRE_PROV_NAME',
                'REGION_CODE',
                'CRITERIA',
                'NOTES',
                'VIRTUAL_STN_INFO_UPDATE_ID',
                'CURRENT_FLAG',
                'LON',
                'LAT',
                'PROVINCECODE',
            ]
            for field in fields_to_delete:
                insert_dict.pop(field)

            # set properties.IDENTIFIER
            insert_dict['IDENTIFIER'] = es_id

            wrapper = {
                'id': es_id,
                'type': 'Feature',
                'properties': insert_dict,
                'geometry': {'type': 'Point', 'coordinates': coords},
            }

            action = {
                '_id': es_id,
                '_index': f'ltce_stations.{self.date}',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True,
            }

            yield action

    def generate_daily_temp_extremes(self):
        """
        Queries stations data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param cur: oracle cursor to perform queries against.
        :returns: generator of bulk API upsert actions.
        """

        try:
            self.cur.execute(
                (
                    "SELECT t1.*, t2.*, t3.*, t4.*, t5.*, t6.*, t7.*, t8.* "
                    "FROM ARKEON2DWH.RECORD_HIGH_VIRTUAL_MAX_TEMP t1 "
                    "JOIN ARKEON2DWH.RECORD_LOW_VIRTUAL_MAX_TEMP t2 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t2.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t2.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t2.LOCAL_DAY "
                    "JOIN ARKEON2DWH.RECORD_LOW_VIRTUAL_MIN_TEMP t3 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t3.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t3.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t3.LOCAL_DAY "
                    "JOIN ARKEON2DWH.RECORD_HIGH_VIRTUAL_MIN_TEMP t4 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t4.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t4.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t4.LOCAL_DAY "
                    "JOIN ARKEON2DWH.EXTREME_HIGH_VIRTUAL_MAX_TEMP t5 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t5.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t5.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t5.LOCAL_DAY "
                    "JOIN ARKEON2DWH.EXTREME_LOW_VIRTUAL_MAX_TEMP t6 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t6.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t6.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t6.LOCAL_DAY "
                    "JOIN ARKEON2DWH.EXTREME_HIGH_VIRTUAL_MIN_TEMP t7 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t7.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t7.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t7.LOCAL_DAY "
                    "JOIN ARKEON2DWH.EXTREME_LOW_VIRTUAL_MIN_TEMP t8 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t8.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t8.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t8.LOCAL_DAY "
                )
            )
        except Exception as err:
            LOGGER.error(
                'Could not fetch records from oracle due to: {}.'.format(
                    str(err)
                )
            )

        # dictionnary to store stations information once retrieved
        stations_dict = {}

        # fetch records and ensure that some records were retrieved
        records = self.cur.fetchall()
        if self.cur.rowcount == 0:
            LOGGER.error('Temperature extremes SQL query returned 0 records.')
            return False

        for row in records:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))

            for key in insert_dict:
                if key in ['LAST_UPDATED']:
                    insert_dict[key] = (
                        strftime_rfc3339(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )

            virtual_climate_id = insert_dict['VIRTUAL_CLIMATE_ID']
            es_id = '{}-{}-{}'.format(
                insert_dict['VIRTUAL_CLIMATE_ID'],
                insert_dict["LOCAL_MONTH"],
                insert_dict["LOCAL_DAY"],
            )

            # check if we have station IDs record begin and end. If not
            # retrieve the information and store in stations_dict
            if virtual_climate_id not in stations_dict:
                stations_dict[virtual_climate_id] = {}
                stations_dict[virtual_climate_id][
                    'MIN'
                ] = self.get_stations_info(
                    'DAILY MINIMUM TEMPERATURE', virtual_climate_id
                )
                stations_dict[virtual_climate_id][
                    'MAX'
                ] = self.get_stations_info(
                    'DAILY MAXIMUM TEMPERATURE', virtual_climate_id
                )

            # check if TEMEPERATURE MIN/MAX for most recent threaded station
            # have same climate identifier value
            min_climate_identifier = stations_dict[virtual_climate_id]['MIN'][
                'climate_identifier'
            ]
            max_climate_identifier = stations_dict[virtual_climate_id]['MAX'][
                'climate_identifier'
            ]

            if min_climate_identifier == max_climate_identifier:
                insert_dict['CLIMATE_IDENTIFIER'] = stations_dict[
                    virtual_climate_id
                ]['MAX']['climate_identifier']
                insert_dict['ENG_STN_NAME'] = stations_dict[
                    virtual_climate_id
                ]['MAX']['eng_stn_name']
                insert_dict['FRE_STN_NAME'] = stations_dict[
                    virtual_climate_id
                ]['MAX']['fre_stn_name']
                insert_dict['PROVINCE_CODE'] = stations_dict[
                    virtual_climate_id
                ]['MAX']['province_code']

            else:
                LOGGER.error(
                    f'Currently threaded station climate identifier value '
                    f'does not match between DAILY MINIMUM TEMPERATURE'
                    f'({min_climate_identifier}) and DAILY MAXIMUM '
                    f'TEMPERATURE({max_climate_identifier}) station threads '
                    f'for virtual climate ID {virtual_climate_id}.'
                )
                continue

            # set new fields
            for level in ['MIN', 'MAX']:
                # set new insert_dict keys
                insert_dict[
                    '{}_TEMP_RECORD_BEGIN'.format(level)
                ] = stations_dict[virtual_climate_id][level]['record_begin']
                insert_dict[
                    '{}_TEMP_RECORD_END'.format(level)
                ] = stations_dict[virtual_climate_id][level]['record_end']

            # cleanup unwanted fields retained from SQL join
            fields_to_delete = [
                'LOCAL_TIME',
                'VIRTUAL_MEAS_DISPLAY_CODE',
                'ENG_STN_NAME',
                'FRE_STN_NAME',
                'CLIMATE_IDENTIFIER',
            ]
            for field in fields_to_delete:
                insert_dict.pop(field)

            # set properties.IDENTIFIER
            insert_dict['IDENTIFIER'] = es_id

            wrapper = {
                'id': es_id,
                'type': 'Feature',
                'properties': insert_dict,
                'geometry': {
                    'type': 'Point',
                    'coordinates': stations_dict[virtual_climate_id]['MAX'][
                        'coords'
                    ],
                },
            }

            action = {
                '_id': es_id,
                '_index': f'ltce_temp_extremes.{self.date}',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True,
            }

            yield action

    def generate_daily_precip_extremes(self):
        """
        Queries stations data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param cur: oracle cursor to perform queries against.
        :returns: generator of bulk API upsert actions.
        """

        try:
            self.cur.execute(
                (
                    "SELECT t1.*, t2.* "
                    "FROM ARKEON2DWH.RECORD_VIRTUAL_PRECIPITATION t1 "
                    "JOIN ARKEON2DWH.EXTREME_VIRTUAL_PRECIPITATION t2 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t2.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t2.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t2.LOCAL_DAY "
                )
            )
        except Exception as err:
            LOGGER.error(
                'Could not fetch records from oracle due to: {}.'.format(
                    str(err)
                )
            )

        stations_dict = {}

        # fetch records and ensure that some records were retrieved
        records = self.cur.fetchall()
        if self.cur.rowcount == 0:
            LOGGER.error(
                'Precipitation extremes SQL query returned 0 records.'
            )
            return False

        for row in records:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))

            for key in insert_dict:
                if key in ['LAST_UPDATED']:
                    insert_dict[key] = (
                        strftime_rfc3339(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )

            virtual_climate_id = insert_dict['VIRTUAL_CLIMATE_ID']
            es_id = '{}-{}-{}'.format(
                insert_dict['VIRTUAL_CLIMATE_ID'],
                insert_dict["LOCAL_MONTH"],
                insert_dict["LOCAL_DAY"],
            )

            # check if we have station IDs record begin and end if not retrieve
            if virtual_climate_id not in stations_dict:
                stations_dict[virtual_climate_id] = self.get_stations_info(
                    'DAILY TOTAL PRECIPITATION', virtual_climate_id
                )

            insert_dict['RECORD_BEGIN'] = stations_dict[virtual_climate_id][
                'record_begin'
            ]
            insert_dict['RECORD_END'] = stations_dict[virtual_climate_id][
                'record_end'
            ]

            insert_dict['CLIMATE_IDENTIFIER'] = stations_dict[
                virtual_climate_id
            ]['climate_identifier']
            insert_dict['ENG_STN_NAME'] = stations_dict[virtual_climate_id][
                'eng_stn_name'
            ]
            insert_dict['FRE_STN_NAME'] = stations_dict[virtual_climate_id][
                'fre_stn_name'
            ]

            insert_dict['PROVINCE_CODE'] = stations_dict[virtual_climate_id][
                'province_code'
            ]
            # cleanup unwanted fields retained from SQL join
            fields_to_delete = [
                'LOCAL_TIME',
                'VIRTUAL_MEAS_DISPLAY_CODE',
                'ENG_STN_NAME',
                'FRE_STN_NAME',
                'CLIMATE_IDENTIFIER',
                'LAST_UPDATED',
            ]
            for field in fields_to_delete:
                insert_dict.pop(field)

            # set properties.IDENTIFIER
            insert_dict['IDENTIFIER'] = es_id

            wrapper = {
                'id': es_id,
                'type': 'Feature',
                'properties': insert_dict,
                'geometry': {
                    'type': 'Point',
                    'coordinates': stations_dict[virtual_climate_id]['coords'],
                },
            }

            action = {
                '_id': es_id,
                '_index': f'ltce_precip_extremes.{self.date}',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True,
            }

            yield action

    def generate_daily_snow_extremes(self):
        """
        Queries stations data from the db, and reformats
        data so it can be inserted into Elasticsearch.

        Returns a generator of dictionaries that represent upsert actions
        into Elasticsearch's bulk API.

        :param cur: oracle cursor to perform queries against.
        :returns: generator of bulk API upsert actions.
        """

        try:
            self.cur.execute(
                (
                    "SELECT t1.*, t2.* "
                    "FROM ARKEON2DWH.RECORD_VIRTUAL_SNOWFALL t1 "
                    "JOIN ARKEON2DWH.EXTREME_VIRTUAL_SNOWFALL t2 "
                    "ON t1.VIRTUAL_CLIMATE_ID = t2.VIRTUAL_CLIMATE_ID "
                    "AND t1.LOCAL_MONTH = t2.LOCAL_MONTH "
                    "AND t1.LOCAL_DAY = t2.LOCAL_DAY "
                )
            )
        except Exception as err:
            LOGGER.error(
                'Could not fetch records from oracle due to: {}.'.format(
                    str(err)
                )
            )

        stations_dict = {}

        # fetch records and ensure that some records were retrieved
        records = self.cur.fetchall()
        if self.cur.rowcount == 0:
            LOGGER.error('Snowfall extremes SQL query returned 0 records.')
            return False

        for row in records:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))

            for key in insert_dict:
                if key in ['LAST_UPDATED']:
                    insert_dict[key] = (
                        strftime_rfc3339(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )

            virtual_climate_id = insert_dict['VIRTUAL_CLIMATE_ID']
            es_id = '{}-{}-{}'.format(
                insert_dict['VIRTUAL_CLIMATE_ID'],
                insert_dict["LOCAL_MONTH"],
                insert_dict["LOCAL_DAY"],
            )

            # check if we have station IDs record begin and end if not retrieve
            if virtual_climate_id not in stations_dict:
                stations_dict[virtual_climate_id] = self.get_stations_info(
                    'DAILY TOTAL SNOWFALL', virtual_climate_id
                )

            insert_dict['RECORD_BEGIN'] = stations_dict[virtual_climate_id][
                'record_begin'
            ]
            insert_dict['RECORD_END'] = stations_dict[virtual_climate_id][
                'record_end'
            ]

            insert_dict['CLIMATE_IDENTIFIER'] = stations_dict[
                virtual_climate_id
            ]['climate_identifier']
            insert_dict['ENG_STN_NAME'] = stations_dict[virtual_climate_id][
                'eng_stn_name'
            ]
            insert_dict['FRE_STN_NAME'] = stations_dict[virtual_climate_id][
                'fre_stn_name'
            ]

            insert_dict['PROVINCE_CODE'] = stations_dict[virtual_climate_id][
                'province_code'
            ]

            # cleanup unwanted fields retained from SQL join
            fields_to_delete = [
                'LOCAL_TIME',
                'VIRTUAL_MEAS_DISPLAY_CODE',
                'ENG_STN_NAME',
                'FRE_STN_NAME',
                'CLIMATE_IDENTIFIER',
                'LAST_UPDATED',
            ]
            for field in fields_to_delete:
                insert_dict.pop(field)

            # set properties.IDENTIFIER
            insert_dict['IDENTIFIER'] = es_id

            wrapper = {
                'id': es_id,
                'type': 'Feature',
                'properties': insert_dict,
                'geometry': {
                    'type': 'Point',
                    'coordinates': stations_dict[virtual_climate_id]['coords'],
                },
            }

            action = {
                '_id': es_id,
                '_index': f'ltce_snow_extremes.{self.date}',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True,
            }

            yield action


@click.group()
def ltce():
    """Manages LTCE indices"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_BATCH_SIZE()
@cli_options.OPTION_DB()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_DATASET(
    type=click.Choice(
        [
            'all',
            'observations',
            'stations',
            'temperature',
            'precipitation',
            'snowfall'
        ]
    ),
    help='LTCE dataset to load',
)
def add(
    ctx,
    db,
    es,
    username,
    password,
    ignore_certs,
    dataset,
    batch_size,
):
    """
    Loads Long Term Climate Extremes(LTCE) data from Oracle DB
    into Elasticsearch.

    :param db: database connection string.
    :param dataset: name of dataset to load, or all for all datasets.
    """

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = LtceLoader(db, conn_config)

    if dataset == 'all':
        datasets_to_process = [
            'stations',
            'temperature',
            'precipitation',
            'snowfall'
        ]
    elif dataset == 'observations':
        datasets_to_process = [
            'temperature',
            'precipitation',
            'snowfall'
        ]
    else:
        datasets_to_process = [dataset]

    if 'stations' in datasets_to_process:
        try:
            stations = loader.generate_stations()
            if stations:
                loader.conn.submit_elastic_package(stations, batch_size)
                LOGGER.info('Stations populated.')
                LOGGER.info(
                    f'Setting alias ltce_station to point '
                    f'to index ltce_stations.{loader.date}.'
                )
                loader.conn.create_alias(
                    'ltce_stations',
                    f'ltce_stations.{loader.date}',
                    overwrite=True,
                )
            else:
                LOGGER.error('No stations populated.')
        except Exception as err:
            LOGGER.error(f'Could not populate stations due to: {err}.')
            raise err

    if 'temperature' in datasets_to_process:
        try:
            temp_extremes = loader.generate_daily_temp_extremes()
            if temp_extremes:
                loader.conn.submit_elastic_package(temp_extremes, batch_size)
                LOGGER.info('Daily temperature extremes populated.')
                LOGGER.info(
                    f'Setting alias ltce_temp_extremes to '
                    f'point to index ltce_temp_extremes.{loader.date}.'
                )
                loader.conn.create_alias(
                    'ltce_temp_extremes',
                    f'ltce_temp_extremes.{loader.date}',
                    overwrite=True,
                )
            else:
                LOGGER.error('No temperature extremes populated.')
        except Exception as err:
            LOGGER.error(
                'Could not populate daily temperature extremes due to: '
                f'{err}.'
            )
            raise err

    if 'precipitation' in datasets_to_process:
        try:
            precip_extremes = loader.generate_daily_precip_extremes()
            if precip_extremes:
                loader.conn.submit_elastic_package(precip_extremes, batch_size)
                LOGGER.info('Daily precipitation extremes populated.')
                LOGGER.info(
                    f'Setting alias ltce_precip_extremes to '
                    f'point to index ltce_precip_extremes.{loader.date}.'
                )
                loader.conn.create_alias(
                    'ltce_precip_extremes',
                    f'ltce_precip_extremes.{loader.date}',
                    overwrite=True,
                )
            else:
                LOGGER.error('No precipitation extremes populated.')
        except Exception as err:
            LOGGER.error(
                'Could not populate daily precipitations extremes due to: '
                f'{err}'
            )
            raise err

    if 'snowfall' in datasets_to_process:
        try:
            snow_extremes = loader.generate_daily_snow_extremes()
            if snow_extremes:
                loader.conn.submit_elastic_package(snow_extremes, batch_size)
                LOGGER.info('Daily snowfall extremes populated.')
                LOGGER.info(
                    f'Setting alias ltce_snow_extremes to '
                    f'point to index ltce_snow_extremes.{loader.date}.'
                )
                loader.conn.create_alias(
                    'ltce_snow_extremes',
                    f'ltce_snow_extremes.{loader.date}',
                    overwrite=True,
                )
            else:
                LOGGER.error('No snowfall extremes populated.')
        except Exception as err:
            LOGGER.error(
                f'Could not populate daily snowfall extremes due to: {err}.'
            )
            raise err

    LOGGER.info('Finished populating indices.')

    loader.db_conn.close()


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help='Delete indexes older than n days (default={})'.format(DAYS_TO_KEEP),
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, days, es, username, password, ignore_certs):
    """Clean hydrometric realtime indexes older than n number of days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get('{}*'.format(INDEX_BASENAME.format('*')))

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(
            indexes,
            days,
            pattern=INDEX_PATTERN
        )
        if indexes_to_delete:
            click.echo('Deleting indexes {}'.format(indexes_to_delete))
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


def confirm(ctx, param, value):
    if not value and ctx.params['index_name']:
        click.confirm(
            'Are you sure you want to delete ES index named: {}?'.format(
                click.style(
                    [f'{i}*' for i in ctx.params['index_name']], fg='red'
                )
            ),
            abort=True,
        )
    elif not value:
        click.confirm(
            'Are you sure you want to delete {} LTCE'
            ' indices ({})?'.format(
                click.style('ALL', fg='red'),
                click.style([f'{index}*' for index in INDICES], fg='red'),
            ),
            abort=True,
        )


@click.command()
@click.pass_context
@click.option(
    '--index_name',
    '-i',
    help='msc-pygeoapi LTCE index name to delete',
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
@cli_options.OPTION_YES(callback=confirm)
def delete_indexes(
    ctx, index_name, es, username, password, ignore_certs, index_template
):
    """
    Delete a particular ES index with a given name as argument or all if no
    argument is passed
    """

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    loader = LtceLoader(conn_config=conn_config)

    if index_name:
        for i in index_name.split(","):
            if i in INDICES:
                LOGGER.info('Deleting ES index {}'.format(i))
                loader.conn.delete(f'{i}.*')
    else:
        LOGGER.info('Deleting all LTCE ES indices')
        loader.conn.delete("ltce_*")

    if index_template:
        for template in [INDEX_BASENAME.format(item) for item in MAPPINGS]:
            LOGGER.info(f'Deleting ES index template {template}')
            loader.conn.delete_template(template)

    return True


ltce.add_command(add)
ltce.add_command(clean_indexes)
ltce.add_command(delete_indexes)
