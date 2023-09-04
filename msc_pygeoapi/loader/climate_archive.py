# =================================================================
#
# Author: Alex Hurka <alex.hurka@canada.ca>
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2020 Etienne Pelletier
# Copyright (c) 2019 Alex Hurka
# Copyright (c) 2023 Tom Kralidis

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

import collections
import logging

import click
import cx_Oracle

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection


logging.basicConfig()
LOGGER = logging.getLogger(__name__)
HTTP_OK = 200
POST_OK = 201
HEADERS = {'Content-type': 'application/json'}


class ClimateArchiveLoader(BaseLoader):
    """Climat Archive Loader"""

    def __init__(self, db_conn_string, conn_config={}):
        """initializer"""

        super().__init__()

        self.conn = ElasticsearchConnector(conn_config)

        # setup DB connection
        try:
            self.db_conn = cx_Oracle.connect(db_conn_string)
        except Exception as err:
            msg = f'Could not connect to Oracle: {err}'
            LOGGER.critical(msg)
            raise click.ClickException(msg)

        self.cur = self.db_conn.cursor()

    def create_index(self, index):
        """
        Creates the Elasticsearch index at path. If the index already exists,
        it is deleted and re-created. The mappings for the two types are also
        created.

        :param index: the index to be created.
        """

        if index == 'stations':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "PROV_STATE_TERR_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STN_ID": {"type": "integer"},
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ENG_PROV_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "FRE_PROV_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "COUNTRY": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LATITUDE": {"type": "integer"},
                                "LONGITUDE": {"type": "integer"},
                                "TIMEZONE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ELEVATION": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "CLIMATE_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "TC_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WMO_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_TYPE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "NORMAL_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PUBLICATION_CODE": {"type": "integer"},
                                "DISPLAY_CODE": {"type": "integer"},
                                "ENG_STN_OPERATOR_ACRONYM": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "FRE_STN_OPERATOR_ACRONYM": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ENG_STN_OPERATOR_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "FRE_STN_OPERATOR_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "HAS_MONTHLY_SUMMARY": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "HAS_NORMALS_DATA": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DLY_FIRST_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                                "DLY_LAST_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                                "FIRST_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                                "LAST_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'climate_station_information'
            self.conn.create(index_name, mapping, overwrite=True)

        if index == 'normals':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STN_ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MONTH": {"type": "integer"},
                                "VALUE": {"type": "integer"},
                                "OCCURRENCE_COUNT": {"type": "integer"},
                                "PUBLICATION_CODE": {"type": "integer"},
                                "CLIMATE_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "NORMAL_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "NORMAL_ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROVINCE_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "E_NORMAL_ELEMENT_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "F_NORMAL_ELEMENT_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PERIOD": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PERIOD_BEGIN": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PERIOD_END": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
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
                                "LAST_YEAR_NORMAL_PERIOD": {"type": "integer"},
                                "FIRST_YEAR": {"type": "integer"},
                                "LAST_YEAR": {"type": "integer"},
                                "TOTAL_OBS_COUNT": {"type": "integer"},
                                "PERCENT_OF_POSSIBLE_OBS": {"type": "integer"},
                                "CURRENT_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "FIRST_OCCURRENCE_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                                "DATE_CALCULATED": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time",  # noqa
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'climate_normals_data'
            self.conn.create(index_name, mapping, overwrite=True)

        if index == 'monthly_summary':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "CLIMATE_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STN_ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROVINCE_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LATITUDE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LONGITUDE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MEAN_TEMPERATURE": {"type": "float"},
                                "NORMAL_MEAN_TEMPERATURE": {"type": "float"},
                                "MAX_TEMPERATURE": {"type": "float"},
                                "MIN_TEMPERATURE": {"type": "float"},
                                "TOTAL_SNOWFALL": {"type": "float"},
                                "NORMAL_SNOWFALL": {"type": "float"},
                                "TOTAL_PRECIPITATION": {"type": "float"},
                                "NORMAL_PRECIPITATION": {"type": "float"},
                                "BRIGHT_SUNSHINE": {"type": "float"},
                                "NORMAL_SUNSHINE": {"type": "float"},
                                "SNOW_ON_GROUND_LAST_DAY": {"type": "float"},
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
                                "DAYS_WITH_VALID_PRECIP": {"type": "integer"},
                                "DAYS_WITH_VALID_SUNSHINE": {
                                    "type": "integer"
                                },
                                "DAYS_WITH_PRECIP_GE_1MM": {"type": "integer"},
                                "HEATING_DEGREE_DAYS": {"type": "integer"},
                                "COOLING_DEGREE_DAYS": {"type": "integer"},
                                "LOCAL_YEAR": {"type": "integer"},
                                "LOCAL_MONTH": {"type": "integer"},
                                "LAST_UPDATED": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                },
                                "LOCAL_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM||strict_date_optional_time",  # noqa
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'climate_public_climate_summary'
            self.conn.create(index_name, mapping, overwrite=True)

        if index == 'daily_summary':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "CLIMATE_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STN_ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "SOURCE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MAX_TEMPERATURE_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MIN_TEMPERATURE_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MEAN_TEMPERATURE_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROVINCE_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MAX_REL_HUMIDITY_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MIN_REL_HUMIDITY_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "TOTAL_RAIN_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "TOTAL_SNOW_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "TOTAL_PRECIPITATION_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "SNOW_ON_GROUND_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DIRECTION_MAX_GUST_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "SPEED_MAX_GUST_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "HEATING_DEGREE_DAYS_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "COOLING_DEGREE_DAYS_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "MEAN_TEMPERATURE": {"type": "float"},
                                "TOTAL_RAIN": {"type": "float"},
                                "MAX_TEMPERATURE": {"type": "float"},
                                "MIN_TEMPERATURE": {"type": "float"},
                                "MAX_REL_HUMIDITY": {"type": "float"},
                                "MIN_REL_HUMIDITY": {"type": "float"},
                                "TOTAL_SNOW": {"type": "float"},
                                "SNOW_ON_GROUND": {"type": "float"},
                                "TOTAL_PRECIPITATION": {"type": "float"},
                                "DIRECTION_MAX_GUST": {"type": "float"},
                                "SPEED_MAX_GUST": {"type": "float"},
                                "HEATING_DEGREE_DAYS": {"type": "integer"},
                                "COOLING_DEGREE_DAYS": {"type": "integer"},
                                "LOCAL_YEAR": {"type": "integer"},
                                "LOCAL_MONTH": {"type": "integer"},
                                "LOCAL_DAY": {"type": "integer"},
                                "LOCAL_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time",  # noqa
                                },
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'climate_public_daily_data'
            self.conn.create(index_name, mapping, overwrite=True)

        if index == 'hourly_summary':
            mapping = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "_meta": {"geomfields": {"geometry": "POINT"}},
                    "properties": {
                        "type": {"type": "text"},
                        "properties": {
                            "properties": {
                                "STN_ID": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "CLIMATE_IDENTIFIER": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_NAME": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "TEMP_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "DEW_POINT_TEMP_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "RELATIVE_HUMIDITY_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PRECIP_AMOUNT_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WIND_DIRECTION_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WIND_SPEED_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "VISIBILITY_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "STATION_PRESSURE_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "HUMIDEX_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WINDCHILL_FLAG": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "PROVINCE_CODE": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WEATHER_ENG_DESC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "WEATHER_FRE_DESC": {
                                    "type": "text",
                                    "fields": {"raw": {"type": "keyword"}},
                                },
                                "LONGITUDE_DECIMAL_DEGREES": {
                                    "type": "float",
                                },
                                "LATITUDE_DECIMAL_DEGREES": {
                                    "type": "float",
                                },
                                "TEMP": {"type": "float"},
                                "DEW_POINT_TEMP": {"type": "float"},
                                "RELATIVE_HUMIDITY": {"type": "float"},
                                "PRECIP_AMOUNT": {"type": "float"},
                                "WIND_DIRECTION": {"type": "float"},
                                "WIND_SPEED": {"type": "float"},
                                "VISIBILITY": {"type": "float"},
                                "STATION_PRESSURE": {"type": "float"},
                                "HUMIDEX": {"type": "float"},
                                "WINDCHILL": {"type": "float"},
                                "LOCAL_YEAR": {"type": "integer"},
                                "LOCAL_MONTH": {"type": "integer"},
                                "LOCAL_DAY": {"type": "integer"},
                                "LOCAL_HOUR": {"type": "integer"},
                                "LOCAL_DATE": {
                                    "type": "date",
                                    "format": "yyyy-MM-dd HH:mm:ss ||strict_date_optional_time",  # noqa
                                },
                                "UTC_YEAR": {"type": "integer"},
                                "UTC_MONTH": {"type": "integer"},
                                "UTC_DAY": {"type": "integer"},
                                "UTC_DATE": {"type": "date"},
                            }
                        },
                        "geometry": {"type": "geo_shape"},
                    },
                },
            }

            index_name = 'climate_public_hourly_data'
            self.conn.create(index_name, mapping, overwrite=True)

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
            self.cur.execute('select * from CCCS_PORTAL.STATION_INFORMATION')
        except Exception as err:
            LOGGER.error(
                f'Could not fetch records from oracle due to: {str(err)}.'
            )

        for row in self.cur:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))
            for key in insert_dict:
                # This is a quick fix for trailing spaces and should not be
                # here. Data should be fixed on db side.
                try:
                    insert_dict[key] = insert_dict[key].strip()
                except Exception as err:
                    LOGGER.debug(
                        f'Could not strip value {insert_dict[key]} due to '
                        f'{str(err)}, skipping'
                    )

                # Transform Date fields from datetime to string.
                if 'DATE' in key:
                    insert_dict[key] = (
                        str(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )

            coords = [
                float(insert_dict['LONGITUDE_DECIMAL_DEGREES']),
                float(insert_dict['LATITUDE_DECIMAL_DEGREES']),
            ]
            del insert_dict['LONGITUDE_DECIMAL_DEGREES']
            del insert_dict['LATITUDE_DECIMAL_DEGREES']
            climate_identifier = insert_dict['CLIMATE_IDENTIFIER']
            wrapper = {
                'type': 'Feature',
                'properties': insert_dict,
                'geometry': {'type': 'Point', 'coordinates': coords},
            }

            action = {
                '_id': climate_identifier,
                '_index': 'climate_station_information',
                '_op_type': 'update',
                'doc': wrapper,
                'doc_as_upsert': True,
            }
            yield action

    def generate_normals(self, stn_dict, normals_dict, periods_dict):
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
            self.cur.execute('select * from CCCS_PORTAL.NORMALS_DATA')
        except Exception as err:
            LOGGER.error(
                f'Could not fetch records from oracle due to: {str(err)}.'
            )

        for row in self.cur:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))

            for key in insert_dict:
                # Transform Date fields from datetime to string.
                if 'DATE' in key:
                    insert_dict[key] = (
                        str(insert_dict[key])
                        if insert_dict[key] is not None
                        else insert_dict[key]
                    )
            insert_dict['ID'] = '.'.join([
                insert_dict['STN_ID'],
                insert_dict['NORMAL_ID'],
                insert_dict['MONTH']
            ])
            if insert_dict['STN_ID'] in stn_dict:
                coords = stn_dict[insert_dict['STN_ID']]['coordinates']
                insert_dict['STATION_NAME'] = stn_dict[insert_dict['STN_ID']][
                    'STATION_NAME'
                ]
                insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']][
                    'PROVINCE_CODE'
                ]
                insert_dict['E_NORMAL_ELEMENT_NAME'] = normals_dict[
                    insert_dict['NORMAL_ID']
                ]['E_NORMAL_ELEMENT_NAME']
                insert_dict['F_NORMAL_ELEMENT_NAME'] = normals_dict[
                    insert_dict['NORMAL_ID']
                ]['F_NORMAL_ELEMENT_NAME']
                insert_dict['PERIOD'] = normals_dict[insert_dict['NORMAL_ID']][
                    'PERIOD'
                ]
                insert_dict['PERIOD_BEGIN'] = periods_dict[
                    insert_dict['NORMAL_PERIOD_ID']
                ]['PERIOD_BEGIN']
                insert_dict['PERIOD_END'] = periods_dict[
                    insert_dict['NORMAL_PERIOD_ID']
                ]['PERIOD_END']
                insert_dict['CLIMATE_IDENTIFIER'] = stn_dict[
                    insert_dict['STN_ID']
                ]['CLIMATE_IDENTIFIER']

                del insert_dict['NORMAL_PERIOD_ID']
                wrapper = {
                    'type': 'Feature',
                    'properties': insert_dict,
                    'geometry': {'type': 'Point', 'coordinates': coords},
                }
                action = {
                    '_id': insert_dict['ID'],
                    '_index': 'climate_normals_data',
                    '_op_type': 'update',
                    'doc': wrapper,
                    'doc_as_upsert': True,
                }
                yield action
            else:
                LOGGER.error(
                    f"Bad STN ID: {insert_dict['STN_ID']}, skipping"
                    f" records for this station"
                )

    def generate_monthly_data(self, stn_dict, date=None):
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
                self.cur.execute(
                    'select * from CCCS_PORTAL.PUBLIC_CLIMATE_SUMMARY'
                )
            except Exception as err:
                LOGGER.error(
                    f'Could not fetch records from oracle due to: {str(err)}.'
                )
        else:
            try:
                self.cur.execute(
                    (
                        f"select * from CCCS_PORTAL.PUBLIC_CLIMATE_SUMMARY "
                        f"WHERE LAST_UPDATED > TO_TIMESTAMP("
                        f"'{date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
                    )
                )
            except Exception as err:
                LOGGER.error(
                    f'Could not fetch records from oracle due to: {str(err)}.'
                )

        for row in self.cur:
            insert_dict = dict(zip([x[0] for x in self.cur.description], row))
            # Transform Date fields from datetime to string.
            insert_dict['LAST_UPDATED'] = (
                str(insert_dict['LAST_UPDATED'])
                if insert_dict['LAST_UPDATED'] is not None
                else insert_dict['LAST_UPDATED']
            )

            insert_dict['ID'] = '.'.join([
                insert_dict['STN_ID'],
                insert_dict['LOCAL_YEAR'],
                insert_dict['LOCAL_MONTH']
            ])
            if insert_dict['STN_ID'] in stn_dict:
                coords = stn_dict[insert_dict['STN_ID']]['coordinates']
                insert_dict['PROVINCE_CODE'] = stn_dict[insert_dict['STN_ID']][
                    'PROVINCE_CODE'
                ]
                wrapper = {
                    'type': 'Feature',
                    'properties': insert_dict,
                    'geometry': {'type': 'Point', 'coordinates': coords},
                }
                action = {
                    '_id': insert_dict['ID'],
                    '_index': 'climate_public_climate_summary',
                    '_op_type': 'update',
                    'doc': wrapper,
                    'doc_as_upsert': True,
                }
                yield action
            else:
                LOGGER.error(
                    f"Bad STN ID: {insert_dict['STN_ID']}, skipping"
                    f" records for this station"
                )

    def generate_daily_data(self, stn_dict, date=None):
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
                    self.cur.execute(
                        f'select * from CCCS_PORTAL.PUBLIC_DAILY_DATA '
                        f'where STN_ID={station}'
                    )
                except Exception as err:
                    LOGGER.error(
                        f'Could not fetch records from oracle due to:'
                        f' {str(err)}.'
                    )
            else:
                try:
                    self.cur.execute(
                        (
                            f"select * from CCCS_PORTAL.PUBLIC_DAILY_DATA "
                            f"where STN_ID={station} and "
                            f"LOCAL_DATE > TO_TIMESTAMP('{date} 00:00:00', "
                            f"'YYYY-MM-DD HH24:MI:SS')"
                        )
                    )
                except Exception as err:
                    LOGGER.error(
                        f'Could not fetch records from oracle due to:'
                        f' {str(err)}.'
                    )

            for row in self.cur:
                insert_dict = dict(
                    zip([x[0] for x in self.cur.description], row)
                )
                # Transform Date fields from datetime to string.
                insert_dict['LOCAL_DATE'] = (
                    str(insert_dict['LOCAL_DATE'])
                    if insert_dict['LOCAL_DATE'] is not None
                    else insert_dict['LOCAL_DATE']
                )

                insert_dict['ID'] = '.'.join([
                    insert_dict['CLIMATE_IDENTIFIER'],
                    insert_dict['LOCAL_YEAR'],
                    insert_dict['LOCAL_MONTH'],
                    insert_dict['LOCAL_DAY']
                ])
                if insert_dict['STN_ID'] in stn_dict:
                    coords = stn_dict[insert_dict['STN_ID']]['coordinates']
                    insert_dict['PROVINCE_CODE'] = stn_dict[
                        insert_dict['STN_ID']
                    ]['PROVINCE_CODE']
                    insert_dict['STATION_NAME'] = stn_dict[
                        insert_dict['STN_ID']
                    ]['STATION_NAME']
                    wrapper = {
                        'type': 'Feature',
                        'properties': insert_dict,
                        'geometry': {'type': 'Point', 'coordinates': coords},
                    }
                    action = {
                        '_id': insert_dict['ID'],
                        '_index': 'climate_public_daily_data',
                        '_op_type': 'update',
                        'doc': wrapper,
                        'doc_as_upsert': True,
                    }
                    yield action
                else:
                    LOGGER.error(
                        f"Bad STN ID: {insert_dict['STN_ID']}, skipping"
                        f" records for this station"
                    )

    def generate_hourly_data(self, stn_dict, date=None):
        """
        Queries hourly data from the db, and reformats
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
                    self.cur.execute(
                        f'select * from CCCS_PORTAL.PUBLIC_HOURLY_DATA '
                        f'where STN_ID={station}'
                    )
                except Exception as err:
                    LOGGER.error(
                        f'Could not fetch records from oracle due to:'
                        f' {str(err)}.'
                    )
            else:
                try:
                    self.cur.execute(
                        (
                            f"select * from CCCS_PORTAL.PUBLIC_HOURLY_DATA "
                            f"where STN_ID={station} and "
                            f"LOCAL_DATE >= TO_TIMESTAMP('{date} 00:00:00', "
                            f"'YYYY-MM-DD HH24:MI:SS')"
                        )
                    )
                except Exception as err:
                    LOGGER.error(
                        f'Could not fetch records from oracle due to:'
                        f' {str(err)}.'
                    )

            for row in self.cur:
                insert_dict = dict(
                    zip([x[0] for x in self.cur.description], row)
                )
                # Transform Date fields from datetime to string.
                insert_dict['LOCAL_DATE'] = (
                    str(insert_dict['LOCAL_DATE'])
                    if insert_dict['LOCAL_DATE'] is not None
                    else insert_dict['LOCAL_DATE']
                )

                insert_dict['ID'] = '.'.join([
                    insert_dict['CLIMATE_IDENTIFIER'],
                    insert_dict['LOCAL_YEAR'],
                    insert_dict['LOCAL_MONTH'],
                    insert_dict['LOCAL_DAY'],
                    insert_dict['LOCAL_HOUR']
                ])
                if insert_dict['STN_ID'] in stn_dict:
                    coords = stn_dict[insert_dict['STN_ID']]['coordinates']
                    insert_dict['PROVINCE_CODE'] = stn_dict[
                        insert_dict['STN_ID']
                    ]['PROVINCE_CODE']
                    insert_dict['STATION_NAME'] = stn_dict[
                        insert_dict['STN_ID']
                    ]['STATION_NAME']
                    wrapper = {
                        'type': 'Feature',
                        'properties': insert_dict,
                        'geometry': {'type': 'Point', 'coordinates': coords},
                    }
                    action = {
                        '_id': insert_dict['ID'],
                        '_index': 'climate_public_hourly_data',
                        '_op_type': 'update',
                        'doc': wrapper,
                        'doc_as_upsert': True,
                    }
                    yield action
                else:
                    LOGGER.error(
                        f"Bad STN ID: {insert_dict['STN_ID']}, skipping"
                        f" records for this station"
                    )

    def get_station_data(self, station, starting_from):
        """
        Creates a mapping of station ID to station coordinates and province
        name.

        :param cur: oracle cursor to perform queries against.

        :returns: A dictionary of dictionaries containing
                station coordinates and province name keyed by station ID.
        """
        stn_dict = collections.OrderedDict()
        try:
            if station:
                if starting_from:
                    self.cur.execute(
                        (
                            f'select STN_ID, LONGITUDE_DECIMAL_DEGREES, '
                            f'LATITUDE_DECIMAL_DEGREES, ENG_PROV_NAME, '
                            f'FRE_PROV_NAME, PROV_STATE_TERR_CODE, '
                            f'STATION_NAME, CLIMATE_IDENTIFIER '
                            f'from CCCS_PORTAL.STATION_INFORMATION '
                            f'where STN_ID >= {station} '
                            f'order by STN_ID'
                        )
                    )
                else:
                    self.cur.execute(
                        (
                            f'select STN_ID, LONGITUDE_DECIMAL_DEGREES, '
                            f'LATITUDE_DECIMAL_DEGREES, ENG_PROV_NAME, '
                            f'FRE_PROV_NAME, PROV_STATE_TERR_CODE, '
                            f'STATION_NAME, CLIMATE_IDENTIFIER '
                            f'from CCCS_PORTAL.STATION_INFORMATION '
                            f'where STN_ID = {station} '
                            f'order by STN_ID'
                        )
                    )
            else:
                self.cur.execute(
                    (
                        'select STN_ID, LONGITUDE_DECIMAL_DEGREES, '
                        'LATITUDE_DECIMAL_DEGREES, ENG_PROV_NAME, '
                        'FRE_PROV_NAME, PROV_STATE_TERR_CODE, '
                        'STATION_NAME, CLIMATE_IDENTIFIER '
                        'from CCCS_PORTAL.STATION_INFORMATION '
                        'order by STN_ID'
                    )
                )
        except Exception as err:
            LOGGER.error(
                f'Could not fetch records from oracle due to: {str(err)}.'
            )

        for row in self.cur:
            stn_dict[row[0]] = {
                'coordinates': [row[1], row[2]],
                'ENG_PROV_NAME': row[3],
                'FRE_PROV_NAME': row[4],
                'PROVINCE_CODE': row[5].strip(),  # remove the strip
                'STATION_NAME': row[6],
                'CLIMATE_IDENTIFIER': row[7].strip(),
            }
        return stn_dict

    def get_normals_data(self):
        """
        Creates a mapping of normal ID to pub_name and period.

        :param cur: oracle cursor to perform queries against.

        :returns: A dictionary of dictionaries containing
                pub_name and period keyed by normal ID.
        """
        normals_dict = {}
        try:
            self.cur.execute(
                (
                    'select NORMAL_ID, E_NORMAL_ELEMENT_NAME, '
                    'F_NORMAL_ELEMENT_NAME, PERIOD '
                    'from CCCS_PORTAL.VALID_NORMALS_ELEMENTS'
                )
            )
        except Exception as err:
            LOGGER.error(
                f'Could not fetch records from oracle due to: {str(err)}.'
            )

        for row in self.cur:
            normals_dict[row[0]] = {
                'E_NORMAL_ELEMENT_NAME': row[1],
                'F_NORMAL_ELEMENT_NAME': row[2],
                'PERIOD': row[3],
            }
        return normals_dict

    def get_normals_periods(self):
        """
        Creates a mapping of normal period ID to period begin and end.

        :param cur: oracle cursor to perform queries against.

        :returns: A dictionary of dictionaries containing
                period begin and end keyed by normal period ID.
        """
        period_dict = {}
        try:
            self.cur.execute(
                (
                    'select NORMAL_PERIOD_ID, PERIOD_BEGIN, PERIOD_END '
                    'from CCCS_PORTAL.NORMAL_PERIODS'
                )
            )
        except Exception as err:
            LOGGER.error(
                f'Could not fetch records from oracle due to: {str(err)}.'
            )

        for row in self.cur:
            period_dict[row[0]] = {
                'PERIOD_BEGIN': row[1],
                'PERIOD_END': row[2],
            }
        return period_dict


@click.group()
def climate_archive():
    """Manages climate archive indices"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_DB()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_BATCH_SIZE()
@cli_options.OPTION_DATASET(
    type=click.Choice(
        ['all', 'stations', 'normals', 'monthly', 'daily', 'hourly']
    ),
)
@click.option(
    '--station', help='station ID (STN_ID) of station to load', required=False,
)
@click.option(
    '--starting_from',
    help=' Load all stations starting from specified station',
    required=False,
)
@click.option(
    '--date', help='Start date to fetch updates (YYYY-MM-DD)', required=False
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
    station=None,
    starting_from=False,
    date=None,
):
    """Loads MSC Climate Archive data from Oracle into Elasticsearch"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = ClimateArchiveLoader(db, conn_config)

    if dataset == 'all':
        datasets_to_process = [
            'hourly',
            'daily',
            'monthly',
            'normals',
            'stations',
        ]
    else:
        datasets_to_process = [dataset]

    click.echo(f'Processing dataset(s): {datasets_to_process}')

    if 'stations' in datasets_to_process:
        try:
            click.echo('Populating stations index')
            loader.create_index('stations')
            stations = loader.generate_stations()
            loader.conn.submit_elastic_package(stations, batch_size)
        except Exception as err:
            msg = f'Could not populate stations index: {err}'
            raise click.ClickException(msg)

    if 'normals' in datasets_to_process:
        try:
            click.echo('Populating normals index')
            stn_dict = loader.get_station_data(station, starting_from)
            normals_dict = loader.get_normals_data()
            periods_dict = loader.get_normals_periods()
            loader.create_index('normals')
            normals = loader.generate_normals(
                stn_dict, normals_dict, periods_dict
            )
            loader.conn.submit_elastic_package(normals, batch_size)
        except Exception as err:
            msg = f'Could not populate normals index: {err}'
            raise click.ClickException(msg)

    if 'monthly' in datasets_to_process:
        try:
            click.echo('Populating monthly index')
            stn_dict = loader.get_station_data(station, starting_from)
            if not (date or station or starting_from):
                loader.create_index('monthly_summary')
            monthlies = loader.generate_monthly_data(stn_dict, date)
            loader.conn.submit_elastic_package(monthlies, batch_size)
        except Exception as err:
            msg = f'Could not populate montly index: {err}'
            raise click.ClickException(msg)

    if 'daily' in datasets_to_process:
        try:
            click.echo('Populating daily index')
            stn_dict = loader.get_station_data(station, starting_from)
            if not (date or station or starting_from):
                loader.create_index('daily_summary')
            dailies = loader.generate_daily_data(stn_dict, date)
            loader.conn.submit_elastic_package(dailies, batch_size)
        except Exception as err:
            msg = f'Could not populate daily index: {err}'
            raise click.ClickException(msg)

    if 'hourly' in datasets_to_process:
        try:
            click.echo('Populating hourly index')
            stn_dict = loader.get_station_data(station, starting_from)
            if not (date or station or starting_from):
                loader.create_index('hourly_summary')
            hourlies = loader.generate_hourly_data(stn_dict, date)
            loader.conn.submit_elastic_package(hourlies, batch_size)
        except Exception as err:
            msg = f'Could not populate hourly index: {err}'
            raise click.ClickException(msg)

    loader.db_conn.close()


climate_archive.add_command(add)
