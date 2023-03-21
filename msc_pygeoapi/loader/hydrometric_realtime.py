# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2023 Tom Kralidis
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
import csv
from datetime import datetime, timedelta
import logging
import os
import urllib.request

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_CACHEDIR
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    check_es_indexes_to_delete,
    configure_es_connection,
    DATETIME_RFC3339_FMT
)


LOGGER = logging.getLogger(__name__)

STATIONS_LIST_NAME = 'hydrometric_StationList.csv'
STATIONS_LIST_URL = f'https://dd.weather.gc.ca/hydrometric/doc/{STATIONS_LIST_NAME}'  # noqa

STATIONS_CACHE = os.path.join(MSC_PYGEOAPI_CACHEDIR, STATIONS_LIST_NAME)

# cleanup settings
DAYS_TO_KEEP = 30

# index settings
INDEX_BASENAME = 'hydrometric_realtime.'

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [f'{INDEX_BASENAME}*'],
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
                    'IDENTIFIER': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                     },
                    'STATION_NUMBER': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'STATION_NAME': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'PROV_TERR_STATE_LOC': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'DATETIME': {
                        'type': 'date',
                        'format': 'date_time_no_millis||strict_date_optional_time'  # noqa
                    },
                    'DATETIME_LST': {
                        'type': 'date',
                    },
                    'LEVEL': {
                        'type': 'float'
                    },
                    'DISCHARGE': {
                        'type': 'float'
                    },
                    'LEVEL_SYMBOL_EN': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'LEVEL_SYMBOL_FR': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'DISCHARGE_SYMBOL_EN': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'DISCHARGE_SYMBOL_FR': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    }
                }
            }
        }
    }
}


def delocalize_date(date_string):
    """
    Converts the datetime in <date_string> from LST (Local Standard Time)
    to UTC. Requires <date_string> to contain an offset timestamp from UTC
    at the end of the string. Returns a datetime.datetime instance.

    :param date_string: A timestamp in LST, complete with a UTC offset
    :returns: A datetime.datetime instance representing the time in UTC
    """

    datestamp = date_string[:-6]
    sign = date_string[-6]
    utcoffset = date_string[-5:]

    offset_hours, offset_minutes = map(int, utcoffset.split(':'))
    if sign == '-':
        offset_hours = -offset_hours
        offset_minutes = -offset_minutes

    dt = datetime.strptime(datestamp, '%Y-%m-%dT%H:%M:%S')
    adjustment = timedelta(hours=offset_hours, minutes=offset_minutes)

    return dt - adjustment


class HydrometricRealtimeLoader(BaseLoader):
    """Hydrometric Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)

        index_template = self.conn.get_template(INDEX_BASENAME)

        # compare index template mappping with mapping defined in SETTINGS
        if index_template:
            # if mappings are different, update the index template
            if (
                index_template[INDEX_BASENAME]['mappings']
                != SETTINGS['mappings']
            ):
                LOGGER.info(
                    f'Updating "{INDEX_BASENAME}" index template with'
                    ' mapping changes in provider.'
                )
                self.conn.create_template(
                    INDEX_BASENAME, SETTINGS, overwrite=True
                )
        else:
            self.conn.create_template(INDEX_BASENAME, SETTINGS)

        self.stations = {}
        self.read_stations_list()

    def read_stations_list(self):
        """
        Parses the local copy of the hydrometric stations list, creating
        a dictionary of station IDs to station info and putting it in
        <self.stations>.

        :returns: void
        """

        if not os.path.exists(STATIONS_CACHE):
            download_stations()

        with open(STATIONS_CACHE) as stations_file:
            reader = csv.reader(stations_file)

            try:
                # Discard one row of headers
                next(reader)
            except StopIteration:
                raise EOFError(f'Stations file at {STATIONS_CACHE} is empty')

            self.stations.clear()
            for row in reader:
                if len(row) > 6:
                    LOGGER.warning(f'Station list row has too many values: {row}' # noqa
                                   ' (using first 6)')
                elif len(row) < 6:
                    LOGGER.error(f'Station list row has too few values: {row}'
                                 ' (skipping)')
                    continue

                stn_id, name, lat, lon, province, timezone = row[:6]

                try:
                    lat = float(lat)
                    lon = float(lon)
                except ValueError:
                    LOGGER.error(f'Cannot interpret coordinates ({lon}, {lat}) for'  # noqa
                                 f' station {stn_id} (skipping)')
                    continue

                utcoffset = timezone[4:]
                if utcoffset.strip() == '':
                    LOGGER.error(f'Cannot interpret UTC offset {timezone} for station {stn_id} (skipping)')  # noqa
                    continue

                LOGGER.debug(
                    f'Station {stn_id}: name={name}, province/territory={province},'  # noqa
                    f' coordinates={(lon, lat)}, utcoffset={utcoffset}')

                stn_info = {
                    'STATION_NAME': name,
                    'PROV_TERR_STATE_LOC': province,
                    'UTCOFFSET': utcoffset,
                    'coordinates': (lon, lat)
                }

                self.stations[stn_id] = stn_info

        LOGGER.debug(f'Collected stations information: loaded {len(self.stations)} stations')  # noqa

    def generate_observations(self, filepath):
        """
        Generates and yields a series of observations, one for each row in
        <filepath>. Observations are returned as Elasticsearch bulk API
        upsert actions, with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :param filename: Path to a data file of realtime hydrometric
        :returns: Generator of Elasticsearch actions to upsert the observations
        """

        today = datetime.utcnow()
        today_start = datetime(year=today.year, month=today.month,
                               day=today.day)

        hourly_domain_start = today_start - timedelta(days=2)
        daily_domain_start = today_start - timedelta(days=DAYS_TO_KEEP)

        with open(filepath) as ff:
            reader = csv.reader(ff)
            # Discard one row of headers
            next(reader)

            for row in reader:
                if len(row) > 10:
                    LOGGER.warning(f'Data row in {filepath} has too many values:'  # noqa
                                   f' {row} (using only first 10)')
                elif len(row) < 10:
                    LOGGER.error(f'Data row in {filepath} has too few values: {row}')  # noqa
                    continue

                station, date_, level, _, level_symbol, _, \
                    discharge, _, discharge_symbol, _ = row

                if station in self.stations:
                    stn_info = self.stations[station]
                    LOGGER.debug(f'Found info for station {station}')
                else:
                    LOGGER.error(f'Cannot find info for station {station} (skipping)')  # noqa
                    continue

                try:
                    # Convert timestamp to UTC time.
                    utc_datetime = delocalize_date(date_)
                    utc_datestamp = utc_datetime.strftime(DATETIME_RFC3339_FMT)
                    # Generate an ID now that all fields are known.
                    observation_id = f'{station}.{utc_datestamp}'

                except Exception as err:
                    LOGGER.error(f'Cannot interpret datetime value {date_} in {filepath}'  # noqa
                                 f' due to: {err} (skipping)')
                    continue

                if 'daily' in filepath and utc_datetime > hourly_domain_start:
                    LOGGER.debug(f'Daily observation {observation_id} overlaps hourly data'  # noqa
                                 ' (skipping)')
                    continue
                elif utc_datetime < daily_domain_start:
                    LOGGER.debug(f'Daily observation {observation_id} precedes retention'  # noqa
                                 ' period (skipping)')
                    continue

                LOGGER.debug(f'Generating observation {observation_id} from {filepath}: datetime={utc_datestamp},'  # noqa
                             f' level={level}, discharge={discharge}')

                try:
                    level = float(level) if level.strip() else None
                except ValueError:
                    LOGGER.error(f'Cannot interpret level value {level}'
                                 ' (setting null)')

                try:
                    discharge = float(discharge) if discharge.strip() else None
                except ValueError:
                    LOGGER.error(f'Cannot interpret discharge value {discharge} (setting null)')  # noqa

                if level_symbol.strip() == '':
                    level_symbol_en = None
                    level_symbol_fr = None
                if discharge_symbol.strip() == '':
                    discharge_symbol_en = None
                    discharge_symbol_fr = None

                observation = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': stn_info['coordinates']
                    },
                    'properties': {
                        'IDENTIFIER': observation_id,
                        'STATION_NUMBER': station,
                        'STATION_NAME': stn_info['STATION_NAME'],
                        'PROV_TERR_STATE_LOC': stn_info['PROV_TERR_STATE_LOC'],
                        'DATETIME': utc_datestamp,
                        'DATETIME_LST': date_,
                        'LEVEL': level,
                        'DISCHARGE': discharge,
                        'LEVEL_SYMBOL_EN': level_symbol_en,
                        'LEVEL_SYMBOL_FR': level_symbol_fr,
                        'DISCHARGE_SYMBOL_EN': discharge_symbol_en,
                        'DISCHARGE_SYMBOL_FR': discharge_symbol_fr
                    }
                }

                LOGGER.debug(f'Observation {observation_id} created successfully')  # noqa

                es_index = f"{INDEX_BASENAME}{utc_datetime.strftime('%Y-%m-%d')}"  # noqa

                action = {
                    '_id': observation_id,
                    '_index': es_index,
                    '_op_type': 'update',
                    'doc': observation,
                    'doc_as_upsert': True
                }

                yield action

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        if filepath.endswith('hydrometric_StationList.csv'):
            return True

        LOGGER.debug(f'Received file {filepath}')

        package = self.generate_observations(filepath)
        self.conn.submit_elastic_package(package, request_size=80000)

        return True


def download_stations():
    """
    Download realtime stations

    :returns: void
    """

    LOGGER.debug(f'Caching {STATIONS_LIST_URL} to {STATIONS_CACHE}')
    urllib.request.urlretrieve(STATIONS_LIST_URL, STATIONS_CACHE)


@click.group()
def hydrometric_realtime():
    """Manages hydrometric realtime indices"""
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
    """adds data to system"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.csv')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = HydrometricRealtimeLoader(conn_config)
        loader.load_data(file_to_process)

    click.echo('Done')


@click.command()
@click.pass_context
def cache_stations(ctx):
    """Cache local copy of hydrometric realtime stations index"""

    click.echo(f'Caching realtime stations to {STATIONS_CACHE}')
    download_stations()


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help=f'Delete indexes older than n days (default={DAYS_TO_KEEP})'
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old indexes?'
)
def clean_indexes(ctx, days, es, username, password, ignore_certs):
    """Clean hydrometric realtime indexes older than n number of days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get(f'{INDEX_BASENAME}*')

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo(f'Deleting indexes {indexes_to_delete}')
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete these indexes?'
)
def delete_indexes(ctx, es, username, password, ignore_certs, index_template):
    """"Delete all hydrometric realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    all_indexes = f'{INDEX_BASENAME}*'

    click.echo(f'Deleting indexes {all_indexes}')
    conn.delete(all_indexes)

    if index_template:
        click.echo(f'Deleting index template {INDEX_BASENAME}')
        conn.delete_template(INDEX_BASENAME)

    click.echo('Done')


hydrometric_realtime.add_command(add)
hydrometric_realtime.add_command(cache_stations)
hydrometric_realtime.add_command(clean_indexes)
hydrometric_realtime.add_command(delete_indexes)
