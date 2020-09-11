# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2020 Tom Kralidis
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
from elasticsearch import helpers, logger as elastic_logger

from msc_pygeoapi import cli_options
from msc_pygeoapi.env import (MSC_PYGEOAPI_CACHEDIR, MSC_PYGEOAPI_ES_TIMEOUT,
                              MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import get_es


LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(logging.WARNING)

STATIONS_LIST_NAME = 'hydrometric_StationList.csv'
STATIONS_LIST_URL = 'https://dd.weather.gc.ca/hydrometric/doc/{}' \
    .format(STATIONS_LIST_NAME)

STATIONS_CACHE = os.path.join(MSC_PYGEOAPI_CACHEDIR, STATIONS_LIST_NAME)

# cleanup settings
DAYS_TO_KEEP = 30

# index settings
INDEX_NAME = 'hydrometric_realtime'

SETTINGS = {
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
                        'format': 'strict_date_hour_minute_second'
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

    def __init__(self, plugin_def):
        """initializer"""

        BaseLoader.__init__(self)

        self.ES = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

        if not self.ES.indices.exists(INDEX_NAME):
            self.ES.indices.create(index=INDEX_NAME, body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)

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
                raise EOFError('Stations file at {} is empty'
                               .format(STATIONS_CACHE))

            self.stations.clear()
            for row in reader:
                if len(row) > 6:
                    LOGGER.warning('Station list row has too many values: {}'
                                   ' (using first 6)'.format(row))
                elif len(row) < 6:
                    LOGGER.error('Station list row has too few values: {}'
                                 ' (skipping)'.format(row))
                    continue

                stn_id, name, lat, lon, province, timezone = row[:6]

                try:
                    lat = float(lat)
                    lon = float(lon)
                except ValueError:
                    LOGGER.error('Cannot interpret coordinates ({}, {}) for'
                                 ' station {} (skipping)'
                                 .format(lon, lat, stn_id))
                    continue

                utcoffset = timezone[4:]
                if utcoffset.strip() == '':
                    LOGGER.error('Cannot interpret UTC offset {} for station'
                                 ' {} (skipping)'.format(timezone, stn_id))
                    continue

                LOGGER.debug(
                    'Station {}: name={}, province/territory={},'
                    ' coordinates={}, utcoffset={}'
                    .format(stn_id, name, province, (lon, lat), utcoffset))

                stn_info = {
                    'STATION_NAME': name,
                    'PROV_TERR_STATE_LOC': province,
                    'UTCOFFSET': utcoffset,
                    'coordinates': (lon, lat)
                }

                self.stations[stn_id] = stn_info

        LOGGER.debug('Collected stations information: loaded {} stations'
                     .format(len(self.stations)))

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
                    LOGGER.warning('Data row in {} has too many values:'
                                   ' {} (using only first 10)'
                                   .format(filepath, row))
                elif len(row) < 10:
                    LOGGER.error('Data row in {} has too few values: {}'
                                 ' (skipping)'.format(filepath, row))
                    continue

                station, date, level, _, level_symbol, _, \
                    discharge, _, discharge_symbol, _ = row

                if station in self.stations:
                    stn_info = self.stations[station]
                    LOGGER.debug('Found info for station {}'.format(station))
                else:
                    LOGGER.error('Cannot find info for station {} (skipping)'
                                 .format(station))
                    continue

                try:
                    # Convert timestamp to UTC time.
                    utc_datetime = delocalize_date(date)
                    utc_datestamp = utc_datetime.strftime('%Y-%m-%d.%H:%M:%S')
                    # Generate an ID now that all fields are known.
                    observation_id = '{}.{}'.format(station, utc_datestamp)

                    utc_datestamp = utc_datestamp.replace('.', 'T')
                except Exception as err:
                    LOGGER.error('Cannot interpret datetime value {} in {}'
                                 ' due to: {} (skipping)'
                                 .format(date, filepath, str(err)))
                    continue

                if 'daily' in filepath and utc_datetime > hourly_domain_start:
                    LOGGER.debug('Daily observation {} overlaps hourly data'
                                 ' (skipping)'.format(observation_id))
                    continue
                elif utc_datetime < daily_domain_start:
                    LOGGER.debug('Daily observation {} precedes retention'
                                 ' period (skipping)'.format(observation_id))
                    continue

                LOGGER.debug('Generating observation {} from {}: datetime={},'
                             ' level={}, discharge={}'
                             .format(observation_id, filepath, utc_datestamp,
                                     level, discharge))

                try:
                    level = float(level) if level.strip() else None
                except ValueError:
                    LOGGER.error('Cannot interpret level value {}'
                                 ' (setting null)'.format(level))

                try:
                    discharge = float(discharge) if discharge.strip() else None
                except ValueError:
                    LOGGER.error('Cannot interpret discharge value {}'
                                 ' (setting null)'.format(discharge))

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
                        'LEVEL': level,
                        'DISCHARGE': discharge,
                        'LEVEL_SYMBOL_EN': level_symbol_en,
                        'LEVEL_SYMBOL_FR': level_symbol_fr,
                        'DISCHARGE_SYMBOL_EN': discharge_symbol_en,
                        'DISCHARGE_SYMBOL_FR': discharge_symbol_fr
                    }
                }

                LOGGER.debug('Observation {} created successfully'
                             .format(observation_id))
                action = {
                    '_id': observation_id,
                    '_index': INDEX_NAME,
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

        inserts = 0
        updates = 0
        noops = 0
        fails = 0

        LOGGER.debug('Received file {}'.format(filepath))
        chunk_size = 80000

        package = self.generate_observations(filepath)
        for ok, response in helpers.streaming_bulk(self.ES, package,
                                                   chunk_size=chunk_size,
                                                   request_timeout=30):
            status = response['update']['result']

            if status == 'created':
                inserts += 1
            elif status == 'updated':
                updates += 1
            elif status == 'noop':
                noops += 1
            else:
                LOGGER.warning('Unhandled status code {}'.format(status))

        total = inserts + updates + noops + fails
        LOGGER.info('Inserted package of {} observations ({} inserts,'
                    ' {} updates, {} no-ops, {} rejects)'
                    .format(total, inserts, updates, noops, fails))
        return True


def download_stations():
    """
    Download realtime stations

    :returns: void
    """

    LOGGER.debug('Caching {} to {}'.format(STATIONS_LIST_URL, STATIONS_CACHE))
    urllib.request.urlretrieve(STATIONS_LIST_URL, STATIONS_CACHE)


@click.group()
def hydrometric_realtime():
    """Manages hydrometric realtime index"""
    pass


@click.command()
@click.pass_context
def cache_stations(ctx):
    """Cache local copy of hydrometric realtime stations index"""

    click.echo('Caching realtime stations to {}'.format(STATIONS_CACHE))
    download_stations()


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help='Delete documents older than n days (default={})'
)
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old documents?'
)
def clean_records(ctx, days):
    """Delete old documents"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    today = datetime.now().replace(hour=0, minute=0)
    older_than = (today - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M')
    click.echo('Deleting documents older than {} ({} full days)'
               .format(older_than.replace('T', ' '), days))

    query = {
        'query': {
            'range': {
                'properties.DATETIME': {
                    'lt': older_than,
                    'format': 'strict_date_hour_minute'
                }
            }
        }
    }

    response = es.delete_by_query(index=INDEX_NAME, body=query,
                                  request_timeout=90)

    click.echo('Deleted {} documents'.format(response['deleted']))
    if len(response['failures']) > 0:
        click.echo('Failed to delete {} documents in time range'
                   .format(len(response['failures'])))


@click.command()
@click.pass_context
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete these indexes?'
)
def delete_index(ctx):
    """Delete hydrometric realtime indexes"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)


hydrometric_realtime.add_command(cache_stations)
hydrometric_realtime.add_command(clean_records)
hydrometric_realtime.add_command(delete_index)
