# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2022 Etienne Pelletier
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
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import re

import click
from elasticsearch import logger as elastic_logger
from parse import parse

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_CACHEDIR,
    MSC_PYGEOAPI_LOGGING_LOGLEVEL
)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    generate_datetime_range,
    DATETIME_RFC3339_FMT
)

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

# cleanup settings
HOURS_TO_KEEP = 6

# index settings
PRECIP_TYPES = {'mmhr': 'rrai', 'cmhr': 'rsno'}
COVERAGE_TYPES = {
    'Merged.json': {
        'geomet_layer_name': 'RADAR_COVERAGE_{}',
        'type': 'merged',
        'pattern': '{datetime}_MSC_Radar-Coverage_{precip_type}-Merged.json'
    },
    'Merged-Inv.json': {
        'geomet_layer_name': 'RADAR_COVERAGE_{}.INV',
        'type': 'merged_inverted',
        'pattern': '{datetime}_MSC_Radar-Coverage_{precip_type}-Merged-Inv.json'  # noqa
    }
}
INDEX_BASENAME = 'radar_coverage-{}-{}-realtime'

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': ['radar_coverage-*'],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {
                'properties': {
                    'type': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    },
                    'datetime': {
                        'type': 'date',
                        'format': 'date_time_no_millis',
                        'ignore_malformed': False,
                    },
                    '_datetime': {
                        'type': 'date',
                        'format': 'yyyy/M/d H:m:s.SSS',
                    }
                }
            }
        }
    }
}


class RadarCoverageRealtimeLoader(BaseLoader):
    """Radar Coverage Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)
        self.conn = ElasticsearchConnector(conn_config)
        self.config_file = None
        self.filepath = None
        self.precip_type = None
        self.product_type = None
        self.datetime = None
        # radar coverage settings
        self.interval_minutes = 6
        self.retention_time_minutes = 180

        self.conn.create_template('radar_coverage', SETTINGS)

    def parse_filename(self):
        """
        Parses a radar coverage filename order to get the date, forecast
        time, and precipitation type.

        :returns: `bool` of parse status
        """

        filename = self.filepath.name

        # parse filepath
        for key in COVERAGE_TYPES.keys():
            if self.filepath.name.endswith(key):
                pattern = COVERAGE_TYPES[key]['pattern']
                parsed_filename = parse(pattern, filename)
                self.precip_type = parsed_filename.named['precip_type'].lower()
                self.geomet_layer_name = COVERAGE_TYPES[key][
                    'geomet_layer_name'
                ].format(PRECIP_TYPES[self.precip_type].upper())
                self.product_type = COVERAGE_TYPES[key]['type']
                break

        self.config_file = (
            Path(MSC_PYGEOAPI_CACHEDIR)
            / f'radar_coverage_{self.precip_type}_{self.product_type}_realtime.json'  # noqa
        )

        self.datetime = datetime.strptime(
            parsed_filename.named['datetime'], '%Y%m%dT%H%MZ'
        )

        return True

    def generate_geojson_features(self):
        """
        Generates and yields a series of radar coverage features.
        Radar coverage is returned as Elasticsearch bulk API upsert actions,
        with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :returns: Generator of Elasticsearch actions to upsert the radar
                  coverage forecasts/observations
        """

        with self.filepath.resolve().open() as f:
            data = json.load(f)
            features = data['features']

        for feature in features:
            # set ES index name for feature
            es_index = INDEX_BASENAME.format(
                self.precip_type,
                self.product_type
            )

            # add properties
            feature['properties']['precip_type'] = self.precip_type.upper()
            feature['properties']['datetime'] = self.datetime.strftime(
                DATETIME_RFC3339_FMT
            )
            feature['properties'][
                '_datetime'
            ] = self.datetime.strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]

            # set feature ID
            dt_numeric = re.sub(
                '[^0-9]',
                '',
                self.datetime.strftime(DATETIME_RFC3339_FMT)
            )

            id = f'{self.geomet_layer_name}-{dt_numeric}'

            # create ES action
            action = {
                '_id': id,
                '_index': es_index,
                '_op_type': 'update',
                'doc': feature,
                'doc_as_upsert': True
            }

            yield action

    def verify_full_extent_available(self, forecast_time):
        """
        Check that the previous 31 timesteps are available for a given
        precipitation type and datetime

        :param forecast_time: `datetime` of the forecast time

        :returns: `bool` of verification status
        """

        start = forecast_time - timedelta(minutes=self.retention_time_minutes)

        expected_intervals = [
            dt
            for dt in generate_datetime_range(
                start, forecast_time, timedelta(minutes=self.interval_minutes)
            )
        ]

        query = {
            'query': {
                'range': {
                    'properties.datetime': {
                        'gte': start.strftime(DATETIME_RFC3339_FMT)
                    }
                }
            },
            'sort': {'properties.datetime': {'order': 'asc'}}
        }

        results = self.conn.Elasticsearch.search(
            index=f'radar_coverage-{self.precip_type}-{self.product_type}-realtime',  # noqa
            body=query,
            _source=['properties.datetime'],
            size=len(expected_intervals)
        )

        retrieved_datetimes = [
            datetime.strptime(
                hit['_source']['properties']['datetime'], DATETIME_RFC3339_FMT
            )
            for hit in results['hits']['hits']
        ]

        # identify expect intervals not retrieved from ES
        diff = list(set(expected_intervals) - set(retrieved_datetimes))
        diff.sort()
        diff_str = [dt.strftime(DATETIME_RFC3339_FMT) for dt in diff]

        # if there are expected intervals missing log an error
        if diff:
            LOGGER.error(
                f'Missing {self.geomet_layer_name} forecast hours (n='
                f'{len(diff)}) detected between {start} and {forecast_time}: '
                f'{", ".join(diff_str)}'
            )

        return retrieved_datetimes == expected_intervals

    def generate_precip_config_dict(self, datetime=None):
        """
        Generates a dictionary of default datetime and 3-hour ISO8601 period
        for a given precipitation type.

        :param datetime: `datetime` of default forecast time to set

        :returns: `dict` of precipitation temporal config
        """

        if datetime is None:
            datetime = self.datetime

        start = (self.datetime - timedelta(hours=3)).strftime(
            DATETIME_RFC3339_FMT
        )
        end = self.datetime.strftime(DATETIME_RFC3339_FMT)

        config = {
            'default_time': datetime.strftime(DATETIME_RFC3339_FMT),
            'time_extent': f'{start}/{end}/PT6M'
        }

        return config

    def update_temporal_config(self):
        '''
        Create/update the temporal configuration file
        for the precipitation type/coverage type combination

        returns: `bool` of update status
        '''
        updated_config = None

        if self.config_file.exists():
            with self.config_file.open() as f:
                config = json.load(f)
                # get previous default time for precip type
                previous_default_datetime = datetime.strptime(
                    config['default_time'],
                    DATETIME_RFC3339_FMT
                )
                # update temporal config if new default time is
                # later than previous
                if self.datetime > previous_default_datetime:
                    # verify that the previous 31 timesteps are available
                    self.verify_full_extent_available(self.datetime)
                    # update config
                    updated_config = {
                        **config,
                        **self.generate_precip_config_dict()
                    }

        # temporal config file does not exist
        else:
            # check full extent available for current datetime
            # and create config file if it is
            if self.verify_full_extent_available(self.datetime):
                updated_config = self.generate_precip_config_dict()

        if not updated_config:
            return False

        LOGGER.info(
            f'Updating radar coverage {self.precip_type.upper()} temporal'
            f' configuration file...'
        )
        with self.config_file.open('w') as f:
            json.dump(updated_config, f)

        return True

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: `str` of filepath to load

        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)
        LOGGER.debug(f'Received file {self.filepath}')

        # set attributes from filename
        LOGGER.debug('Parsing filename...')
        self.parse_filename()

        # generate geojson features
        LOGGER.debug('Generating ES documents from features...')
        package = self.generate_geojson_features()

        LOGGER.debug('Submitting ES documents...')
        self.conn.submit_elastic_package(
            package, request_size=80000, refresh='wait_for'
        )

        # update radar coverage temporal configuration
        self.update_temporal_config()

        return True


@click.group()
def radar_coverage_realtime():
    """Manages radar coverage indexes"""
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
    """Add radar coverage data to Elasticsearch"""

    if file_ is None and directory is None:
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [
                file
                for file in files
                if any(file.endswith(type_) for type_ in COVERAGE_TYPES)
            ]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = RadarCoverageRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('Features not generated...')


def confirm(ctx, param, value):
    if not value and ctx.params['datetime']:
        click.confirm(
            f'Are you sure you want to delete {ctx.params["dataset"]} '
            / 'radar coverage documents older than '
            / f'{click.style(ctx.params["datetime"], fg="red")} ?',
            abort=True,
        )


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    type=click.Choice(['all', 'cmhr', 'mmhr']),
    help='Radar coverage index to clean',
)
@click.option(
    '--datetime',
    help='Delete radar coverage forecast hours older than YYYY-MM-DDTHH:MM:SSZ',  # noqa
    required=False,
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(callback=confirm)
def clean_indexes(
    ctx, dataset, datetime, es, username, password, ignore_certs
):
    """Delete old radar coverage documents older than n hours"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes_to_delete = 'radar_coverage-*'
    else:
        indexes_to_delete = f'radar_coverage-{dataset}*'

    query = {'query': {'range': {'properties.datetime': {'lt': datetime}}}}

    conn.Elasticsearch.delete_by_query(indexes_to_delete, query)

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='Radar coverage dataset indexes to delete.',
    type=click.Choice(['all', 'mmhr', 'cmhr'])
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(
    ctx, dataset, es, username, password, ignore_certs, index_template
):
    """Delete all radar coverage realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        indexes = 'radar_coverage-*'
    else:
        indexes = '{}*'.format(INDEX_BASENAME.format(dataset))

    click.echo('Deleting indexes {}'.format(indexes))

    conn.delete(indexes)

    if index_template:
        click.echo('Deleting index template radar_coverage...')
        conn.delete_template('radar_coverage')

    click.echo('Done')


radar_coverage_realtime.add_command(add)
radar_coverage_realtime.add_command(clean_indexes)
radar_coverage_realtime.add_command(delete_indexes)
