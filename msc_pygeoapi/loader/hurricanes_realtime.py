# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2024 Louis-Philippe Rousseau-Lambert
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

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30
ACTIVE_HOURS = 48

# Mapping for indexes
TEMPLATE_MAPPINGS = ['cyclone', 'track', 'error_cone', 'wind_radii']

# index settings
INDEX_BASENAME = 'hurricanes-{}.'

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

MAPPINGS = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'publication_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'forecast_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'validity_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'latest_publication': {
                    'type': 'boolean'
                },
                'active': {
                    'type': 'boolean'
                },
                'id': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'storm_name': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'file_name': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                }
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': [f'{INDEX_BASENAME}*'],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}


class HurricanesRealtimeLoader(BaseLoader):
    """Hurricane tracks loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.filepath = None
        self.datetime = None
        self.es_index = None
        self.storm_name = None
        self.file_id = None
        self.newer = None
        self.conn = ElasticsearchConnector(conn_config)

        for hurricane_type in TEMPLATE_MAPPINGS:
            template_name = INDEX_BASENAME.format(hurricane_type)
            SETTINGS['index_patterns'] = [f'{template_name}*']
            SETTINGS['mappings'] = MAPPINGS
            self.conn.create_template(template_name, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of hurricanes.
        They are returned as Elasticsearch bulk API upsert actions,
        with documents in GeoJSON to match the Elasticsearch index mappings.

        :returns: Generator of Elasticsearch actions to upsert the
                  hurricanes data
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)['features']

        filename = f.name.split('/')[-1]

        if len(data) > 0:
            for feature in data:
                if 'metobject' in feature['properties']:
                    # flatten metobject properties
                    metobj = feature['properties']['metobject']
                    for item, values in metobj.items():
                        try:
                            metobj_flat_item = self.flatten_json(item,
                                                                 values,
                                                                 'metobject')
                            feature['properties'].update(metobj_flat_item)
                        except Exception as err:
                            msg = f'Error while flattening hurricane {err}'
                            LOGGER.error(f'{msg}')
                            pass

                    del feature['properties']['metobject']

                # check if id is already in ES and if amendment is +=1
                amendment = feature['properties']['amendment']
                self.file_id = filename
                feature['properties']['file_name'] = self.file_id

                # set default status ti inactive
                # activate status will be set later with a ES request
                feature['properties']['active'] = False
                feature['properties']['latest_publication'] = False

                type_ = feature['properties']['type']
                date_ = datetime.strptime(
                    feature['properties']['publication_datetime'],
                    DATETIME_FORMAT
                )
                index_name = INDEX_BASENAME.format(type_)
                index_date = date_.strftime('%Y-%m-%d')
                self.es_index = f'{index_name}{index_date}'

                is_newer = self.check_if_newer(self.file_id, amendment)

                if is_newer['update']:
                    self.newer = True
                    feature['properties']['latest_publication'] = True

                    now = datetime.utcnow()
                    gte = (now - timedelta(hours=48)).strftime(DATETIME_FORMAT)
                    if feature['properties']['validity_datetime'] >= gte:
                        feature['properties']['active'] = True

                    # if this file is newer
                    # set all previous forecast publication latest
                    # to False for this hurricane storm name
                    # before adding the latest file with status latest = True
                    self.storm_name = feature['properties']['storm_name']

                action = {
                    '_id': feature['properties']['id'],
                    '_index': self.es_index,
                    '_op_type': 'update',
                    'doc': feature,
                    'doc_as_upsert': True
                }

                yield action

                for id_ in is_newer['id_list']:
                    self.conn.Elasticsearch.delete(index=self.es_index,
                                                   id=id_)

                # update active hurricane status
                # self.update_active_status(type_)

            else:
                LOGGER.warning(f'empty hurricane json in {filename}')

    def flatten_json(self, key, values, parent_key=''):
        """
        flatten GeoJSON properties

        :returns: item array
        """

        items = {}
        new_key = f'{parent_key}.{key}'
        value = values
        if isinstance(values, dict):
            for sub_key, sub_value in values.items():
                new_key = f'{parent_key}.{key}.{sub_key}'
                items[new_key] = sub_value
        else:
            items[new_key] = value
        return items

    def check_if_newer(self, file_id, amendment):
        """
        check if the hurricane json is the newest version

        :returns: `bool` if latest, id_list for ids to delete
        """

        upt_ = True
        id_list = []

        query = {
            'query': {
                'term': {
                    'properties.file_name.keyword': file_id
                }
            }
        }

        # Fetch the document
        try:
            result = self.conn.Elasticsearch.search(index=self.es_index,
                                                    body=query)
            if result:
                hit = result['hits']['hits'][0]
                es_amendement = hit['_source']['properties']['amendment']
                if es_amendement >= amendment:
                    upt_ = False
                else:
                    for id_ in result['hits']['hits']:
                        id_list.append(id_['_id'])
        except Exception:
            LOGGER.warning(f'Item ({file_id}) does not exist in index')

        return {'update': upt_, 'id_list': id_list}

    def update_latest_status(self, storm_name, file_id):
        """
        update latest status for the latest hurricane publication

        :return `bool` of update status
        """

        query = {'script': {
                    'source': '''
                        ctx._source.properties.latest_publication = false;
                        ctx._source.properties.active = false;
                    ''',
                    'lang': 'painless'
                },
                'query': {
                        'bool': {
                            'must': [{
                                'match': {
                                    'properties.storm_name': storm_name
                                }
                            }],
                            'must_not': [{
                                'match': {
                                    'properties.file_name': file_id
                                    }
                            }]
                        }
                    }
                }

        # create list of today and yesterday index
        wildcard = '*'
        index_ = f'{INDEX_BASENAME.format(wildcard)}*'
        try:
            self.conn.update_by_query(query, index_)
        except Exception as err:
            LOGGER.warning(f'Failed to update ES index: {err}')

        return True

    def update_active_status(self, in_hours):
        """
        update active status for the latest hurricane

        :return `bool` of update status
        """

        now = datetime.utcnow()
        lt_date = (now - timedelta(hours=in_hours)).strftime(DATETIME_FORMAT)

        query = {'script': {
            'source': 'ctx._source.properties.active=false',
            'lang': 'painless'
            },
            'query': {
                    'bool': {
                        'must': [{
                            'match': {
                                'properties.active': 'true'
                            }
                        }, {
                            'range': {
                                'properties.validity_datetime': {
                                    'lt': lt_date
                                }
                            }
                        }]
                    }
                }
            }

        # create list of today and yesterday index
        wildcard = '*'
        index_ = f'{INDEX_BASENAME.format(wildcard)}*'

        try:
            self.conn.update_by_query(query, index_)
        except Exception as err:
            LOGGER.warning(f'Failed to update ES index: {err}')

        return True

    def load_data(self, filepath):
        """
        loads data from event to target

        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)

        LOGGER.debug(f'Received file {self.filepath}')

        # generate geojson features
        package = self.generate_geojson_features()
        try:
            r = self.conn.submit_elastic_package(package)
            LOGGER.debug(f'Result: {r}')
            if self.newer:
                self.update_latest_status(self.storm_name, self.file_id)
                msg = f'Update active and latest status: {self.storm_name} {self.file_id}' # noqa
                LOGGER.debug(msg)
            return True
        except Exception as err:
            LOGGER.warning(f'Error indexing: {err}')
            return False


@click.group()
def hurricanes():
    """Manages hurricanes index"""
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
    """add data to system"""

    if [file_, directory] == [None, None]:
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [f for f in files if f.endswith('.json')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = HurricanesRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)

        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help=f'Delete indexes older than n days (default={DAYS_TO_KEEP})'
)
@cli_options.OPTION_DATASET(
    help='Hurricane dataset indexes to delete.',
    type=click.Choice(TEMPLATE_MAPPINGS + ['all'])
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, es, username, password, dataset, days, ignore_certs):
    """Delete expired hurricane documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        wildcard = '*'
        indexes_to_fetch = f'{INDEX_BASENAME.format(wildcard)}*'
    else:
        indexes_to_fetch = f'{INDEX_BASENAME.format(dataset)}*'

    indexes = conn.get(indexes_to_fetch)

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo(f'Deleting indexes {indexes_to_delete}')
            conn.delete(','.join(indexes_to_delete))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_HOURS(
    default=ACTIVE_HOURS,
    help=f'Change activate status older than n hours (default={ACTIVE_HOURS})'
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def update_active_status(ctx, es, username, password, hours, ignore_certs):
    """Set inactive hurricane documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = HurricanesRealtimeLoader(conn_config)
    result = loader.update_active_status(hours)

    if result:
        click.echo('active status updated')


@click.command()
@click.pass_context
@cli_options.OPTION_DATASET(
    help='Hurricane dataset indexes to delete.',
    type=click.Choice(TEMPLATE_MAPPINGS + ['all'])
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
def delete_indexes(ctx, dataset, es, username, password, ignore_certs,
                   index_template):
    """Delete all hurricane realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    if dataset == 'all':
        wildcard = '*'
        indexes = f'{INDEX_BASENAME.format(wildcard)}*'
    else:
        indexes = f'{INDEX_BASENAME.format(dataset)}*'

    click.echo(f'Deleting indexes {indexes}')

    conn.delete(indexes)

    if index_template:
        for type_ in TEMPLATE_MAPPINGS:
            index_name = INDEX_BASENAME.format(type_)
            click.echo(f'Deleting index template {index_name}')
            conn.delete_template(index_name)

    click.echo('Done')


hurricanes.add_command(add)
hurricanes.add_command(update_active_status)
hurricanes.add_command(clean_indexes)
hurricanes.add_command(delete_indexes)
