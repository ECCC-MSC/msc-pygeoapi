# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2024 Louis-Philippe Rousseau-Lambert
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re

import click
from elasticsearch import helpers

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import GEOMET_LOCAL_BASEPATH
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'thunderstorm_outlook'

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# local GeoJSON copy served by GeoMet-Weather
LOCAL_FILEPATH = (
    Path(GEOMET_LOCAL_BASEPATH)
    / 'thunderstorm-outlooks'
    / 'active-thunderstorm-outlooks.json'
    if GEOMET_LOCAL_BASEPATH
    else None
)

MAPPINGS = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'publication_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'expiration_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'validity_datetime': {
                    'type': 'date',
                    'format': 'strict_date_time_no_millis'
                },
                'product_type': {'type': 'keyword'},
                'product_sub_type': {'type': 'keyword'},
                'file_id': {'type': 'keyword'},
                'amendment': {'type': 'integer'},
                'version': {'type': 'integer'}
            }
        }
    }
}

SETTINGS = {'settings': {'number_of_shards': 1, 'number_of_replicas': 0}}


class ThunderstormOutlookLoader(BaseLoader):
    """Thunder storm outlook loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.filepath = None
        self.datetime = None
        self.conn = ElasticsearchConnector(conn_config)
        self.loaded_panel = None

        SETTINGS['mappings'] = MAPPINGS
        if not self.conn.exists(INDEX_NAME):
            LOGGER.debug(f'Creating index {INDEX_NAME}')
            self.conn.create(INDEX_NAME, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of thunderstorm outlook.
        They are returned as Elasticsearch bulk API upsert actions,
        with documents in GeoJSON to match the Elasticsearch index mappings.

        :returns: Generator of Elasticsearch actions to upsert the
                  thunderstorm outlook
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)['features']

        features = []
        filename = self.filepath.name
        file_id = re.sub(r'_v\d+\.json', '', filename)

        match = re.search(r'_v(\d+)\.json$', filename)
        if match is None:
            LOGGER.warning(f'{filename} carries no version, assuming 1')
            version = 1
        else:
            version = int(match.group(1))

        if not data:
            LOGGER.warning(f'empty thunderstorm outlook json in {filename}')

            if version > 1:
                # forecast can emit empty outlooks which redacts
                # previous versions, the query below will remove
                # previous issued outlooks for the same issuance
                # and panel
                query = {
                    'query': {
                        'bool': {
                            'filter': [
                                {'term': {'properties.file_id': file_id}}
                            ],
                            'must_not': [
                                {
                                    'range': {
                                        'properties.version': {'gte': version}
                                    }
                                }
                            ]
                        }
                    }
                }
                self.conn.Elasticsearch.delete_by_query(
                    index=INDEX_NAME, body=query, conflicts='proceed'
                )
            return

        for feature in data:

            # flatten metobject properties
            metobj = feature['properties']['metobject']
            for item, values in metobj.items():
                try:
                    metobj_flat_item = self.flatten_json(
                        item, values, 'metobject'
                    )
                    feature['properties'].update(metobj_flat_item)
                except Exception as err:
                    msg = f'Error while flattening Thunderstorm JSON {err}'
                    LOGGER.error(f'{msg}')

            feature['properties']['file_id'] = file_id
            feature['properties']['version'] = version

            del feature['properties']['metobject']
            f_exp_datetime = feature['properties']['expiration_datetime']
            exp_datetime = datetime.strptime(
                f_exp_datetime, DATETIME_FORMAT
            ).replace(tzinfo=timezone.utc)

            if exp_datetime > datetime.now(timezone.utc):
                features.append(feature)

        if not features:
            LOGGER.warning(f'all outlooks expired in {filename}')
            return

        properties = features[0]['properties']
        amendment = int(properties.get('amendment') or 0)

        # check if outlook is already superseded
        # this can happen when files come in out of order
        # or while manually replaying file ingestion
        if self.is_superseded(file_id, amendment):
            LOGGER.warning(f'{filename} is already superseded, skipping')
            return

        for outlook in features:
            action = {
                '_id': outlook['properties']['id'],
                '_index': INDEX_NAME,
                '_op_type': 'update',
                'doc': outlook,
                'doc_as_upsert': True
            }

            yield action

        self.loaded_panel = (file_id, amendment)

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

    def panel_filter(self, file_id, amendment, operator):
        """
        Elasticsearch filter clauses selecting the other amendments of one
        forecast panel.

        Every issuance stays indexed so the collection can be queried for
        them; only the amendments of a single panel are ordered here.

        :param operator: 'gt' for the amendments that outrank this one,
                         'lt' for the ones it outranks
        :returns: `list` of filter clauses
        """

        return [
            {'term': {'properties.file_id': file_id}},
            {'range': {'properties.amendment': {operator: amendment}}}
        ]

    def is_superseded(self, file_id, amendment):
        """
        Checks whether a later amendment of this forecast panel is already
        indexed, which happens when files arrive out of order or are
        replayed

        :returns: `bool` True if this panel is already superseded
        """

        query = {
            'query': {
                'bool': {'filter': self.panel_filter(file_id, amendment, 'gt')}
            }
        }

        try:
            result = self.conn.Elasticsearch.count(
                index=INDEX_NAME, body=query
            )
            return result['count'] > 0
        except Exception as err:
            LOGGER.warning(f'Failed to check for newer amendments: {err}')
            return False

    def delete_superseded(self, file_id, amendment):
        """
        Drops the earlier amendments of the provided forecast panel.

        :returns: `bool` of status result
        """

        query = {
            'query': {
                'bool': {'filter': self.panel_filter(file_id, amendment, 'lt')}
            }
        }

        try:
            self.conn.Elasticsearch.delete_by_query(
                index=INDEX_NAME, body=query, conflicts='proceed'
            )
            return True
        except Exception as err:
            LOGGER.warning(f'Failed to delete superseded amendments: {err}')
            return False

    def latest_per_centre(self, features):
        """
        Keep the latest issuance of each storm prediction centre, and
        within that issuance the latest amendment of each forecast panel.

        Since the index holds all the issuances, this functions filters out
        the superseded ones for each storm prediction centre and region.

        :returns: `list` of features
        """

        required = (
            'product_type',
            'product_sub_type',
            'publication_datetime',
            'file_id'
        )
        usable = []
        for feature in features:
            missing = [
                key for key in required if key not in feature['properties']
            ]
            if missing:
                LOGGER.warning(
                    f'leaving a feature missing {missing} out of '
                    f'the local copy'
                )
                continue
            usable.append(feature)

        newest = {}
        for feature in usable:
            properties = feature['properties']
            centre = (
                properties['product_type'],
                properties['product_sub_type'],
            )
            publication = properties['publication_datetime']
            if publication > newest.get(centre, ''):
                newest[centre] = publication

        panels = {}
        for feature in usable:
            properties = feature['properties']
            centre = (
                properties['product_type'],
                properties['product_sub_type'],
            )
            if properties['publication_datetime'] != newest[centre]:
                continue
            file_id = properties['file_id']
            amendment = int(properties.get('amendment') or 0)
            latest, kept = panels.get(file_id, (-1, []))
            if amendment > latest:
                panels[file_id] = (amendment, [feature])
            elif amendment == latest:
                kept.append(feature)

        return [f for _, kept in panels.values() for f in kept]

    def update_local_copy(self):
        """
        Writes a local copy of all active and latest thunderstorm outlooks
        for each centre. This is performed atomically to avoid
        potential conflicts during concurrent writes.

        :returns: `bool` of write status
        """

        if LOCAL_FILEPATH is None:
            LOGGER.warning(
                'GEOMET_LOCAL_BASEPATH not set, ' 'skipping local copy'
            )
            return False

        self.conn.Elasticsearch.indices.refresh(
            index=INDEX_NAME, ignore_unavailable=True
        )

        now = datetime.now(timezone.utc).strftime(DATETIME_FORMAT)
        query = {
            'query': {'range': {'properties.expiration_datetime': {'gt': now}}}
        }

        features = []

        try:
            for hit in helpers.scan(
                self.conn.Elasticsearch,
                index=INDEX_NAME,
                query=query,
                preserve_order=False
            ):
                features.append(hit['_source'])
        except Exception as err:
            LOGGER.warning(f'Failed to query ES for local copy: {err}')
            return False

        indexed = len(features)
        features = self.latest_per_centre(features)
        LOGGER.info(
            f'{len(features)} of {indexed} active outlooks kept for '
            f'the local copy'
        )

        feature_collection = {
            'type': 'FeatureCollection',
            'features': features
        }

        output_dir = LOCAL_FILEPATH.parent
        tmp_path = f'{LOCAL_FILEPATH}.{os.getpid()}.tmp'

        try:
            os.makedirs(output_dir, exist_ok=True)
            with open(tmp_path, 'w') as f:
                json.dump(feature_collection, f)
            os.replace(tmp_path, LOCAL_FILEPATH)
            LOGGER.debug(f'Local copy written to {LOCAL_FILEPATH}')
        except Exception as err:
            LOGGER.warning(
                f'Failed to write local copy to {LOCAL_FILEPATH}: {err}'
            )
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return False

        return True

    def load_data(self, filepath):
        """
        loads data from event to target

        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)
        self.loaded_panel = None

        LOGGER.debug(f'Received file {self.filepath}')

        # generate geojson features
        package = self.generate_geojson_features()
        try:
            r = self.conn.submit_elastic_package(package)
            LOGGER.debug(f'Result: {r}')
        except Exception as err:
            LOGGER.warning(f'Error indexing: {err}')
            return False

        # only retire the earlier amendments once this panel is indexed
        if self.loaded_panel is not None:
            self.delete_superseded(*self.loaded_panel)

        self.update_local_copy()

        return True


@click.group()
def thunderstorm_outlook():
    """Manages thunderstorm outlook index"""
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

    if file_ is None and directory is None:
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
        loader = ThunderstormOutlookLoader(conn_config)
        result = loader.load_data(file_to_process)

        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old outlooks?'
)
def clean_outlooks(ctx, es, username, password, ignore_certs):
    """Delete expired outlook documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    loader = ThunderstormOutlookLoader(conn_config)

    click.echo('Deleting documents older than datetime.now()')
    now = datetime.now(timezone.utc).strftime(DATETIME_FORMAT)

    query = {
        'query': {
            'range': {
                'properties.expiration_datetime': {
                    'lte': now
                }
            }
        }
    }

    loader.conn.Elasticsearch.delete_by_query(
        index=INDEX_NAME, body=query, conflicts='proceed'
    )

    loader.update_local_copy()


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def update_local_copy(ctx, es, username, password, ignore_certs):
    """Refresh the local GeoJSON copy served by GeoMet-Weather"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    loader = ThunderstormOutlookLoader(conn_config)

    if not loader.update_local_copy():
        raise click.ClickException('local copy not updated')

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_INDEX_TEMPLATE()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs, index_template):
    """Delete cumulative effects hotspots index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    click.echo(f'Deleting indexes {INDEX_NAME}')
    conn.delete(INDEX_NAME)

    if index_template:
        click.echo(f'Deleting index template {INDEX_NAME}')
        conn.delete_template(INDEX_NAME)

    click.echo('Done')


thunderstorm_outlook.add_command(add)
thunderstorm_outlook.add_command(clean_outlooks)
thunderstorm_outlook.add_command(update_local_copy)
thunderstorm_outlook.add_command(delete_index)
