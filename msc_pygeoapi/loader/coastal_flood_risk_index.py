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

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import re

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'coastal_flood_risk_index'

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

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
                }
            }
        }
    }
}

SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0}
}


class CoastalFloodRiskIndexLoader(BaseLoader):
    """Coastal flodd risk index loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.filepath = None
        self.datetime = None
        self.conn = ElasticsearchConnector(conn_config)

        SETTINGS['mappings'] = MAPPINGS
        if not self.conn.exists(INDEX_NAME):
            LOGGER.debug(f'Creating index {INDEX_NAME}')
            self.conn.create(INDEX_NAME, SETTINGS)

    def generate_geojson_features(self):
        """
        Generates and yields a series of flod risk index.
        They are returned as Elasticsearch bulk API upsert actions,
        with documents in GeoJSON to match the Elasticsearch index mappings.

        :returns: Generator of Elasticsearch actions to upsert the
                  flod risk index
        """

        with open(self.filepath.resolve()) as f:
            data = json.load(f)['features']

        features = []
        filename = f.name.split('/')[-1]
        file_id = re.sub(r'_v\d+\.json', '', filename)

        if len(data) > 0:
            for feature in data:

                # flatten metobject properties
                metobj = feature['properties']['metobject']
                for item, values in metobj.items():
                    try:
                        metobj_flat_item = self.flatten_json(item,
                                                             values,
                                                             'metobject')
                        feature['properties'].update(metobj_flat_item)
                        feature['properties']['file_id'] = file_id
                    except Exception as err:
                        msg = f'Error while flattening flood index JSON {err}'
                        LOGGER.error(f'{msg}')
                        pass

                del feature['properties']['metobject']

                try:
                    f_exp_time = feature['properties']['expiration_datetime']
                    exp_time = datetime.strptime(f_exp_time,
                                                 DATETIME_FORMAT)
                except KeyError:
                    validity = feature['properties']['validity_datetime']
                    validity_datetime = datetime.strptime(validity,
                                                          DATETIME_FORMAT)
                    exp_time = validity_datetime + timedelta(hours=24)

                expiration = datetime.strftime(exp_time, DATETIME_FORMAT)
                feature['properties']['expiration_datetime'] = expiration

                if exp_time > datetime.now():
                    features.append(feature)

            # check if id is already in ES and if amendment is +=1
            amendment = features[0]['properties']['amendment']
            is_newer = self.check_if_newer(file_id, amendment)

            if is_newer['update']:
                for outlook in features:
                    action = {
                        '_id': outlook['properties']['id'],
                        '_index': INDEX_NAME,
                        '_op_type': 'update',
                        'doc': outlook,
                        'doc_as_upsert': True
                    }

                    yield action

                for id_ in is_newer['id_list']:
                    self.conn.Elasticsearch.delete(index=INDEX_NAME,
                                                   id=id_)
        else:
            LOGGER.warning(f'empty flood risk index json in {filename}')

            version = re.search(r'v(\d+)\.json$', filename).group(1)
            if int(version) > 1:
                # we need to delete the associated outlooks
                query = {
                    "query": {
                        "match": {
                            "properties.file_id": file_id
                        }
                    }
                }
                self.conn.Elasticsearch.delete_by_query(index=INDEX_NAME,
                                                        body=query)

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
        check if the coastal flood risk index is the newest version

        :returns: `bool` if latest, id_list for ids to delete
        """

        upt_ = True
        id_list = []

        query = {
            "query": {
                "match": {
                    "properties.file_id": file_id
                }
            }
        }

        # Fetch the document
        try:
            result = self.conn.Elasticsearch.search(index=INDEX_NAME,
                                                    body=query)
            if result:
                hit = result['hits']['hits'][0]
                es_amendement = hit["_source"]['properties']['amendment']
                if es_amendement >= amendment:
                    upt_ = False
                else:
                    for id_ in result['hits']['hits']:
                        id_list.append(id_['_id'])
        except Exception:
            LOGGER.warning(f'Item ({file_id}) does not exist in index')

        return {'update': upt_, 'id_list': id_list}

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
            return True
        except Exception as err:
            LOGGER.warning(f'Error indexing: {err}')
            return False


@click.group()
def coastal_flood_risk_index():
    """Manages coastal flood risk index outlook index"""
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
        loader = CoastalFloodRiskIndexLoader(conn_config)
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
def clean_index(ctx, es, username, password, ignore_certs):
    """Delete expired outlook documents"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    click.echo('Deleting documents older than datetime.now()')
    now = datetime.now().strftime(DATETIME_FORMAT)

    query = {
        'query': {
            'range': {
                'properties.expiration_datetime': {
                    'lte': now
                }
            }
        }
    }

    conn.Elasticsearch.delete_by_query(index=INDEX_NAME, body=query)


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


coastal_flood_risk_index.add_command(add)
coastal_flood_risk_index.add_command(clean_index)
coastal_flood_risk_index.add_command(delete_index)
