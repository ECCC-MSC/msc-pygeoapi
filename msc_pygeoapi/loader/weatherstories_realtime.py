# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2025 Etienne Pelletier
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
import json

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import configure_es_connection

DAYS_TO_KEEP = 7
INDEX_BASENAME = 'weatherstories'
MAPPINGS = {
    "properties": {
        "geometry": {
            "properties": {
                "coordinates": {"type": "float"},
                "type": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                }
            }
        }
    }
}

SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': MAPPINGS
}


class WeatherStoriesRealtimeLoader(BaseLoader):
    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filepath = None

        SETTINGS['mappings'] = MAPPINGS
        self.conn.create(INDEX_BASENAME, SETTINGS)

    def load_data(self, filepath: str) -> bool:
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        with open(filepath) as fh:
            json_data = json.load(fh)

        features = json_data.get('features')

        for feature in features:
            properties = feature.get('properties')
            if 'subregion_name' in properties:
                id_ = f"{str(properties['province_territory_name_en'])}-{properties['subregion_name_en']}"  # noqa
            else:
                id_ = f"{str(properties['province_territory_name_en'])}"

            self.conn.Elasticsearch.index(
                index="weatherstories",
                id=id_,
                body=feature
            )

        return True


# CLI options to interact manually with Weather Stories file for testing
@click.group()
def weatherstories_realtime():
    """Manages Weather Stories index"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, es, username, password, ignore_certs):
    """add data to system"""

    if file_ is None:
        raise click.ClickException('Missing --file/-f')

    conn_config = configure_es_connection(
        es, username, password, ignore_certs
        )

    loader = WeatherStoriesRealtimeLoader(conn_config)
    result = loader.load_data(file_)

    if not result:
        click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete this index?')
def delete_index(ctx, es, username, password, ignore_certs):
    """Delete Weather Stories index"""

    conn_config = configure_es_connection(
        es, username, password, ignore_certs
    )
    conn = ElasticsearchConnector(conn_config)

    all_indexes = f'{INDEX_BASENAME}'

    click.echo(f'Deleting index {all_indexes}')
    conn.delete(all_indexes)

    click.echo('Done')


weatherstories_realtime.add_command(add)
weatherstories_realtime.add_command(delete_index)
