# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#           <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2022 Louis-Philippe Rousseau-Lambert
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
import json
import logging
import os
from pathlib import Path

import click
from elasticsearch import logger as elastic_logger

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_CACHEDIR,
    MSC_PYGEOAPI_LOGGING_LOGLEVEL
)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    check_es_indexes_to_delete,
    configure_es_connection
)

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

DATETIME_FORMAT = '%Y%m%dT%H%M%S.%fZ'

# cleanup settings
DAYS_TO_KEEP = 7

INDEX_BASENAME = 'metnotes.'

MAPPINGS = {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'properties': {
                'properties': {
                    'id': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'aors': {
                        'type': 'keyword',
                        'index': 'true'
                    },
                    'type_id': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'publication_version': {
                        'type': 'integer',
                    },
                    'start_datetime': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss.SSS'Z'"
                    },
                    'end_datetime': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss.SSS'Z'"
                    },
                    'expiration_datetime': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss.SSS'Z'"
                    },
                    'publication_datetime': {
                        'type': 'date',
                        'format': "YYYY-MM-DD'T'HH:mm:ss.SSS'Z'"
                    },
                    'metnote_status': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'filename': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'weather_narrative_id': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'weather_narrative_version': {
                        'type': 'integer',
                    },
                    'content_en': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'content_fr': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    }
                }
            }
        }
    }

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': ['{}*'.format(INDEX_BASENAME)],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}


class MetNotesRealtimeLoader(BaseLoader):
    """MetNotes real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.config_file = None
        self.filename = None
        self.latest_file = None

        SETTINGS['mappings'] = MAPPINGS
        self.conn.create_template(INDEX_BASENAME, SETTINGS)

    def load_data(self, filepath):
        """
        fonction from base to load the data in ES

        :param filepath: filepath for parsing the current condition file
        
        :returns: True/False
        """

        self.config_file = (
            Path(MSC_PYGEOAPI_CACHEDIR)
            / 'metnotes-latest-file.json'
        )

        self.filename = os.path.basename(filepath)

        with open(filepath) as f:
            data = json.load(f)['features']

        if len(data) == 0:
            LOGGER.debug('No active MetNotes')
        else:
            for feature in data:

                b_dt = datetime.strptime(feature['properties']['publication_datetime'],
                                        '%Y-%m-%dT%H:%M:%S.%fZ')
                b_dt2 = b_dt.strftime('%Y-%m-%d')
                es_index = '{}{}'.format(INDEX_BASENAME, b_dt2)

                id = f"{feature['id']}_{feature['properties']['publication_version']}"

                feature['properties']['id'] = feature['id']
                feature['properties']['metnote_status'] = 'inactive'
                feature['properties']['filename'] = self.filename

                try:
                    self.update_es_index(es_index, id, feature)
                except Exception as err:
                    LOGGER.warning('Error indexing: {}'.format(err))
                    return False


        self.update_temporal_config()
        # update metnote status to active
        self.set_active_metnote()

        return True

    def update_es_index(self, es_index, id, feature):
        """
        Create/update the temporal configuration file
        for the metnotes

        :param es_index: ES index name
        :param id: id to use for feature
        :param feature: feature to add to ES

        returns: `bool` of update status
        """

        try:
            r = self.conn.Elasticsearch.index(
                index=es_index, id=id, body=feature,
                refresh=True
            )
            LOGGER.debug('Result: {}'.format(r))
            return True
        except Exception as err:
            LOGGER.warning('Error indexing: {}'.format(err))
            return False

    def update_temporal_config(self):
        """
        Create/update the temporal configuration file
        for the metnotes

        returns: `bool` of update status
        """
        updated_config = None

        latest_time = self.filename.split('_')[0]
        latest_file_time = datetime.strptime(latest_time, 
            DATETIME_FORMAT)

        if self.config_file.exists():
            with self.config_file.open() as f:
                config = json.load(f)
                # get previous default time for precip type
                previous_default_datetime = datetime.strptime(
                    config['latest_file_time'], DATETIME_FORMAT,
                )
                # update temporal config if new default time is
                # later than previous
                if latest_file_time > previous_default_datetime:
                    # update config
                    updated_config = {
                        'latest_file': self.filename,
                        'latest_file_time': latest_time
                    }
                    self.latest_file = self.filename
                else:
                    self.latest_file = config['latest_file']

        # temporal config file does not exist
        else:
            # and create config
            updated_config = updated_config = {
                                'latest_file': self.filename,
                                'latest_file_time': latest_time
                             }
            self.latest_file = self.filename

        if not updated_config:
            return False

        LOGGER.info(
            'Updating metnotes temporal configuration file...'
        )
        with self.config_file.open('w') as f:
            json.dump(updated_config, f)

        return True

    def set_active_metnote(self):
        """
        Update the metote status

        returns: `bool` of update status
        """

        query = {"script": {
            "source": f"if(ctx._source.properties.filename == '{self.latest_file}')"
                "{ctx._source.properties.metnote_status = 'active'}"
                "else{ctx._source.properties.metnote_status = 'inactive'}",
            "lang": "painless"
            }}
        try:
            self.conn.update_by_query(query, f'{INDEX_BASENAME}*')
        except Exception as err:
            LOGGER.warning('{}: failed to update ES index'.format(err))

        return True


@click.group()
def metnotes():
    """Manages MetNotes index"""
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

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    loader = MetNotesRealtimeLoader(conn_config)
    result = loader.load_data(file_)

    if not result:
        click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP,
    help='Delete indexes older than n days (default={})'.format(DAYS_TO_KEEP)
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete old indexes?'
)
def clean_indexes(ctx, days, es, username, password, ignore_certs):
    """Clean MetNotes indexes older than n number of days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get('{}*'.format(INDEX_BASENAME))

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo('Deleting indexes {}'.format(indexes_to_delete))
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
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs, index_template):
    """Delete MetNotes index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    all_indexes = '{}*'.format(INDEX_BASENAME)

    click.echo('Deleting indexes {}'.format(all_indexes))
    conn.delete(all_indexes)

    if index_template:
        click.echo('Deleting index template {}'.format(INDEX_BASENAME))
        conn.delete_template(INDEX_BASENAME)

    click.echo('Done')


metnotes.add_command(add)
metnotes.add_command(clean_indexes)
metnotes.add_command(delete_index)
