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
from datetime import datetime, timedelta
import logging

from msc_pygeoapi.env import MSC_PYGEOAPI_ES_TIMEOUT, MSC_PYGEOAPI_ES_URL
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import click_abort_if_false, get_es


LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# index settings
INDEX_NAME = 'hydrometric_realtime'
INDEX_NAME_STATIONS = 'hydrometric_realtime_stations'

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
                    'DATETIME': {
                        'type': 'date',
                        'format': 'strict_date_hour_minute_second'
                    }
                }
            }
        }
    }
}


class HydrometricRealtimeLoader(BaseLoader):
    """Hydrometric Real-time loader"""

    def __init__(self, filepath):
        """initializer"""

        BaseLoader.__init__(self)

        self.ES = get_es(MSC_PYGEOAPI_ES_URL)

        if not self.ES.indices.exists(INDEX_NAME):
            self.ES.indices.create(index=INDEX_NAME, body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)
        if not self.ES.indices.exists(INDEX_NAME_STATIONS):
            self.ES.indices.create(index=INDEX_NAME_STATIONS, body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        LOGGER.info(filepath)


@click.group()
def hydrometric_realtime():
    """Manages hydrometric realtime index"""
    pass


@click.command()
@click.pass_context
@click.option('--yes', is_flag=True, callback=click_abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to delete old documents?')
def clean_records(ctx):
    """Delete old documents"""

    es = get_es(MSC_PYGEOAPI_ES_URL)

    older_than = (datetime.now() - timedelta(days=DAYS_TO_KEEP)).strftime(
        '%Y-%m-%d %H:%M')
    click.echo('Deleting documents older than {} ({} days)'.format(
        older_than, DAYS_TO_KEEP))

    query = {
        'query': {
            'range': {
                'properties.DATETIME': {
                    'lte': older_than
                }
            }
        }
    }

    es.delete_by_query(index=INDEX_NAME, body=query)


@click.command()
@click.pass_context
@click.option('--yes', is_flag=True, callback=click_abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to delete these indexes?')
def delete_index(ctx):
    """Delete hydrometric realtime indexes"""

    es = get_es(MSC_PYGEOAPI_ES_URL)

    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)
    if es.indices.exists(INDEX_NAME_STATIONS):
        es.indices.delete(INDEX_NAME_STATIONS)


hydrometric_realtime.add_command(clean_records)
hydrometric_realtime.add_command(delete_index)
