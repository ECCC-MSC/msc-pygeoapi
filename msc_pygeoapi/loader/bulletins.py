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

from msc_pygeoapi.env import (MSC_PYGEOAPI_ES_TIMEOUT, MSC_PYGEOAPI_ES_URL,
                              MSC_PYGEOAPI_ES_AUTH)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import click_abort_if_false, get_es


LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# index settings
INDEX_NAME = 'bulletins'

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
                    'datetime': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd HH:mm'
                    }
                }
            }
        }
    }
}


class BulletinsRealtimeLoader(BaseLoader):
    """Bulletins real-time loader"""

    def __init__(self, filepath):
        """initializer"""

        BaseLoader.__init__(self)

        self.DD_URL = 'https://dd.weather.gc.ca/bulletins/alphanumeric'
        self.ES = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

        if not self.ES.indices.exists(INDEX_NAME):
            self.ES.indices.create(index=INDEX_NAME, body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        LOGGER.debug(filepath)

        data = self.bulletin2dict(filepath)

        try:
            r = self.ES.index(index=INDEX_NAME, id=data['ID'], body=data)
            LOGGER.debug('Result: {}'.format(r))
            return True
        except Exception as err:
            LOGGER.warning('Error indexing: {}'.format(err))
            return False

    def bulletin2dict(self, filepath):
        """
        convert a bulletin into a GeoJSON object

        :param filepath: path to filename

        :returns: `dict` of GeoJSON
        """

        dict_ = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [-75, 45]
            },
            'properties': {}
        }

        try:
            bulletin_path = filepath.split('/alphanumeric/')[1]
        except IndexError as err:
            LOGGER.warning('no bulletin path: {}'.format(err))
            raise RuntimeError(err)

        identifier = bulletin_path.replace('/', '.')
        issuer_name = None
        issuer_country = None

        dict_['ID'] = dict_['properties']['identifier'] = identifier

        tokens = bulletin_path.split('/')

        yyyymmdd = tokens[0]
        hh = tokens[3]
        filename = tokens[-1]

        yyyy = yyyymmdd[0:4]
        mm = yyyymmdd[4:6]
        dd = yyyymmdd[6:8]

        min_ = filename.split('_')[2][-2:]

        datetime = '{}-{}-{} {}:{}'.format(yyyy, mm, dd, hh, min_)

        dict_['geometry'] = {
            'type': 'Point',
            'coordinates': [-75, 45]  # TODO: use real coordinates
        }

        dict_['properties']['datetime'] = datetime
        dict_['properties']['type'] = tokens[1]
        dict_['properties']['issuer_code'] = tokens[2]
        dict_['properties']['issuer_name'] = issuer_name
        dict_['properties']['issuer_country'] = issuer_country
        dict_['properties']['issuing_office'] = tokens[2][2:]
        dict_['properties']['url'] = '{}/{}'.format(self.DD_URL, bulletin_path)

        return dict_


@click.group()
def bulletins():
    """Manages bulletins index"""
    pass


@click.command()
@click.pass_context
@click.option('--days', '-d', default=DAYS_TO_KEEP, type=int,
              help='delete documents older than n days (default={})'.format(
                  DAYS_TO_KEEP))
@click.option('--yes', is_flag=True, callback=click_abort_if_false,
              expose_value=False,
              prompt='Are you sure you want to delete old documents?')
def clean_records(ctx, days):
    """Delete old documents"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    older_than = (datetime.now() - timedelta(days=days)).strftime(
        '%Y-%m-%d %H:%M')
    click.echo('Deleting documents older than {} ({} days)'.format(
        older_than, days))

    query = {
        'query': {
            'range': {
                'properties.datetime': {
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
              prompt='Are you sure you want to delete this index?')
def delete_index(ctx):
    """Delete bulletins index"""

    es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)

    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)


bulletins.add_command(clean_records)
bulletins.add_command(delete_index)
