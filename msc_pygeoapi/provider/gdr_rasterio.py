# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
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

from copy import deepcopy
import logging

from elasticsearch import Elasticsearch

from pygeoapi.provider.base import (
    ProviderConnectionError, ProviderInvalidQueryError)
from pygeoapi.provider.rasterio_ import RasterioProvider

LOGGER = logging.getLogger(__name__)


class GDRRasterioIndexProvider(RasterioProvider):
    """Rasterio Index Provider"""

    def __init__(self, provider_def):

        url_tokens = provider_def['data'].split('/')
        print("URL_", provider_def['data'])

        LOGGER.debug('Setting Elasticsearch properties')
        self.es_host = url_tokens[2]
        self.index_name = url_tokens[-1]

        LOGGER.debug('host: {}'.format(self.es_host))
        LOGGER.debug('index: {}'.format(self.index_name))

        LOGGER.debug('Connecting to Elasticsearch')
        self.es = Elasticsearch(self.es_host)
        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        query_body = {
            'query': {
                'match': {
                    'properties.layer': 'HRDPS.CONTINENTAL_TT'
                }
            },
            'sort': [{
                'properties.reference_datetime': {
                    'order': 'desc'
                }
            }]
        }

        result = self.es.search(index='geomet-data-registry', size=1,
                                body=query_body)

        provider_def_ = deepcopy(provider_def)
        provider_def_['data'] = (result['hits']['hits'][0]['_source']
                                 ['properties']['filepath'])

        LOGGER.debug('Default coverage: {}'.format(provider_def_['data']))

        super().__init__(provider_def_)

    def query(self, range_subset=[], subsets={}, datetime=None,
              bbox=[], format_='json'):
        """
        Extract data from collection collection
        :param range_subset: list of bands
        :param subsets: dict of subset names with lists of ranges
        :param format_: data format of output

        :returns: coverage data as dict of CoverageJSON or native format
        """

        query_body = {
            'query': {
                'bool': {
                    'filter': [{
                        'match': {
                            'properties.layer': 'HRDPS.CONTINENTAL_TT'
                        }
                    }]
                 }
            },
            'sort': [{
                'properties.reference_datetime': {
                    'order': 'desc'
                }
            }]
        }

        if datetime is not None:
            query_body['query']['bool']['filter'].append({
                'match': {
                    'properties.forecast_hour_datetime': datetime
                }
            })

        result = self.es.search(index='geomet-data-registry', size=1,
                                body=query_body)

        if result['hits']['total']['value'] == 0:
            msg = 'no matching coverage for datetime'
            LOGGER.debug(msg)
            raise ProviderInvalidQueryError(msg)

        self.data = (result['hits']['hits'][0]['_source']['properties']
                     ['filepath'])

        return super().query(range_subset=range_subset, subsets=subsets,
                             bbox=bbox, format_=format_)

    def __repr__(self):
        return '<GDRRasterioIndexProvider> {}'.format(self.data)
