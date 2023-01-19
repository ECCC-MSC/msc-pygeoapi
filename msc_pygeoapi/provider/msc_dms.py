# =================================================================
#
# Authors: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2023 Etienne Pelletier
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
'''
# sample msc-pygeoapi provider configuration for DMS Core API
providers:
    - type: feature
    default: true
    name: msc_pygeoapi.provider.msc_dms.MSCDMSCoreAPIProvider
    data: http://localhost/dms-api/dms_data+msc+observation+atmospheric+surface_weather+ca-1.1-ascii # noqa
    id_field: identifier
'''
from collections import OrderedDict
import json
import logging
from urllib import parse
import requests

from pygeoapi.provider.base import (
    BaseProvider,
    ProviderConnectionError,
    ProviderQueryError,
    ProviderItemNotFoundError
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class MSCDMSCoreAPIProvider(BaseProvider):
    """DMS Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.dms.DMSProvider
        """

        super().__init__(provider_def)

        LOGGER.debug(f'data: {self.data}')

        self.select_properties = []

        self.session = requests.Session()

        # parse url and retrieve alias and base dms core api url
        self.parsed_url = parse.urlparse(self.data)
        self.path, self.alias = self.parsed_url.path.rsplit('/', 1)
        self.dms_host = (
            f'{self.parsed_url.scheme}://{self.parsed_url.netloc}{self.path}'
        )

        if not self.session.get(self.dms_host).status_code == 200:
            msg = f'Cannot connect to DMS Core API via {self.dms_host}'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        LOGGER.debug('Grabbing field information')
        self.fields = self.get_fields()

    def get_fields(self):
        """
         Get provider field information (names, types)

        :returns: dict of fields
        """

        fields_ = {}

        try:
            sa = self.session.get(
                f'{self.dms_host}/search/v2.0/{self.alias}/templateSearch',  # noqa
                params={'size': 1}
            ).json()
        except json.JSONDecodeError as e:
            raise ProviderQueryError(
                f'Could not decode JSON from query response: {e}'
            )

        try:
            properties = sa['hits']['hits'][0]['_source']
        except IndexError:
            LOGGER.debug(
                'No items in DMS Core alias. Could not retrieve fields.'
            )
            return fields_

        for k, v in properties.items():
            if isinstance(v, str):
                type_ = 'string'
            elif isinstance(v, int):
                type_ = 'integer'
            elif isinstance(v, float):
                type_ = 'float'
            elif isinstance(v, list):
                type_ = 'nested'
            elif isinstance(v, dict):
                type_ = 'object'

            fields_[k] = {'type': type_}
            fields_ = dict(sorted(fields_.items()))

        return fields_

    def query(
        self,
        offset=0,
        limit=10,
        resulttype='results',
        # bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        # skip_geometry=False,
        # q=None,
        **kwargs,
    ):
        """
        query DMS index

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of 0..n GeoJSON features
        """

        self.select_properties = select_properties

        if resulttype == 'hits':
            LOGGER.debug('hits only specified')
            limit = 0

        params = {
            'trackTotalHits': True,
            'startIndex': offset,
            'size': limit,
        }

        feature_collection = {'type': 'FeatureCollection', 'features': []}

        if datetime_ is not None:
            LOGGER.debug('processing datetime parameter')
            if self.time_field is None:
                LOGGER.error('time_field not enabled for collection')
                raise ProviderQueryError()

            params['datetimeType'] = self.time_field

            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')

                params['from'] = time_begin
                params['to'] = time_end

            else:  # time instant
                params['from'] = datetime_
                params['to'] = datetime_

        if properties:
            LOGGER.debug('processing properties')
            params['query'] = ' AND '.join(
                ['{}:"{}"'.format(*p) for p in properties]
            )

        if sortby:
            LOGGER.debug('processing sortby')
            sort_by_values = []
            for sort in sortby:
                LOGGER.debug('processing sort object: {}'.format(sort))
                sort_by_values.append(f'{sort["order"]}{sort["property"]}')

            params['sortFields'] = ','.join(sort_by_values)

        try:
            LOGGER.debug('querying DMS Core API')
            results = self.session.get(
                f'{self.dms_host}/search/v2.0/{self.alias}/templateSearch',  # noqa
                params=params
            ).json()
            results['hits']['total'] = results['hits']['total']['value']
        except Exception as e:
            raise e

        feature_collection['numberMatched'] = results['hits']['total']

        feature_collection['numberReturned'] = len(results['hits']['hits'])

        LOGGER.debug('serializing features')
        for feature in results['hits']['hits']:
            feature_ = self.dmsdoc2geojson(feature)
            feature_collection['features'].append(feature_)

        return feature_collection

    def get(self, identifier, **kwargs):
        """
        Get ES document by id

        :param identifier: feature id

        :returns: dict of single GeoJSON feature
        """

        LOGGER.debug('Fetching identifier {}'.format(identifier))

        try:
            result = self.session.get(
                f'{self.dms_host}/search/v2.0/{self.alias}/templateSearch',  # noqa
                params={'query': f'_id:"{identifier}"'}
            ).json()
        except json.JSONDecodeError as e:
            raise ProviderQueryError(
                f'Could not decode JSON from query response: {e}'
            )

        try:
            LOGGER.debug('Serializing feature')
            feature_ = self.dmsdoc2geojson(result['hits']['hits'][0])
        except IndexError as e:
            raise ProviderItemNotFoundError(e)

        return feature_

    def dmsdoc2geojson(self, doc):
        """
        generate GeoJSON `dict` from DMS document

        :param doc: `dict` of DMS document

        :returns: GeoJSON `dict`
        """

        feature_ = {}
        feature_thinned = {}

        if 'properties' not in doc['_source']:
            LOGGER.debug('Looks like a GDAL ES 7 document')
            id_ = doc['_id']
            if 'type' not in doc['_source']:
                feature_['id'] = id_
                feature_['type'] = 'Feature'
            feature_['geometry'] = doc['_source'].get('location')
            feature_['properties'] = {}
            for key, value in doc['_source'].items():
                if key == 'location':
                    continue
                feature_['properties'][key] = value
        else:
            LOGGER.debug('Looks like true GeoJSON document')
            feature_ = doc['_source']
            id_ = doc['_source']['properties'][self.id_field]
            feature_['id'] = id_
            feature_['geometry'] = doc['_source'].get('location')

        if self.properties or self.select_properties:
            LOGGER.debug('Filtering properties')
            all_properties = self.get_properties()

            feature_thinned = {
                'id': id_,
                'type': feature_['type'],
                'geometry': doc['_source'].get('location'),
                'properties': OrderedDict(),
            }
            for p in all_properties:
                try:
                    feature_thinned['properties'][p] = feature_['properties'][
                        p
                    ]  # noqa
                except KeyError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError()

        feature_['geometry']['type'] = feature_['geometry'][
            'type'
        ].capitalize()
        if feature_thinned:
            return feature_thinned
        else:
            return feature_

    def get_properties(self):
        all_properties = []

        LOGGER.debug('configured properties: {}'.format(self.properties))
        LOGGER.debug('selected properties: {}'.format(self.select_properties))

        if not self.properties and not self.select_properties:
            all_properties = self.get_fields()
        if self.properties and self.select_properties:
            all_properties = set(self.properties) & set(self.select_properties)
        else:
            all_properties = set(self.properties) | set(self.select_properties)

        LOGGER.debug('resulting properties: {}'.format(all_properties))
        return all_properties

    def __repr__(self):
        return '<MSCDMSCoreAPIProvider> {}'.format(self.data)
