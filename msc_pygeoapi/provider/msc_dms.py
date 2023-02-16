# =================================================================
#
# Authors: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#          Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2023 Etienne Pelletier
# Copyright (c) 2023 Tom Kralidis
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

"""
# sample msc-pygeoapi provider configuration for DMS Core API
providers:
    - type: feature
    default: true
    name: msc_pygeoapi.provider.msc_dms.MSCDMSCoreAPIProvider
    data: http://localhost/dms-api/dms_data+msc+observation+atmospheric+surface_weather+ca-1.1-ascii # noqa
    id_field: id
    time_field: obs_date_tm
    _time_field_format: "%Y%m%d%H%M"
"""

from collections import OrderedDict
from datetime import datetime
import json
import logging
from urllib import parse
import requests

from pygeoapi.provider.base import (
    BaseProvider,
    ProviderConnectionError,
    ProviderQueryError,
    ProviderInvalidQueryError,
    ProviderItemNotFoundError
)
from pygeoapi.provider.base_edr import BaseEDRProvider

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

        self.time_field_format = provider_def.get('_time_field_format',
                                                  '%Y%m%d%H%M')
        self.geom_field = provider_def.get('geom_field', 'location')

        LOGGER.debug(f'data: {self.data}')

        self.select_properties = []

        self.session = requests.Session()

        # parse url and retrieve alias and base dms core api url
        self.parsed_url = parse.urlparse(self.data)
        self.path, self.alias = self.parsed_url.path.rsplit('/', 1)
        self.dms_host = (
            f'{self.parsed_url.scheme}://{self.parsed_url.netloc}'
        )

        if not self.session.head(self.dms_host).status_code == 200:
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

        params = {
            'datetimeType': f'properties.{self.time_field}',
            'size': 1
        }

        try:
            url = f'{self.data}/templateSearch'
            sa = self.session.get(url, params=params).json()
        except json.JSONDecodeError as e:
            msg = f'Could not decode JSON from query response: {e}'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        try:
            properties = sa['hits']['hits'][0]['_source']['properties']
        except IndexError:
            LOGGER.debug(
                'No items in DMS Core alias. Could not retrieve fields.'
            )
            return fields_

        for k, v in properties.items():
            if k == 'geometry':
                continue
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
        bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        # skip_geometry=False,
        # q=None,
        **kwargs,
    ):
        """
        query DMS API

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
        :param within: distance (for EDR radius queries)
        :param within_units: distance units (for EDR radius queries)

        :returns: dict of 0..n GeoJSON features
        """

        self.select_properties = select_properties

        if resulttype == 'hits':
            LOGGER.debug('hits only specified')
            limit = 0

        params = {
            'datetimeType': f'properties.{self.time_field}',
            'trackTotalHits': True,
            'startIndex': offset,
            'size': limit
        }

        feature_collection = {'type': 'FeatureCollection', 'features': []}

        if bbox:
            LOGGER.debug('processing bbox')
            params['locationField'] = self.geom_field
            if bbox[0] == bbox[2] and bbox[1] == bbox[3]:
                LOGGER.debug('Point-based geometry query detected')
                params['latitude'] = bbox[1]
                params['longitude'] = bbox[0]
            else:
                params['bbox'] = ','.join([str(b) for b in bbox])

        if None not in [kwargs.get('within'), kwargs.get('within_units')]:
            LOGGER.debug('Setting radius parameters')
            distance = f"{kwargs.get('within')}{kwargs.get('within_units')}"
            params['distance'] = distance

        if datetime_ is not None:
            LOGGER.debug('processing datetime parameter')
            if self.time_field is None:
                msg = 'time_field not enabled for collection'
                LOGGER.error(msg)
                raise ProviderQueryError(msg)

            params['datetimeType'] = f'properties.{self.time_field}'

            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')

                params['from'] = self._rfc3339_to_datetime_string(time_begin)
                params['to'] = self._rfc3339_to_datetime_string(time_end)

            else:  # time instant
                params['from'] = self._rfc3339_to_datetime_string(datetime_)
                params['to'] = self._rfc3339_to_datetime_string(datetime_)

        if properties:
            LOGGER.debug('processing properties')
            params['query'] = ' AND '.join(
                [f'properties.{p}:"{v}"' for p, v in properties]
            )

        if sortby:
            LOGGER.debug('processing sortby')
            sort_by_values = []
            for sort in sortby:
                # only allow sort on time_field
                if sort['property'] != self.time_field:
                    msg = f'Sorting only enabled for {self.time_field}'
                    raise ProviderQueryError(msg)
                LOGGER.debug(f'processing sort object: {sort}')
                sort_property = f'{sort["order"]}properties.{sort["property"]}'
                sort_by_values.append(sort_property)

            params['sortFields'] = ','.join(sort_by_values)

        try:
            LOGGER.debug(f'querying DMS Core API with: {params}')
            url = f'{self.dms_host}/search/v2.0/{self.alias}/templateSearch'
            results = self.session.get(url, params=params).json()
            results['hits']['total'] = results['hits']['total']['value']
        except Exception as e:
            msg = f'Query error: {e}'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        feature_collection['numberMatched'] = results['hits']['total']
        feature_collection['numberReturned'] = len(results['hits']['hits'])

        LOGGER.debug(f"Matched: {feature_collection['numberMatched']}"))
        LOGGER.debug(f"Returned: {feature_collection['numberReturned']}"))

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

        LOGGER.debug(f'Fetching identifier {identifier}')

        url = f'{self.dms_host}/search/v2.0/{self.alias}/templateSearch'

        params = {
            'datetimeType': f'properties.{self.time_field}',
            'query': f'properties.{self.id_field}="{identifier}"'
        }

        try:
            result = self.session.get(url, params=params).json()
        except json.JSONDecodeError as e:
            msg = f'Could not decode JSON from query response: {e}'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

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

        feature_ = doc['_source']
        feature_.pop('indexDateTime')

        if self.properties or self.select_properties:
            LOGGER.debug('Filtering properties')
            all_properties = self._get_properties()

            feature_thinned = {
                'id': doc['_source']['id'],
                'type': doc['_source']['type'],
                'geometry': doc['_source']['geometry'],
                'properties': OrderedDict(),
            }
            for p in all_properties:
                try:
                    feature_thinned['properties'][p] = feature_['properties'][
                        p
                    ]
                except KeyError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError(err)

        if feature_thinned:
            return feature_thinned
        else:
            return feature_

    def _get_properties(self):
        """
        Helper function to derive properties to return in a feature

        :returns: `list` of default/selected properties
        """

        all_properties = []

        LOGGER.debug(f'configured properties: {self.properties}')
        LOGGER.debug(f'selected properties: {self.select_properties}')

        if not self.properties and not self.select_properties:
            all_properties = self.get_fields()
        if self.properties and self.select_properties:
            all_properties = set(self.properties) & set(self.select_properties)
        else:
            all_properties = set(self.properties) | set(self.select_properties)

        LOGGER.debug(f'resulting properties: {all_properties}')
        return all_properties

    def _rfc3339_to_datetime_string(self, datetime_string):
        """
        Helper function which transforms RFC3339 datetime into custom
        formatted datetime string

        :param datetime_string: string of RFC3339 datetime
        :param datetime_format: time format to apply for formatting

        :returns: `str` of custom formatted datetime
        """

        try:
            value = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            raise ProviderInvalidQueryError(
                f'Invalid datetime parameter format: {e}'
            )

        return value.strftime(self.time_field_format)

    def __repr__(self):
        return f'<MSCDMSCoreAPIProvider> {self.data}'


class MSCDMSCoreAPIEDRProvider(BaseEDRProvider, MSCDMSCoreAPIProvider):
    def __init__(self, provider_def):

        BaseEDRProvider.__init__(self, provider_def)
        MSCDMSCoreAPIProvider.__init__(self, provider_def)

    @BaseEDRProvider.register()
    def radius(self, **kwargs):
        wkt = kwargs.get('wkt')
        kwargs['bbox'] = [wkt.x, wkt.y, wkt.x, wkt.y]

        return MSCDMSCoreAPIProvider.query(self, **kwargs)
