# =================================================================
#
# Authors: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2024 Etienne Pelletier
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
from collections import OrderedDict
import json
import logging

from elasticsearch import exceptions, helpers

from pygeoapi.provider.base import ProviderConnectionError, ProviderQueryError
from pygeoapi.provider.elasticsearch_ import (
    update_query
)
from pygeoapi.util import crs_transform

from msc_pygeoapi.provider.elasticsearch import MSCElasticsearchProvider


LOGGER = logging.getLogger(__name__)


class CPWElasticsearchProvider(MSCElasticsearchProvider):
    """CPW Elasticsearch Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: msc_pygeoapi.provider.elasticsearch.CPWElasticsearchProvider
        """
        self._nested_fields = []
        super().__init__(provider_def)

    def get_nested_fields(self, properties, fields, prev_field=None):
        """
        Get Elasticsearch fields (names, types) for all nested properties

        :param properties: `dict` of Elasticsearch mappings properties
        :param fields: `dict` of fields in the current iteration
        :param prev_field: name of the parent field

        :returns: `dict` of fields
        """
        for k, v in properties['properties'].items():
            cur_field = k if prev_field is None else f'{prev_field}.{k}'

            if isinstance(v, dict) and 'properties' in v:
                fields = self.get_nested_fields(
                    v, fields, cur_field
                )
                if v.get('type') == 'nested':
                    self._nested_fields.append(cur_field)
                    fields[cur_field] = {'type': v['type']}
            else:
                if 'type' in v:
                    if v['type'] == 'text':
                        fields[cur_field] = {'type': 'string'}
                    elif v['type'] == 'date':
                        fields[cur_field] = {
                            'type': 'string',
                            'format': 'date'
                        }
                    elif v['type'] in ('float', 'long'):
                        fields[cur_field] = {
                            'type': 'number',
                            'format': v['type']
                        }
                    else:
                        fields[cur_field] = {'type': v['type']}
        return fields

    def get_fields(self):
        """
         Get provider field information (names, types)

        :returns: dict of fields
        """
        if not self._fields:
            ii = self.es.indices.get(
                index=self.index_name, allow_no_indices=False
            )

            LOGGER.debug(f'Response: {ii}')
            try:
                if '*' not in self.index_name:
                    mappings = ii[self.index_name]['mappings']
                    p = mappings['properties']['properties']
                else:
                    LOGGER.debug('Wildcard index; setting from first match')
                    index_name_ = list(ii.keys())[0]
                    p = ii[index_name_]['mappings']['properties']['properties']
            except KeyError:
                LOGGER.warning('Trying for alias')
                alias_name = next(iter(ii))
                p = ii[alias_name]['mappings']['properties']['properties']
            except IndexError:
                LOGGER.warning('could not get fields; returning empty set')
                return {}

            self._fields = self.get_nested_fields(p, self._fields)

        return self._fields

    @crs_transform
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
        skip_geometry=False,
        q=None,
        filterq=None,
        **kwargs
    ):
        """
        query Elasticsearch index

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
        :param filterq: filter object

        :returns: dict of 0..n GeoJSON features
        """

        self.select_properties = select_properties

        query = {'track_total_hits': True, 'query': {'bool': {'filter': []}}}
        filter_ = []

        feature_collection = {'type': 'FeatureCollection', 'features': []}

        if resulttype == 'hits':
            LOGGER.debug('hits only specified')
            limit = 0

        if bbox:
            LOGGER.debug('processing bbox parameter')
            minx, miny, maxx, maxy = bbox
            bbox_filter = {
                'geo_shape': {
                    'geometry': {
                        'shape': {
                            'type': 'envelope',
                            'coordinates': [[minx, maxy], [maxx, miny]],
                        },
                        'relation': 'intersects'
                    }
                }
            }

            query['query']['bool']['filter'].append(bbox_filter)

        if datetime_ is not None:
            LOGGER.debug('processing datetime parameter')
            if self.time_field is None:
                LOGGER.error('time_field not enabled for collection')
                raise ProviderQueryError()

            time_field = self.mask_prop(self.time_field)

            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')

                range_ = {
                    'range': {time_field: {'gte': time_begin, 'lte': time_end}}
                }
                if time_begin == '..':
                    range_['range'][time_field].pop('gte')
                elif time_end == '..':
                    range_['range'][time_field].pop('lte')

                filter_.append(range_)

            else:  # time instant
                LOGGER.debug('detected time instant')
                filter_.append({'match': {time_field: datetime_}})

            LOGGER.debug(filter_)
            query['query']['bool']['filter'].append(*filter_)

        if properties:
            LOGGER.debug('processing properties')
            for prop in properties:
                prop_name = self.mask_prop(prop[0])
                matching_nested_field = next(
                    (f for f in self._nested_fields if prop[0].startswith(f)),
                    False
                )
                if matching_nested_field:
                    prop_values = prop[1].split('|')
                    occur = 'should' if '|' in prop[1] else 'must'
                    pf = {
                        'nested': {
                            'path': f'{self.mask_prop(matching_nested_field)}',
                            'query': {'bool': {occur: []}}
                        },
                    }
                    for prop_value in prop_values:
                        pf['nested']['query']['bool'][occur].append(
                            {'match': {prop_name: {'query': prop_value}}}
                        )
                    query['query']['bool']['filter'].append(pf)
                else:
                    pf = {'match': {prop_name: {'query': prop[1]}}}
                    query['query']['bool']['filter'].append(pf)

                    if '|' not in prop[1]:
                        pf['match'][prop_name]['minimum_should_match'] = '100%'

        if sortby:
            LOGGER.debug('processing sortby')
            query['sort'] = []
            for sort in sortby:
                LOGGER.debug(f'processing sort object: {sort}')

                sp = sort['property']

                if self.fields[sp]['type'] in ['object', 'nested']:
                    LOGGER.warning(
                        'Cannot sort by property of type object or nested'
                    )
                    continue

                if (
                    self.fields[sp]['type'] == 'string'
                    and self.fields[sp].get('format') != 'date'
                ):
                    LOGGER.debug('setting ES .raw on property')
                    sort_property = f'{self.mask_prop(sp)}.raw'
                else:
                    sort_property = self.mask_prop(sp)

                sort_order = 'asc'
                if sort['order'] == '-':
                    sort_order = 'desc'

                sort_ = {sort_property: {'order': sort_order}}
                query['sort'].append(sort_)

        if q is not None:
            LOGGER.debug('Adding free-text search')
            query['query']['bool']['must'] = {'query_string': {'query': q}}

            query['_source'] = {
                'excludes': [
                    'properties._metadata-payload',
                    'properties._metadata-schema',
                    'properties._metadata-format'
                ]
            }

        if self.properties or self.select_properties:
            LOGGER.debug('filtering properties')

            all_properties = self.get_properties()

            query['_source'] = {
                'includes': list(map(self.mask_prop, all_properties))
            }

            query['_source']['includes'].append('id')
            query['_source']['includes'].append('type')
            query['_source']['includes'].append('geometry')

        if skip_geometry:
            LOGGER.debug('excluding geometry')
            try:
                query['_source']['excludes'] = ['geometry']
            except KeyError:
                query['_source'] = {'excludes': ['geometry']}
        try:
            LOGGER.debug('querying Elasticsearch')
            if filterq:
                LOGGER.debug(f'adding cql object: {filterq.json()}')
                query = update_query(input_query=query, cql=filterq)
            LOGGER.debug(json.dumps(query, indent=4))

            LOGGER.debug('Testing for ES scrolling')
            if offset + limit > 10000:
                gen = helpers.scan(
                    client=self.es,
                    query=query,
                    preserve_order=True,
                    index=self.index_name
                )
                results = {'hits': {'total': limit, 'hits': []}}
                for i in range(offset + limit):
                    try:
                        if i >= offset:
                            results['hits']['hits'].append(next(gen))
                        else:
                            next(gen)
                    except StopIteration:
                        break

                matched = len(results['hits']['hits']) + offset
                returned = len(results['hits']['hits'])
            else:
                es_results = self.es.search(
                    index=self.index_name, from_=offset, size=limit, **query
                )
                results = es_results
                matched = es_results['hits']['total']['value']
                returned = len(es_results['hits']['hits'])

        except exceptions.ConnectionError as err:
            LOGGER.error(err)
            raise ProviderConnectionError()
        except exceptions.RequestError as err:
            LOGGER.error(err)
            raise ProviderQueryError()
        except exceptions.NotFoundError as err:
            LOGGER.error(err)
            raise ProviderQueryError()

        feature_collection['numberMatched'] = matched

        if resulttype == 'hits':
            return feature_collection

        feature_collection['numberReturned'] = returned

        LOGGER.debug('serializing features')
        for feature in results['hits']['hits']:
            feature_ = self.esdoc2geojson(feature)
            feature_collection['features'].append(feature_)

        return feature_collection

    def esdoc2geojson(self, doc):
        """
        generate GeoJSON `dict` from ES document

        :param doc: `dict` of ES document

        :returns: GeoJSON `dict`
        """

        feature_ = {}
        feature_thinned = {}

        LOGGER.debug('Fetching id and geometry from GeoJSON document')
        feature_ = doc['_source']

        try:
            id_ = doc['_source']['properties'][self.id_field]
        except KeyError as err:
            LOGGER.debug(f'Missing field: {err}')
            id_ = doc['_source'].get('id', doc['_id'])

        feature_['id'] = id_
        feature_['geometry'] = doc['_source'].get('geometry')

        # safeguard against ES returning doc without properties
        if 'properties' not in feature_:
            feature_['properties'] = {}

        if self.properties or self.select_properties:
            LOGGER.debug('Filtering properties')
            all_properties = self.get_properties()
            feature_thinned = {
                'id': id_,
                'type': feature_['type'],
                'geometry': feature_.get('geometry'),
                'properties': OrderedDict()
            }
            for p in all_properties:
                try:
                    if p in feature_['properties']:
                        feature_thinned['properties'][p] = \
                            feature_['properties'][p]
                    else:
                        if '.' in p:
                            p_root = p.split('.')[0]
                            feature_thinned['properties'][p_root] = feature_[
                                'properties'
                            ].get(p_root)
                except KeyError as err:
                    msg = f'Property missing {err}; continuing'
                    LOGGER.warning(msg)

        if feature_thinned:
            return feature_thinned
        else:
            return feature_
