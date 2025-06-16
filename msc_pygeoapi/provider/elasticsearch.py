# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
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

from datetime import datetime
import logging

from pygeoapi.provider.elasticsearch_ import ElasticsearchProvider

from msc_pygeoapi.util import DATETIME_RFC3339_FMT

LOGGER = logging.getLogger(__name__)


class MSCElasticsearchProvider(ElasticsearchProvider):
    """MSC Elasticsearch Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: msc_pygeoapi.provider.elasticsearch_.MSCElasticsearchProvider
        """

        super().__init__(provider_def)

    def _get_timefield_format(self):
        """
        Retrieve time_field format from ES index mapping

        :returns: `str` of time_field format
        """
        mapping = self.es.indices.get_mapping(index=self.index_name)
        try:
            p = mapping[self.index_name]['mappings']['properties'][
                'properties'
            ]
        except KeyError:
            LOGGER.debug(
                'Could not find index name in mapping. '
                'Setting from first matching index.'
            )
            index_name_ = list(mapping.keys())[0]
            p = mapping[index_name_]['mappings']['properties']['properties']

        format_ = p['properties'][self.time_field].get('format')

        if format_ and '||' in format_:
            format_ = format_.split('||')[0]

        LOGGER.debug(f'time_field format: {format_}')

        return format_

    def _clamp_datetime(self, datetime_, timefield_format):
        """
        Clamp datetime to timefield_format

        :param datetime: `datetime` object
        :param timefield_format: time_field format retrieved from Elasticsearch

        :returns: clamped `datetime` object
        """

        time_patterns_replace = {
            'yyyy': dict(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
            'yyyy-MM': dict(day=1, hour=0, minute=0, second=0, microsecond=0),
            'yyyy-MM-dd': dict(hour=0, minute=0, second=0, microsecond=0),
            "yyyy-MM-dd'T'HH": dict(minute=0, second=0, microsecond=0),
            "yyyy-MM-dd'T'HH:mm": dict(second=0, microsecond=0),
            "yyyy-MM-dd'T'HH:mm:ss'Z'": dict(microsecond=0)
        }

        if timefield_format not in time_patterns_replace:
            LOGGER.warning(
                'Unrecognized time_field format. Skipping clamping.'
            )
            return datetime_

        return datetime_.replace(**time_patterns_replace[timefield_format])

    def _get_clamped_datetime_range(self, datetime_):
        """
        Return clamped datetime range or instant from RFC 3339 datetime string

        :param datetime_: `str` datetime range or instant in RFC 3339 format

        :returns: `tuple` of `datetime.datetime` objects
        """
        if '/' in datetime_:
            LOGGER.debug('detected time range')
            time_begin, time_end = [
                datetime.strptime(time, DATETIME_RFC3339_FMT)
                for time in datetime_.split('/')
            ]
            return (
                self._clamp_datetime(time_begin, self.timefield_format),
                self._clamp_datetime(time_end, self.timefield_format)
            )
        else:
            LOGGER.debug('detected time instant')
            time = datetime.strptime(datetime_, DATETIME_RFC3339_FMT)
            return (self._clamp_datetime(time, self.timefield_format),)

    def query(self, *args, **kwargs):

        if kwargs.get('datetime_'):
            self.timefield_format = self._get_timefield_format()

            if not self.timefield_format:
                LOGGER.warning(
                    'Could not retrieve time_field format from index '
                    'mapping. Skipping time clamping.'
                )
                return super().query(*args, **kwargs)

            try:
                clamped_datetime_range = self._get_clamped_datetime_range(
                    kwargs['datetime_']
                )
                kwargs['datetime_'] = '/'.join(
                    [
                        datetime_.strftime(DATETIME_RFC3339_FMT)
                        for datetime_ in clamped_datetime_range
                    ]
                )
            except ValueError:
                LOGGER.warning(
                    'Invalid RFC 3339 datetime format received. '
                    'Skipping time clamping.'
                )

        return super().query(*args, **kwargs)

    def __repr__(self):
        return f'<MSCElasticsearchProvider> {self.data}'
