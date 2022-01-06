# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2021 Louis-Philippe Rousseau-Lambert
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

import tempfile
import logging

from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderNoDataError,
                                    ProviderQueryError)
from msc_pygeoapi.provider.climate_xarray import (ClimateProvider,
                                                  open_data)
from pygeoapi.provider.xarray_ import (_get_zarr_data)

LOGGER = logging.getLogger(__name__)


class SPEIProvider(ClimateProvider):
    """CMIP5 Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.xarray_.XarrayProvider
        """

        BaseProvider.__init__(self, provider_def)

        try:
            self._data = open_data(self.data)
            self._coverage_properties = self._get_coverage_properties()

            self.axes = [self._coverage_properties['x_axis_label'],
                         self._coverage_properties['y_axis_label'],
                         self._coverage_properties['time_axis_label'],
                         'percentile']

            if 'RCP' in self.data:
                self.axes.append('scenario')

            self.fields = self._coverage_properties['fields']
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = super().get_coverage_domainset()

        for axis in domainset['generalGrid']['axis']:
            if axis['axisLabel'] == 'percentile':
                domainset['generalGrid']['axis'][3]['coordinate'] = [25,
                                                                     50,
                                                                     75]
                domainset['generalGrid']['axis'][3]['lowerBound'] = 25
                domainset['generalGrid']['axis'][3]['upperBound'] = 75

        return domainset

    def query(self, range_subset=['spei'], subsets={},
              bbox=[], datetime_=None, format_='json'):
        """
         Extract data from collection collection

        :param range_subset: empty for SPEI
        :param subsets: dict of subset names with lists of ranges
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime: temporal (datestamp or extent)
        :param format_: data format of output

        :returns: coverage data as dict of CoverageJSON or native format
        """

        if 'scenario' in subsets:
            scenario = subsets['scenario']
            try:
                if len(scenario) > 1:
                    msg = 'multiple scenario are not supported'
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                elif scenario[0] not in ['RCP2.6', 'hist']:
                    scenario_value = scenario[0].replace('RCP', '')
                    self.data = self.data.replace('2.6', scenario_value)
            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('scenario')

        if 'percentile' in subsets:
            percentile = subsets['percentile']

            try:
                if percentile != [50]:
                    pctl = str(percentile[0])
                    self.data = self.data.replace('pctl50',
                                                  'pctl{}'.format(pctl))

            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('percentile')

        self._data = open_data(self.data)

        data = self._data[[*range_subset]]

        if any([self._coverage_properties['x_axis_label'] in subsets,
                self._coverage_properties['y_axis_label'] in subsets,
                self._coverage_properties['time_axis_label'] in subsets,
                bbox,
                datetime_ is not None]):

            LOGGER.debug('Creating spatio-temporal subset')

            query_params = {}
            for key, val in subsets.items():
                val_0 = self._data.coords[key].values[0]
                val_1 = self._data.coords[key].values[-1]
                if val_0 > val_1:
                    LOGGER.debug('Reversing slicing low/high')
                    query_params[key] = slice(val[1], val[0])
                else:
                    query_params[key] = slice(val[0], val[1])

            if bbox:
                if all([self._coverage_properties['x_axis_label'] in subsets,
                        self._coverage_properties['y_axis_label'] in subsets,
                        len(bbox) > 0]):
                    msg = 'bbox and subsetting by coordinates are exclusive'
                    LOGGER.warning(msg)
                    raise ProviderQueryError(msg)
                else:
                    query_params[self._coverage_properties['x_axis_label']] = \
                        slice(bbox[0], bbox[2])
                    query_params[self._coverage_properties['y_axis_label']] = \
                        slice(bbox[3], bbox[1])

            if datetime_ is not None:
                if self._coverage_properties['time_axis_label'] in subsets:
                    msg = 'datetime and temporal subsetting are exclusive'
                    LOGGER.error(msg)
                else:
                    if '/' in datetime_:

                        begin, end = datetime_.split('/')

                        if begin < end:
                            query_params[self.time_field] = slice(begin, end)
                        else:
                            LOGGER.debug('Reversing slicing from high to low')
                            query_params[self.time_field] = slice(end, begin)
                    else:
                        query_params[self.time_field] = datetime_

            LOGGER.debug('Query parameters: {}'.format(query_params))
            try:
                data = self._data.loc[query_params]
            except Exception as err:
                LOGGER.warning(err)
                raise ProviderQueryError(err)

        if (any([data.coords[self.x_field].size == 0,
                data.coords[self.y_field].size == 0])):
            msg = 'No data found'
            LOGGER.warning(msg)
            raise ProviderNoDataError(msg)

        out_meta = {
            'bbox': [
                data.coords[self.x_field].values[0],
                data.coords[self.y_field].values[0],
                data.coords[self.x_field].values[-1],
                data.coords[self.y_field].values[-1]
            ],
            "time": [
                self._to_datetime_string(
                    data.coords[self.time_field].values[0]),
                self._to_datetime_string(
                    data.coords[self.time_field].values[-1])
            ],
            "driver": "xarray",
            "height": data.dims[self.y_field],
            "width": data.dims[self.x_field],
            "time_steps": data.dims[self.time_field],
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }

        LOGGER.debug('Serializing data in memory')
        if format_ == 'json':
            LOGGER.debug('Creating output in CoverageJSON')
            return self.gen_covjson(out_meta, data, range_subset)
        elif format_ == 'zarr':
            LOGGER.debug('Returning data in native zarr format')
            return _get_zarr_data(data)
        else:  # return data in native format
            with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in native NetCDF format')
                fp.write(data.to_netcdf())
                fp.seek(0)
                return fp.read()
