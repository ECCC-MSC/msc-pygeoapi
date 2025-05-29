# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2025 Louis-Philippe Rousseau-Lambert
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

import logging
import tempfile

from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderNoDataError,
                                    ProviderQueryError)
from msc_pygeoapi.provider.climate_xarray import (ClimateProvider,
                                                  open_data)

LOGGER = logging.getLogger(__name__)


class CanDCSU6Provider(ClimateProvider):
    """CanDCSU6 Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.xarray_.XarrayProvider
        """

        BaseProvider.__init__(self, provider_def)

        try:
            self.period = 'P1Y'
            self._data = open_data(self.data)
            self._coverage_properties = self._get_coverage_properties()

            self.axes = [self._coverage_properties['x_axis_label'],
                         self._coverage_properties['y_axis_label']]

            if 'SSP' in self.data:
                self.axes.append('scenario')
            if any(item in self.data for item in ['DJF', 'MAM', 'JJA', 'SON']):
                self.axes.append('season')
            if 'Pct' in self.data:
                self.axes.append('percentile')
            if all(item not in self.data for item in
                   ['2021-2050', '2041-2070', '2071-2100']):
                self.axes.extend(
                    [self._coverage_properties['time_axis_label']])
            else:
                self.axes.append('P30Y-Avg')

            self.get_fields()

        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = super().get_coverage_domainset()

        if any(item in self.data for item in
               ['2021-2050', '2041-2070', '2071-2100']):
            domainset['P30Y-Avg'] = {
                'definition': 'P30Y-Avg - IrregularAxis',
                'interval': [['2021-2050', '2041-2070', '2071-2100']]
                }

        if 'percentile' in domainset:
            domainset['percentile'] = {
                'definition': 'Percentiles - IrregularAxis',
                'interval': [[10, 50, 90]],
                'unit': '%',
                }
        if 'SSP' in self.data:
            domainset['scenario'] = {
                'definition': 'Scenarios - IrregularAxis',
                'interval': [['SSP126', 'SSP245', 'SSP585']]
                }
        if any(item in self.data for item in ['DJF', 'MAM', 'JJA', 'SON']):
            domainset['season'] = {
                'definition': 'Seasons - IrregularAxis',
                'interval': [['DJF', 'MAM', 'JJA', 'SON']]
                }

        return domainset

    def query(self, properties=[], subsets={},
              bbox=[], datetime_=None, format_='json'):
        """
         Extract data from collection collection

        :param properties: list of data variables to return
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
                elif scenario[0] != 'SSP126':
                    scenario_value = scenario[0].replace('SSP', '')
                    self.data = self.data.replace('126', scenario_value)
            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            LOGGER.debug(self.data)

            subsets.pop('scenario')

        if 'percentile' in subsets:
            percentile = subsets['percentile']

            try:
                if percentile != [50]:
                    pctl = str(percentile[0])
                    self.data = self.data.replace('Pct50', f'Pct{pctl}')

            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('percentile')

        if 'P30Y-Avg' in subsets:
            years_avg = subsets['P30Y-Avg']

            try:
                if len(years_avg) > 1:
                    msg = 'multiple 30 years average are not supported'
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                elif years_avg[0] != '2021-2050':
                    self.data = self.data.replace('2021-2050', years_avg[0])
            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('P30Y-Avg')

        if 'season' in subsets:
            seasonal = subsets['season']

            try:
                if len(seasonal) > 1:
                    msg = 'multiple seasons are not supported'
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                elif seasonal != ['DJF']:
                    season = str(seasonal[0])
                    self.data = self.data.replace('DJF',
                                                  season)

            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('season')

        self._data = open_data(self.data)

        # set default variable if properties is None
        properties_ = properties.copy()

        var_list = list(self._data.variables.keys())

        if not properties:
            name = var_list[0]
            properties_.append(name)

        data = self._data[[*properties_]]

        if any([self._coverage_properties['time_axis_label'] in subsets,
                bbox,
                datetime_ is not None]):

            LOGGER.debug('Creating spatio-temporal subset')

            query_params = {}
            for key, val in subsets.items():
                if data.coords[key].values[0] > data.coords[key].values[-1]:
                    LOGGER.debug('Reversing slicing low/high')
                    query_params[key] = slice(val[1], val[0])
                else:
                    query_params[key] = slice(val[0], val[1])

            if bbox:
                query_params[self._coverage_properties['x_axis_label']] = \
                    slice(bbox[0], bbox[2])

                self._coverage_properties['time_axis_label']

                lat = self._data.coords[self.y_field]
                lat_field = self._coverage_properties['y_axis_label']

                if lat.values[1] > lat.values[0]:
                    query_params[lat_field] = \
                        slice(bbox[1], bbox[3])
                else:
                    query_params[lat_field] = \
                        slice(bbox[3], bbox[1])

            if datetime_ is not None:
                if 'avg_30years' in self.data:
                    msg = 'datetime not suported for 30 years average layers'
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                elif self._coverage_properties['time_axis_label'] in subsets:
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

            LOGGER.debug(f'Query parameters: {query_params}')
            try:
                data = data.loc[query_params]
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
            'time': [None, None],
            "driver": "xarray",
            "height": data.sizes[self.y_field],
            "width": data.sizes[self.x_field],
            "time_steps": 1,
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }

        if 'avg_30years' not in self.data:
            out_meta["time"] = [
                self._to_datetime_string(
                    data.coords[self.time_field].values[0]),
                self._to_datetime_string(
                    data.coords[self.time_field].values[-1])
            ]
            out_meta['time_steps'] = data.sizes[self.time_field]

        self.filename = self.data.split('/')[-1].replace(
            '*', '-'.join(properties))

        LOGGER.debug('Serializing data in memory')
        if format_ == 'json':
            LOGGER.debug('Creating output in CoverageJSON')
            return self.gen_covjson(out_meta, data, properties_)
        else:  # return data in native format
            with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in native NetCDF format')
                fp.write(data.to_netcdf())
                fp.seek(0)
                return fp.read()
