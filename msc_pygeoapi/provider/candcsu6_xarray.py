# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2023 Louis-Philippe Rousseau-Lambert
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

import numpy as np

from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderNoDataError,
                                    ProviderQueryError)
from pygeoapi.provider.xarray_ import XarrayProvider
from msc_pygeoapi.provider.climate_xarray import open_data

LOGGER = logging.getLogger(__name__)


class CanDCSU6Provider(XarrayProvider):
    """CanDCSU6 Provider"""

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

            self.fields = self._coverage_properties['fields']

        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        c_props = self._coverage_properties

        domainset = super().get_coverage_domainset()

        new_axis_name = []
        new_axis = []

        new_axis_name.extend(['percentile'])
        new_axis.extend([{
                            'type': 'IrregularAxis',
                            'axisLabel': 'percentile',
                            'coordinate': [10, 50, 90],
                            'lowerBound': 10,
                            'upperBound': 90,
                            'uomLabel': '%'
                            }])

        if all(item not in self.data for item in
               ['1971-2000', '2021-2050', '2041-2070', '2071-2100']):
            time_resolution = c_props['restime']['value']
            time_period = c_props['restime']['period']
            domainset['generalGrid']['axis'][2]['uomLabel'] = time_period
            domainset['generalGrid']['axis'][2]['resolution'] = time_resolution
        elif '1971-2000' not in self.data:
            new_axis_name.extend(['P30Y-Avg'])
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'P30Y-Avg',
                             'coordinate': ['2021-2050', '2041-2070',
                                            '2071-2100'],
                             }])
            domainset['generalGrid']['axis'].pop(2)
            domainset['generalGrid']['axisLabels'].pop(-1)
        else:
            domainset['generalGrid']['axis'].pop(2)
            domainset['generalGrid']['axisLabels'].pop(-1)

        if 'SSP' in self.data:
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'scenario',
                             'coordinate': ['SSP126', 'SSP245', 'SSP585']
                             }])
            new_axis_name.append('scenario')

        if any(item in self.data for item in ['DJF', 'MAM', 'JJA', 'SON']):
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'season',
                             'coordinate': ['DJF', 'MAM', 'JJA', 'SON']
                             }])
            new_axis_name.append('season')

        domainset['generalGrid']['axisLabels'].extend(new_axis_name)
        domainset['generalGrid']['axis'].extend(new_axis)

        return domainset

    def get_coverage_rangetype(self):
        """
        Provide coverage rangetype

        :returns: CIS JSON object of rangetype metadata
        """

        rangetype = {
            'type': 'DataRecord',
            'field': []
        }

        for name, var in self._data.variables.items():
            LOGGER.debug(f'Determining rangetype for {name}')

            desc, units = None, None
            if len(var.shape) >= 2:
                parameter = self._get_parameter_metadata(
                    name, var.attrs)
                desc = parameter['description']
                units = parameter['unit_label']

                rangetype['field'].append({
                    'id': name,
                    'type': 'Quantity',
                    'name': var.attrs.get('long_name') or desc,
                    'encodingInfo': {
                        'dataType': f'http://www.opengis.net/def/dataType/OGC/0/{var.dtype}'  # noqa
                    },
                    'nodata': 'null',
                    'uom': {
                        'id': f'http://www.opengis.net/def/uom/UCUM/{units}',
                        'type': 'UnitReference',
                        'code': units
                    },
                    '_meta': {
                        'tags': var.attrs
                    }
                })

        return rangetype

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """

        time_var, y_var, x_var = [None, None, None]
        for coord in self._data.coords:
            if coord.lower() == 'time':
                time_var = coord
                continue
            try:
                if self._data.coords[coord].attrs['units'] == 'degrees_north':
                    y_var = coord
                    continue
                if self._data.coords[coord].attrs['units'] == 'degrees_east':
                    x_var = coord
                    continue
            except KeyError:
                LOGGER.warning(f'unknown dimension{self._data.coords[coord]}')

        if self.x_field is None:
            self.x_field = x_var
        if self.y_field is None:
            self.y_field = y_var
        if self.time_field is None:
            self.time_field = time_var

        properties = {
            'bbox': [
                self._data.coords[self.x_field].values[0],
                self._data.coords[self.y_field].values[0],
                self._data.coords[self.x_field].values[-1],
                self._data.coords[self.y_field].values[-1],
            ],
            'time_range': [0, 0],
            'restime': 0,
            'time_axis_label': self.time_field,
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'x_axis_label': self.x_field,
            'y_axis_label': self.y_field,
            'width': self._data.sizes[self.x_field],
            'height': self._data.sizes[self.y_field],
            'bbox_units': 'degrees',
            'resx': np.abs(self._data.coords[self.x_field].values[1]
                           - self._data.coords[self.x_field].values[0]),
            'resy': np.abs(self._data.coords[self.y_field].values[1]
                           - self._data.coords[self.y_field].values[0])
        }

        if 'crs' in self._data.variables.keys():
            bbox_crs = f'http://www.opengis.net/def/crs/OGC/1.3/{self._data.crs.epsg_code}'  # noqa
            properties['bbox_crs'] = bbox_crs

            properties['inverse_flattening'] = self._data.crs.\
                inverse_flattening

            properties['crs_type'] = 'ProjectedCRS'

        properties['axes'] = [
            properties['x_axis_label'],
            properties['y_axis_label']
        ]

        if 'avg_30years' not in self.data:
            properties['axes'].append(properties['time_axis_label'])
            properties['time_duration'] = self.get_time_coverage_duration()
            properties['restime'] = self.get_time_resolution()
            properties['time_range'] = [
                self._to_datetime_string(
                    self._data.coords[self.time_field].values[0]
                ),
                self._to_datetime_string(
                    self._data.coords[self.time_field].values[-1]
                )
            ]
            properties['time'] = self._data.sizes[self.time_field]

        properties['fields'] = [name for name in self._data.variables
                                if len(self._data.variables[name].shape) >= 3]

        return properties

    def get_time_resolution(self):
        """
        Helper function to derive time resolution
        :returns: time resolution string
        """

        if self._data[self.time_field].size > 1:

            self.monthly_data = ['P1M']

            if any(month in self.data for month in self.monthly_data):
                period = 'month'
            else:
                period = 'year'

            return {'value': 1, 'period': period}

        else:
            return None

    def _to_datetime_string(self, datetime_):
        """
        Convenience function to formulate string from various datetime objects

        :param datetime_obj: datetime object (native datetime, cftime)

        :returns: str representation of datetime
        """

        try:
            if any(month in self.data for month in self.monthly_data):
                month = datetime_.astype('datetime64[M]').astype(int) % 12 + 1
                year = datetime_.astype('datetime64[Y]').astype(int) + 1970
                value = f'{year}-{month:02}'
            else:
                value = datetime_.astype('datetime64[Y]').astype(int) + 1970
                value = str(value)
            return value
        except Exception as err:
            LOGGER.warning(err)

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

        if any([self._coverage_properties['x_axis_label'] in subsets,
                self._coverage_properties['y_axis_label'] in subsets,
                self._coverage_properties['time_axis_label'] in subsets,
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
                if all([self._coverage_properties['x_axis_label'] in subsets,
                        self._coverage_properties['y_axis_label'] in subsets,
                        len(bbox) > 0]):
                    msg = 'bbox and subsetting by coordinates are exclusive'
                    LOGGER.warning(msg)
                    raise ProviderQueryError(msg)
                else:
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
