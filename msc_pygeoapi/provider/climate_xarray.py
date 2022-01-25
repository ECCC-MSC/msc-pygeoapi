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

import numpy as np
import xarray

from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderNoDataError,
                                    ProviderQueryError)
from pygeoapi.provider.xarray_ import (XarrayProvider,
                                       _convert_float32_to_float64,
                                       _get_zarr_data)

LOGGER = logging.getLogger(__name__)

DCS_VAR = ('tx', 'tm', 'tn', 'pr')


class ClimateProvider(XarrayProvider):
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
                         self._coverage_properties['y_axis_label']]

            if 'RCP' in self.data:
                self.axes.append('scenario')
            if 'season' in self.data:
                self.axes.append('season')
            if 'avg_20years' not in self.data:
                self.axes.extend([self._coverage_properties['time_axis_label'],
                                  'percentile'])
            else:
                self.axes.append('P20Y-Avg')

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

        domainset = super().get_coverage_domainset(self)

        new_axis_name = []
        new_axis = []

        if 'avg_20years' not in self.data:
            new_axis_name.extend(['percentile'])
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'percentile',
                             'coordinate': [5, 25, 50, 75, 95],
                             'lowerBound': 5,
                             'upperBound': 95,
                             'uomLabel': '%',
                             }])
            time_resolution = c_props['restime']['value']
            time_period = c_props['restime']['period']
            domainset['generalGrid']['axis'][2]['uomLabel'] = time_period
            domainset['generalGrid']['axis'][2]['resolution'] = time_resolution
        else:
            new_axis_name.extend(['P20Y-Avg'])
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'P20Y-Avg',
                             'coordinate': ['2021-2040', '2041-2060',
                                            '2061-2080', '2081-2100'],
                             }])

            domainset['generalGrid']['axis'].pop(2)
            domainset['generalGrid']['axisLabels'].pop(-1)

        if 'RCP' in self.data:
            new_axis.extend([{
                             'type': 'IrregularAxis',
                             'axisLabel': 'scenario',
                             'coordinate': ['RCP2.6', 'RCP4.5', 'RCP8.5']
                             }])
            new_axis_name.append('scenario')

        if 'season' in self.data:
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
            LOGGER.debug('Determining rangetype for {}'.format(name))

            desc, units = None, None
            if len(var.shape) >= 2:
                parameter = self._get_parameter_metadata(
                    name, var.attrs)
                desc = parameter['description']
                units = parameter['unit_label']

                if 'dcs' in self.data:
                    if 'monthly' not in self.data:
                        name = name[:2]
                    else:
                        dcs = {'pr': 'pr', 'tasmax': 'tx',
                               'tmean': 'tm', 'tasmin': 'tn',
                               'time_bnds': 'time_bnds'}
                        name = dcs[name]

                rangetype['field'].append({
                    'id': name,
                    'type': 'Quantity',
                    'name': var.attrs.get('long_name') or desc,
                    'encodingInfo': {
                        'dataType': 'http://www.opengis.net/def/dataType/OGC/0/{}'.format(str(var.dtype))  # noqa
                    },
                    'nodata': 'null',
                    'uom': {
                        'id': 'http://www.opengis.net/def/uom/UCUM/{}'.format(
                             units),
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
            if self._data.coords[coord].attrs['units'] == 'degrees_north':
                y_var = coord
                continue
            if self._data.coords[coord].attrs['units'] == 'degrees_east':
                x_var = coord
                continue

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
            'width': self._data.dims[self.x_field],
            'height': self._data.dims[self.y_field],
            'bbox_units': 'degrees',
            'resx': np.abs(self._data.coords[self.x_field].values[1]
                           - self._data.coords[self.x_field].values[0]),
            'resy': np.abs(self._data.coords[self.y_field].values[1]
                           - self._data.coords[self.y_field].values[0]),
        }

        if 'crs' in self._data.variables.keys():
            properties['bbox_crs'] = '{}/{}'.format(
                'http://www.opengis.net/def/crs/OGC/1.3/',
                self._data.crs.epsg_code)

            properties['inverse_flattening'] = self._data.crs.\
                inverse_flattening

            properties['crs_type'] = 'ProjectedCRS'

        properties['axes'] = [
            properties['x_axis_label'],
            properties['y_axis_label']
        ]

        if 'avg_20years' not in self.data:
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
            properties['time'] = self._data.dims[self.time_field]

        properties['fields'] = [name for name in self._data.variables
                                if len(self._data.variables[name].shape) >= 3]
        if 'dcs' in self.data:
            properties['fields'].extend(('tx', 'tm', 'tn', 'pr'))

        return properties

    def get_time_resolution(self):
        """
        Helper function to derive time resolution
        :returns: time resolution string
        """

        if self._data[self.time_field].size > 1:

            self.monthly_data = ['monthly_ens', 'SPEI']

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
                value = '{}-{}'.format(year, str(month).zfill(2))
            else:
                value = datetime_.astype('datetime64[Y]').astype(int) + 1970
                value = str(value)
            return value
        except Exception as err:
            LOGGER.error(err)

    def query(self, range_subset=[], subsets={},
              bbox=[], datetime_=None, format_='json'):
        """
         Extract data from collection collection

        :param range_subset: list of data variables to return
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

        if 'P20Y-Avg' in subsets:
            years_avg = subsets['P20Y-Avg']

            try:
                if len(years_avg) > 1:
                    msg = 'multiple 20 years average are not supported'
                    LOGGER.error(msg)
                    raise ProviderQueryError(msg)
                elif years_avg[0] not in ['2021-2040']:
                    self.data = self.data.replace('2021-2040', years_avg[0])
            except Exception as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

            subsets.pop('P20Y-Avg')

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

        # set default variable if range_subset is None
        range_subset_ = range_subset.copy()

        if not range_subset:
            name = list(self._data.variables.keys())[-1]
            range_subset_.append(name)
        elif 'dcs' in self.data:
            dcs_range_subset = []
            if 'monthly' not in self.data:
                for v in range_subset_:
                    dcs_range_subset.append(next(k for k in list(
                        self._data.variables.keys()) if v in k))
            else:
                dcs = {'pr': 'pr', 'tx': 'tasmax',
                       'tm': 'tmean', 'tn': 'tasmin'}
                for v in range_subset_:
                    dcs_range_subset.append(dcs[v])
            range_subset_ = dcs_range_subset

        # workaround for inconsistent time values in the NetCDF
        if 'cmip5' in self.data:
            if len(range_subset) > 1:
                err = 'Only one range-subset value is supported for this data'
                LOGGER.error(err)
                raise ProviderQueryError(err)
            try:
                cmip5_var = {'pr': 'PCP',
                             'sfcWind': 'SFCWND',
                             'sic': 'SICECONC',
                             'sit': 'SICETHKN',
                             'snd': 'SNDPT',
                             'tas': 'TEMP'}
                _var = cmip5_var[range_subset_[0]]
                cmip5_file = self.data.replace('*', _var)
            except KeyError as err:
                LOGGER.error(err)
                msg = 'Not a validd range-subset value'
                raise ProviderQueryError(msg)
            data = xarray.open_dataset(cmip5_file)
        else:
            data = self._data[[*range_subset_]]

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
                if 'avg_20years' in self.data:
                    msg = 'datetime not suported for 20 years average layers'
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

            LOGGER.debug('Query parameters: {}'.format(query_params))
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
            "height": data.dims[self.y_field],
            "width": data.dims[self.x_field],
            "time_steps": 1,
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }

        if 'avg_20years' not in self.data:
            out_meta["time"] = [
                self._to_datetime_string(
                    data.coords[self.time_field].values[0]),
                self._to_datetime_string(
                    data.coords[self.time_field].values[-1])
            ]
            out_meta['time_steps'] = data.dims[self.time_field]

        LOGGER.debug('Serializing data in memory')
        if format_ == 'json':
            LOGGER.debug('Creating output in CoverageJSON')
            return self.gen_covjson(out_meta, data, range_subset_)
        elif format_ == 'zarr':
            LOGGER.debug('Returning data in native zarr format')
            return _get_zarr_data(data)
        # elif format_.lower() == 'geotiff':
        #     if len(range_subset) == 1:
        #         import rioxarray
        #         with tempfile.TemporaryFile() as fp:
        #             LOGGER.debug('Returning data in GeoTIFF format')
        #             data.rio.write_crs("epsg:4326", inplace=True)
        #             data[range_subset[0]].rio.to_raster('/tmp/tmp.tif')
        #             with open('/tmp/tmp.tif') as fp:
        #                 fp.seek(0)
        #                 return fp
        #     else:
        #         err = 'Only one range subset supoported for GeoTIFF'
        #         LOGGER.error(err)
        #         raise ProviderQueryError(err)

        else:  # return data in native format
            with tempfile.TemporaryFile() as fp:
                LOGGER.debug('Returning data in native NetCDF format')
                fp.write(data.to_netcdf())
                fp.seek(0)
                return fp.read()


def open_data(data):
    """
    Convenience function to open multiple files with xarray
    :param data: path to files

    :returns: xarray dataset
    """

    try:
        open_func = xarray.open_mfdataset
        _data = open_func(data)
        _data = _convert_float32_to_float64(_data)

        return _data
    except Exception as err:
        LOGGER.error(err)
