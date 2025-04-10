# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2023 Tom Kralidis
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

import cftime
from datetime import datetime
import logging
import tempfile

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

            self.get_fields()

        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_fields(self):

        """
        Provide coverage fields

        :returns: CIS JSON object of rangetype metadata
        """

        for name, var in self._data.variables.items():
            LOGGER.debug(f'Determining rangetype for {name}')

            desc, units = None, None
            if len(var.shape) >= 2:
                parameter = self._get_parameter_metadata(
                    name, var.attrs)
                if name not in ['time_bnds', 'spatial_ref']:
                    desc = parameter['description']
                    units = parameter['unit_label']

                    if 'dcs/' in self.data:
                        if 'monthly' not in self.data:
                            name = name[:2]
                        else:
                            dcs = {'pr': 'pr', 'tasmax': 'tx',
                                   'tmean': 'tm', 'tasmin': 'tn'}
                            name = dcs[name]

                    dtype = var.dtype
                    if dtype.name.startswith('float'):
                        dtype = 'number'
                    elif dtype.name.startswith('int'):
                        dtype = 'integer'

                    self._fields[name] = {
                        'title': var.attrs.get('long_name') or desc,
                        'type': dtype,
                        'x-ogc-unit': units,
                        '_meta': {
                            'tags': var.attrs,
                            'uom': {
                                'id': f'http://www.opengis.net/def/uom/UCUM/{units}', # noqa
                                'type': 'UnitReference',
                                'code': units
                            },
                            'encodingInfo': {
                                'dataType': f'http://www.opengis.net/def/dataType/OGC/0/{var.dtype}' # noqa
                            },
                            'nodata': 'null',
                        }
                    }

        return self._fields

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = {}

        if 'avg_20years' not in self.data:
            domainset['percentile'] = {
                'definition': 'Percentiles - IrregularAxis',
                'interval': [[5, 25, 50, 75, 95]],
                'unit': '%',
                }
        else:
            domainset['P20Y-Avg'] = {
                'definition': 'P20Y-Avg - IrregularAxis',
                'interval': [['2021-2040', '2041-2060',
                              '2061-2080', '2081-2100']],
                }

        if 'RCP' in self.data:
            domainset['scenario'] = {
                'definition': 'Scenarios - IrregularAxis',
                'interval': [['RCP2.6', 'RCP4.5', 'RCP8.5']]
                }

        if 'season' in self.data:
            domainset['season'] = {
                'definition': 'Seasons - IrregularAxis',
                'interval': [['DJF', 'MAM', 'JJA', 'SON']]
                }

        return domainset

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """

        time_enabled = False
        time_var, y_var, x_var = [None, None, None]
        for coord in self._data.coords:
            try:
                if coord.lower() == 'time':
                    time_var = coord
                    if len(self._data.coords[coord]) > 1:
                        time_enabled = True
                    continue
                if self._data.coords[coord].attrs['units'] == 'degrees_north':
                    y_var = coord
                    continue
                if self._data.coords[coord].attrs['units'] == 'degrees_east':
                    x_var = coord
                    continue
            except KeyError as err:
                LOGGER.debug(f'Unknown coords in {self.data}: {err}')

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
            'time_axis_label': self.time_field,
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'x_axis_label': self.x_field,
            'y_axis_label': self.y_field,
            'width': self._data.sizes[self.x_field],
            'height': self._data.sizes[self.y_field],
            'resx': np.abs(self._data.coords[self.x_field].values[1]
                           - self._data.coords[self.x_field].values[0]),
            'resy': np.abs(self._data.coords[self.y_field].values[1]
                           - self._data.coords[self.y_field].values[0])
        }

        if 'crs' in self._data.variables.keys():
            bbox_crs = f'http://www.opengis.net/def/crs/OGC/1.3/{self._data.crs.epsg_code}'  # noqa
            properties['bbox_crs'] = bbox_crs

            properties['crs_type'] = 'ProjectedCRS'

        # if 'avg_20years' not in self.data:
        if time_enabled:
            properties['restime'] = self.get_time_resolution()
            properties['time_range'] = [
                self._to_datetime_string(
                    self._data.coords[self.time_field].values[0]
                ),
                self._to_datetime_string(
                    self._data.coords[self.time_field].values[-1]
                )
            ]

        properties['uad'] = self.get_coverage_domainset()

        return properties

    def get_time_resolution(self):
        """
        Helper function to derive time resolution
        :returns: time resolution string
        """

        if self._data[self.time_field].size > 1:

            self.monthly_data = ['monthly_ens', 'SPEI', '_P1M']

            if any(month in self.data for month in self.monthly_data):
                period = 'P1M'
            else:
                period = 'P1Y'

            return period

        else:
            return None

    def _to_datetime_string(self, datetime_):
        """
        Convenience function to formulate string from various datetime objects

        :param datetime_obj: datetime object (native datetime, cftime)

        :returns: str representation of datetime
        """

        try:
            if isinstance(datetime_, (cftime._cftime.DatetimeNoLeap,
                                      cftime._cftime.DatetimeGregorian)):
                cftime_str = datetime_.strftime('%Y-%m-%d %H:%M:%S')
                python_datetime = datetime.strptime(cftime_str,
                                                    '%Y-%m-%d %H:%M:%S')
                datetime_ = np.datetime64(python_datetime)

            if any(month in self.data for month in self.monthly_data):
                month = datetime_.astype('datetime64[M]').astype(int) % 12 + 1
                year = datetime_.astype('datetime64[Y]').astype(int) + 1970
                value = f'{year}-{month:02}'
            else:
                value = datetime_.astype('datetime64[Y]').astype(int) + 1970
                value = str(value)
            return value
        except Exception as err:
            LOGGER.error(err)

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
                    self.data = self.data.replace('pctl50', f'pctl{pctl}')

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

        # set default variable if properties is None
        properties_ = properties.copy()

        if not properties:
            name = list(self._data.variables.keys())[-1]
            properties_.append(name)
        elif 'dcs' in self.data:
            dcs_properties = []
            if 'monthly' not in self.data:
                for v in properties_:
                    dcs_properties.append(next(k for k in list(
                        self._data.variables.keys()) if v in k))
            else:
                dcs = {'pr': 'pr', 'tx': 'tasmax',
                       'tm': 'tmean', 'tn': 'tasmin'}
                for v in properties_:
                    dcs_properties.append(dcs[v])
            properties_ = dcs_properties

        # workaround for inconsistent time values in the NetCDF
        if 'cmip5' in self.data:
            if len(properties) > 1:
                err = 'Only one properties value is supported for this data'
                LOGGER.error(err)
                raise ProviderQueryError(err)
            try:
                cmip5_var = {'pr': 'PCP',
                             'sfcWind': 'SFCWND',
                             'sic': 'SICECONC',
                             'sit': 'SICETHKN',
                             'snd': 'SNDPT',
                             'tas': 'TEMP'}
                _var = cmip5_var[properties_[0]]
                cmip5_file = self.data.replace('*', _var)
            except KeyError as err:
                LOGGER.error(err)
                msg = 'Not a valid properties value'
                raise ProviderQueryError(msg)
            data = xarray.open_dataset(cmip5_file)
        else:
            data = self._data[[*properties_]]

        if any([bbox, datetime_ is not None]):

            LOGGER.debug('Creating spatio-temporal subset')

            query_params = {}

            if bbox and len(bbox) > 0:
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

        if 'avg_20years' not in self.data:
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
        elif format_ == 'zarr':
            LOGGER.debug('Returning data in native zarr format')
            return _get_zarr_data(data)
        # elif format_.lower() == 'geotiff':
        #     if len(properties) == 1:
        #         import rioxarray
        #         with tempfile.TemporaryFile() as fp:
        #             LOGGER.debug('Returning data in GeoTIFF format')
        #             data.rio.write_crs("epsg:4326", inplace=True)
        #             data[properties[0]].rio.to_raster('/tmp/tmp.tif')
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

    def gen_covjson(self, metadata, data, range_type):
        """
        Generate coverage as CoverageJSON representation

        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :param range_type: range type list

        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')
        minx, miny, maxx, maxy = metadata['bbox']
        mint, maxt = metadata['time']

        try:
            tmp_min = data.coords[self.y_field].values[0]
        except IndexError:
            tmp_min = data.coords[self.y_field].values
        try:
            tmp_max = data.coords[self.y_field].values[-1]
        except IndexError:
            tmp_max = data.coords[self.y_field].values

        if tmp_min > tmp_max:
            LOGGER.debug(f'Reversing direction of {self.y_field}')
            miny = tmp_max
            maxy = tmp_min

        cj = {
            'type': 'Coverage',
            'domain': {
                'type': 'Domain',
                'domainType': 'Grid',
                'axes': {
                    'x': {
                        'start': minx,
                        'stop': maxx,
                        'num': metadata['width']
                    },
                    'y': {
                        'start': maxy,
                        'stop': miny,
                        'num': metadata['height']
                    },
                    self.time_field: {
                        'start': mint,
                        'stop': maxt,
                        'num': metadata['time_steps']
                    }
                },
                'referencing': [{
                    'coordinates': ['x', 'y'],
                    'system': {
                        'type': self._coverage_properties['crs_type'],
                        'id': self._coverage_properties['bbox_crs']
                    }
                }]
            },
            'parameters': {},
            'ranges': {}
        }

        for variable in range_type:
            pm = self._get_parameter_metadata(
                variable, self._data[variable].attrs)

            parameter = {
                'type': 'Parameter',
                'description': {
                    'en': pm['description']
                    },
                'unit': {
                    'symbol': pm['unit_label']
                },
                'observedProperty': {
                    'id': pm['observed_property_id'],
                    'label': {
                        'en': pm['observed_property_name']
                    }
                }
            }

            cj['parameters'][pm['id']] = parameter

        data = data.fillna(None)
        data = _convert_float32_to_float64(data)

        try:
            for key in cj['parameters'].keys():
                cj['ranges'][key] = {
                    'type': 'NdArray',
                    'dataType': str(self._data[variable].dtype),
                    'axisNames': [
                        'y', 'x', self._coverage_properties['time_axis_label']
                    ],
                    'shape': [metadata['height'],
                              metadata['width'],
                              metadata['time_steps']]
                }
                cj['ranges'][key]['values'] = data[key].values.flatten().tolist()  # noqa
        except IndexError as err:
            LOGGER.warning(err)
            raise ProviderQueryError('Invalid query parameter')

        return cj


def open_data(data):
    """
    Convenience function to open multiple files with xarray
    :param data: path to files

    :returns: xarray dataset
    """

    try:
        open_func = xarray.open_mfdataset
        try:
            _data = open_func(data)
        except ValueError:
            _data = open_func(data, decode_times=False)
            _data['time'] = cftime.num2date(_data['time'].values,
                                            units='hours since 0001-01-01',
                                            calendar='standard')
        _data = _convert_float32_to_float64(_data)

        return _data
    except Exception as err:
        LOGGER.error(err)
