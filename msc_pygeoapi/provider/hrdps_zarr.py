# =================================================================
#
# Authors: Adan Butt <Adan.Butt@ec.gc.ca>
#
# Copyright (c) 2023 Adan Butt
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
import os
import sys
import tempfile

import numpy
from pygeoapi.provider.base import (
    BaseProvider,
    ProviderInvalidQueryError,
    ProviderNoDataError
)
from pyproj import CRS, Transformer
import xarray
import zarr

LOGGER = logging.getLogger(__name__)
DEFAULT_LIMIT_JSON = 5
MAX_DASK_BYTES = 225000


class HRDPSWEonGZarrProvider(BaseProvider):
    """MSC WEonG Zarr provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.base.BaseProvider
        """
        super().__init__(provider_def)

        try:

            self.name = provider_def['name']
            self.type = provider_def['type']
            self.data = provider_def['data']
            self._data = xarray.open_zarr(self.data)

            self._coverage_properties = self._get_coverage_properties()

            # for coverage providers
            self.axes = self._coverage_properties['dimensions']
            self.crs = self._coverage_properties['crs']
        except KeyError:
            raise RuntimeError('name/type/data are required')

        self.editable = provider_def.get('editable', False)
        self.options = provider_def.get('options')
        self.id_field = provider_def.get('id_field')
        self.uri_field = provider_def.get('uri_field')
        self.x_field = provider_def.get('x_field')
        self.y_field = provider_def.get('y_field')
        self.time_field = provider_def.get('time_field')
        self.title_field = provider_def.get('title_field')
        self.properties = provider_def.get('properties', [])
        self.file_types = provider_def.get('file_types', [])
        self.fields = {}
        self.filename = None

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """
        # Dynammically getting all of the axis names
        all_axis = []
        for coord in self._data.coords:
            try:
                some_coord = self._data[coord].attrs["axis"]
                if some_coord not in all_axis:
                    all_axis.append(some_coord)
            except AttributeError:
                msg = f'{coord} has no axis attribute but is a coordinate.'
                LOGGER.warning(msg)
                pass

        all_variables = []
        for var in self._data.data_vars:
            all_variables.append(var)

        all_dimensions = []
        for dim in self._data.dims:
            all_dimensions.append(dim)

        try:
            size_x = float(abs(self._data.lon[1] - self._data.lon[0]))
        except IndexError:
            size_x = float(abs(self._data.lon[0]))

        try:
            size_y = float(abs(self._data.lat[1] - self._data.lat[0]))
        except IndexError:
            size_y = float(abs(self._data.lat[0]))

        properties = {
            # have to convert values to float and int to serilize into json
            'crs': self._data.attrs['_CRS'],
            'axis': all_axis,
            'extent': {
                'minx': float(self._data.lon.min().values),
                'miny': float(self._data.lat.min().values),
                'maxx': float(self._data.lon.max().values),
                'maxy': float(self._data.lat.max().values),
                'coordinate_reference_system':
                ("http://www.opengis.net/def/crs/ECCC-MSC" +
                    "/-/ob_tran-longlat-weong")
                },
            'size': {
                'width': int(self._data.lon.size),
                'height': int(self._data.lat.size)
                },
            'resolution': {
                'x': size_x,
                'y': size_y
                },
            'variables': all_variables,
            'dimensions': all_dimensions
        }

        return properties

    def _get_parameter_metadata(self, var_name):
        """
        Helper function to derive parameter name and units
        :param var_name: representation of variable name
        :returns: dict of parameter metadata
        """
        parameter = {
            'array_dimensons': None,
            'coordinates': None,
            'grid_mapping': None,
            'long_name': None
            }

        if var_name in self._coverage_properties['variables']:
            parameter['array_dimensons'] = self._data[var_name].dims
            parameter['coordinates'] = self._data[var_name].coords
            parameter['grid_mapping'] = (
                self._data[var_name].attrs['grid_mapping'])
            parameter['units'] = self._data[var_name].attrs['units']
            parameter['long_name'] = self._data[var_name].attrs['long_name']
            parameter['id'] = self._data[var_name].attrs['nomvar'],
            parameter['data_type'] = self._data[var_name].dtype

        return parameter

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        'CIS JSON':https://docs.opengeospatial.org/is/09-146r6/09-146r6.html#46
        """
        a = _gen_domain_axis(self, data=self._data)
        sr = self._coverage_properties['extent']['coordinate_reference_system']
        w = self._coverage_properties['size']['width']
        h = self._coverage_properties['size']['height']

        domainset = {
            'type': 'DomainSetType',
            'generalGrid': {
                'type': 'GeneralGridCoverageType',
                'srsName': sr,
                'axisLabels': a[1],
                'axis': a[0],
                'gridLimits': {
                    'type': 'GridLimitsType',
                    'srsName': sr,
                    'axisLabels': ['i', 'j'],
                    'axis': [
                                {
                                    "type": 'IndexAxisType',
                                    "axisLabel": 'i',
                                    "lowerBound": 0,
                                    "upperBound": w
                                },
                                {
                                    "type": 'IndexAxisType',
                                    "axisLabel": 'j',
                                    "lowerBound": 0,
                                    "upperBound": h
                                }
                            ],
                    }

                }
            }

        return domainset

    def get_coverage_rangetype(self):
        """
        Provide coverage rangetype

        :returns: CIS JSON object of rangetype metadata
        'CIS JSON':https://docs.opengeospatial.org/is/09-146r6/09-146r6.html#46
        """
        # at 0, we are dealing with one variable (1 zarr file per variable)
        var_name = self._coverage_properties['variables'][0]
        parameter_metadata = self._get_parameter_metadata(var_name)

        rangetype = {
            'type': 'DataRecordType',
            'field': [
                {
                    'id': parameter_metadata['id'][0],
                    'type': 'QuantityType',
                    'name': parameter_metadata['long_name'],
                    'encodingInfo': {
                        'dataType': str(parameter_metadata['data_type'])
                    },
                    'definition': parameter_metadata['units'],
                    'uom': {
                        'type': 'UnitReferenceType',
                        'code': parameter_metadata['units']
                    }
                }
            ]
        }

        return rangetype

    def query(self, bbox=[], datetime_=None, subsets={}, format_="json"):
        """
        query the provider

        :returns: dict of 0..n GeoJSON features or coverage data
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime: temporal (datestamp or extent)
        :param format_: data format of output
        #TODO: antimeridian bbox
        """
        var_name = self._coverage_properties['variables'][0]
        var_dims = self._coverage_properties['dimensions']
        query_return = {}
        if not subsets and not bbox and datetime_ is None:
            for i in reversed(range(1, DEFAULT_LIMIT_JSON+1)):
                for dim in var_dims:
                    query_return[dim] = i
                data_vals = self._data[var_name].head(**query_return)
                if format_ == 'zarr':
                    new_dataset = data_vals.to_dataset()
                    new_dataset.attrs['_CRS'] = self.crs
                    return _get_zarr_data_stream(new_dataset)
                elif data_vals.data.nbytes < MAX_DASK_BYTES:
                    return _gen_covjson(self, the_data=data_vals)
        else:
            if subsets:
                for dim, value in subsets.items():
                    if dim in var_dims:
                        if (
                            len(value) == 2 and
                            isinstance(value[0], (int, float)) and
                            isinstance(value[1], (int, float))
                        ):
                            query_return[dim] = slice(value[0], value[1])

                        else:
                            msg = 'values must be well-defined range'
                            LOGGER.error(msg)
                            raise ProviderInvalidQueryError(msg)
                    else:  # redundant check (done in api.py)
                        msg = f'Invalid Dimension (Dimension {dim} not found)'
                        LOGGER.error(msg)
                        raise ProviderInvalidQueryError(msg)

            if bbox:
                # convert bbox projection
                bbox = _convert_bbox_to_crs(bbox, self.crs)
                LOGGER.info(f'bbox: {bbox}')

                if not all([
                    self._coverage_properties['extent']['minx'] < bbox[0] <
                    self._coverage_properties['extent']['maxx'],
                    self._coverage_properties['extent']['miny'] < bbox[1] <
                    self._coverage_properties['extent']['maxy'],
                    self._coverage_properties['extent']['minx'] < bbox[2] <
                    self._coverage_properties['extent']['maxx'],
                    self._coverage_properties['extent']['miny'] < bbox[3] <
                    self._coverage_properties['extent']['maxy']
                ]):
                    msg = 'Invalid bbox (Values must fit coverage extent)'
                    LOGGER.error(msg)
                    raise ProviderNoDataError(msg)
                elif 'lat' in query_return or 'lon' in query_return:
                    msg = (
                          'Invalid subset' +
                          '(Cannot subset by both "lat", "lon" and "bbox")'
                    )
                    LOGGER.error(msg)
                    raise ProviderInvalidQueryError(msg)
                else:
                    query_return['lat'] = slice(bbox[1], bbox[3])
                    query_return['lon'] = slice(bbox[0], bbox[2])

            if datetime_:
                if '/' not in datetime_:  # single date
                    query_return['time'] = datetime_

                else:
                    start_date = datetime_.split('/')[0]
                    end_date = datetime_.split('/')[1]
                    query_return['time'] = slice(start_date, end_date)

        try:
            # is a xarray data-array
            data_vals = self._data[var_name].sel(**query_return)

        except Exception as e:
            msg = f'Invalid query (Error: {e})'
            LOGGER.error(msg)
            raise ProviderInvalidQueryError(msg)

        if data_vals.values.size == 0:
            msg = 'Invalid query: No data found'
            LOGGER.error(msg)
            raise ProviderNoDataError(msg)

        if format_ == 'zarr':
            new_dataset = data_vals.to_dataset()
            new_dataset.attrs['_CRS'] = self.crs
            return _get_zarr_data_stream(new_dataset)

        if 0 not in data_vals.shape:
            d_max = float(data_vals.max())
            d_min = float(data_vals.min())

            if (
                (str(d_max)[0].isnumeric()) and
                (str(d_min)[0].isnumeric()) or
                (
                    (str(d_max)[0] == '-') and
                    (str(d_min)[0] == '-')
                )
            ):
                da_max = str(abs(d_max))
                da_min = str(abs(d_min))

# NOTE: float16 can only represent numbers up to 65504 (+/-)
# NOTE: float16 only has 3 decimal places of precision, but saves memory
                if (da_max[0] != '0') or (da_min[0] != '0'):
                    if float(da_max) <= 65504:
                        data_vals = self._data[var_name].astype('float16')
                        data_vals = data_vals.sel(**query_return)

        if data_vals.data.nbytes > MAX_DASK_BYTES:
            raise ProviderInvalidQueryError(
                'Data size exceeds maximum allowed size'
                )

        return _gen_covjson(self, the_data=data_vals)

    def __repr__(self):
        return '<BaseProvider> {}'.format(self.type)


def _convert_bbox_to_crs(bbox, crs):
    """
    Helper function to convert a bbox to a new crs
    :param bbox: Bounding box (minx, miny, maxx, maxy)
    :param crs: CRS to convert to
    :returns: Bounding box in new CRS (minx, miny, maxx, maxy)
    """

    LOGGER.debug('Old bbox: {bbox}')
    crs_src = CRS.from_epsg(4326)
    crs_dst = CRS.from_wkt(crs)

    to_transform = Transformer.from_crs(crs_src, crs_dst, always_xy=True)

    minx, miny = to_transform.transform(bbox[0], bbox[1])
    maxx, maxy = to_transform.transform(bbox[2], bbox[3])

    LOGGER.debug('New bbox', [minx, miny, maxx, maxy])

    return [minx, miny, maxx, maxy]


def _get_zarr_data_stream(data):
    """
    Helper function to convert a xarray dataset to zip file in memory
    :param data: Xarray dataset of coverage data
    :returns: bytes of zip (zarr) data
    """

    mem_bytes = (
        (os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')) * 0.75
        )

    try:
        with tempfile.SpooledTemporaryFile(
            max_size=int((mem_bytes*mem_bytes)+1), suffix='zip'
        ) as f:
            with tempfile.NamedTemporaryFile() as f2:
                data.to_zarr(zarr.ZipStore(f2.name), mode='w')
                return f2.read()
            LOGGER.info(f'satisfy flake8 tests, there is no need to use {f}')
    except Exception:
        raise ProviderInvalidQueryError(
            'Data size is too large to be processed'
        )


def _gen_domain_axis(self, data):
    """
    Helper function to generate domain axis
    :returns: list of dict of domain axis
    """
    var_name = self._coverage_properties['variables'][0]

    # Dynammically getting all of the axis names
    all_axis = []
    for coord in data.dims:
        all_axis.append(coord)

    # Makes sure axis are in the correct order
    j, k = all_axis.index('lon'), all_axis.index(all_axis[0])
    all_axis[j], all_axis[k] = all_axis[k], all_axis[j]

    j, k = all_axis.index('lat'), all_axis.index(all_axis[1])
    all_axis[j], all_axis[k] = all_axis[k], all_axis[j]

    all_dims = []
    for dim in data.dims:
        all_dims.append(dim)

    j, k = all_dims.index('lon'), all_dims.index(all_dims[0])
    all_dims[j], all_dims[k] = all_dims[k], all_dims[j]

    j, k = all_dims.index('lat'), all_dims.index(all_dims[1])
    all_dims[j], all_dims[k] = all_dims[k], all_dims[j]

    aa = []

    for a, dim in zip(all_axis, all_dims):
        if a == 'T':
            res = ''.join(c for c in (
                str(data[dim].values[1] - data[dim].values[0]))
                if c.isdigit())
            uom = ''.join(c for c in (
                str(data[dim].values[1] - data[dim].values[0]))
                if not c.isdigit())
            aa.append(
                {
                    'type': 'RegularAxisType',
                    'axisLabel': a,
                    'lowerBound': str(data[dim].min().values),
                    'upperBound': str(data[dim].max().values),
                    'uomLabel': uom.strip(),
                    'resolution': float(res)
                })

        else:
            try:
                uom = self._data[var_name][dim].attrs['units']
            except KeyError:
                uom = 'n/a'

            try:
                rez = float(abs(data[dim].values[1] - data[dim].values[0]))
            except IndexError:
                rez = float(abs(data[dim].values[0]))
            aa.append(
                {
                    'type': 'RegularAxisType',
                    'axisLabel': a,
                    'lowerBound': float(data[dim].min().values),
                    'upperBound': float(data[dim].max().values),
                    'uomLabel': uom,
                    'resolution': rez
                })
    return aa, all_dims


def _gen_covjson(self, the_data):
    """
    Helper function to Generate coverage as CoverageJSON representation
    :param the_data: xarray dataArray from query
    :returns: dict of CoverageJSON representation
    """

    LOGGER.debug('Creating CoverageJSON domain')
    numpy.set_printoptions(threshold=sys.maxsize)
    props = self._coverage_properties
    var_name = self._coverage_properties['variables'][0]
    parameter_metadata = self._get_parameter_metadata(var_name)

    cov_json = {
        'type': 'CoverageType',
        'domain': {
            'type': 'DomainType',
            'domainType': 'Grid',
            'axes': {
                'x': {
                    'start': float(the_data.lon.min().values),
                    'stop': float(the_data.lon.max().values),
                    'num': int(the_data.lon.size)
                },
                'y': {
                    'start': float(the_data.lat.min().values),
                    'stop': float(the_data.lat.max().values),
                    'num': int(the_data.lat.size)
                },
                't': {
                    'start': str(the_data.time.min().values),
                    'stop': str(the_data.time.max().values),
                    'num': int(the_data.time.size)
                }
            },
            'referencing': [{
                'coordinates': ['x', 'y'],
                'system': {
                    'type': 'GeographicCRS',
                    'id': props['extent']['coordinate_reference_system']
                }
            }]
        }
    }

    parameter = {
        parameter_metadata['id'][0]: {
            'type': 'Parameter',
            'description': {
                'en': parameter_metadata['long_name']
            },
            'unit': {
                'symbol': parameter_metadata['units']
            },
            'observedProperty': {
                'id': parameter_metadata['id'][0],
                'label': {
                    'en': parameter_metadata['long_name']
                }
            }
        }
    }

    cov_json['parameters'] = parameter

    the_range = {
        parameter_metadata['id'][0]: {
                                        'type': 'NdArray',
                                        'dataType': str(the_data.dtype),
                                        'axisNames': the_data.dims,
                                        'shape': the_data.shape
                                        }
    }

    if 0 in the_data.shape:
        raise ProviderInvalidQueryError(
            'No data found. Pass in correct (exact) parameters.'
        )

    else:
        the_range[parameter_metadata['id'][0]]['values'] = (
                the_data.data.flatten().compute().tolist()
            )

    cov_json['ranges'] = the_range

    LOGGER.info(cov_json)

    return cov_json
