# =================================================================
#
# Authors: Adan Butt <Adan.Butt@ec.gc.ca>
#
# Copyright (c) 2022 Adan Butt
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
import json
from glob import glob
import logging
import tempfile
import xarray
import zarr
import os
import numpy
import sys
import ast


from pygeoapi.provider.base import (BaseProvider)

LOGGER = logging.getLogger(__name__)
DEFAULT_LIMIT_JSON = 5
MAX_DASK_BYTES = 225000

class HRDPSWEonGZarrProvider(BaseProvider):
    """ Zarr Provider """

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
        self.options = provider_def.get('options', None)
        self.id_field = provider_def.get('id_field', None)
        self.uri_field = provider_def.get('uri_field', None)
        self.x_field = provider_def.get('x_field', None)
        self.y_field = provider_def.get('y_field', None)
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
        #Dynammically getting all of the axis names
        all_axis =[]
        for coord in self._data.coords:
            try:
                some_coord = self._data[coord].attrs["axis"]
                if some_coord not in all_axis:
                    all_axis.append(some_coord)
            except AttributeError:
                pass

        all_variables = []
        for var in self._data.data_vars:
            all_variables.append(var)

        all_dimensions = []
        for dim in self._data.dims:
            all_dimensions.append(dim)

        properties = {
            #have to convert values to float and int to serilize into json
            'crs': self._data.attrs["CRS"],
            'axis': all_axis,
            'extent': {
                'minx': float(self._data.lon.min().values),
                'miny': float(self._data.lat.min().values),
                'maxx': float(self._data.lon.max().values),
                'maxy': float(self._data.lat.max().values),
                'coordinate_reference_system': "http://www.opengis.net/def/crs/ECCC-MSC/-/ob_tran-longlat-weong"
                },
            'size': {
                'width': int(self._data.lon.size),
                'height': int(self._data.lat.size)
                },
            'resolution': {
                'x': float(abs(self._data.lon[1] - self._data.lon[0])),
                'y': float(abs(self._data.lat[1] - self._data.lat[0]))
                },
            'variables': all_variables,
            'dimensions': all_dimensions
        }

        return properties


    def _get_parameter_metadata(self,var_name):
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
            parameter['coordinates'] = self._data[var_name].coords # list of coordinate names
            parameter['grid_mapping'] = self._data[var_name].attrs['grid_mapping'] # name of grid mapping variable
            parameter['units'] = self._data[var_name].attrs['units']
            parameter['long_name'] = self._data[var_name].attrs['long_name']
            parameter['id'] = self._data[var_name].attrs['nomvar']

        return parameter

    def get_fields(self):
        """
        Get provider field information (names, types)

        :returns: dict of fields
        """

        raise NotImplementedError()

    def get_data_path(self, baseurl, urlpath, dirpath):
        """
        Gets directory listing or file description or raw file dump

        :param baseurl: base URL of endpoint
        :param urlpath: base path of URL
        :param dirpath: directory basepath (equivalent of URL)

        :returns: `dict` of file listing or `dict` of GeoJSON item or raw file
        """

        raise NotImplementedError()

    def get_metadata(self):
        """
        Provide data/file metadata

        :returns: `dict` of metadata construct (format
                  determined by provider/standard)
        """
        '''the_metadata = {
            'name': self.name,
            'type': self.type,
            'coverage properties': self._coverage_properties
        }'''


        raise NotImplementedError()

    def get(self, identifier):
        """
        query the provider by id

        :param identifier: feature id

        :returns: dict of single GeoJSON feature
        """

        raise NotImplementedError()

    def create(self, item):
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        raise NotImplementedError()

    def update(self, identifier, item):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        raise NotImplementedError()

    def delete(self, identifier):
        """
        Deletes an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """

        raise NotImplementedError()

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        'CIS JSON': https://docs.opengeospatial.org/is/09-146r6/09-146r6.html#46
        """
        domainset = {
            'type': 'DomainSetType',
            'generalGrid': {
                'type': 'GeneralGridCoverageType',
                'srsName': self._coverage_properties['extent']['coordinate_reference_system'],
                'axisLabels': self.axes,
                'axis': [
                    {"type":"IndexAxisType","axisLabel":'x',"lowerBound": self._coverage_properties['extent']['minx'], "upperBound": self._coverage_properties['extent']['maxx'], "resolution": self._coverage_properties['resolution']['x'] }, #for extent and resolution
                    {"type":"IndexAxisType","axisLabel": 'y',"lowerBound": self._coverage_properties['extent']['miny'], "upperBound": self._coverage_properties['extent']['maxy'], "resolution": self._coverage_properties['resolution']['x'] }
                ],
                'gridLimits': {
                    'type': 'GridLimits',
                    'axis': [
                        {"upperBound": self._coverage_properties['size']['width']}, #for width and height
                        {"upperBound": self._coverage_properties['size']['height']}],
                    }

                }
            }
            
        

        return domainset

        

        

    def get_coverage_rangetype(self):
        """
        Provide coverage rangetype

        :returns: CIS JSON object of rangetype metadata
        'CIS JSON': https://docs.opengeospatial.org/is/09-146r6/09-146r6.html#46
        """
        global MAX_DASK_BYTES
        #at 0 becuase we are only dealing with one variable (thats the way the data is structured, 1 zarr file per variable)
        var_name = self._coverage_properties['variables'][0]
        parameter_metadata = self._get_parameter_metadata(var_name)
        
        rangetype = {
            'type': 'DataRecordType',
            'field': [
                {
                    'id': parameter_metadata['id'],
                    'name': parameter_metadata['long_name'],
                    'definition': parameter_metadata['units'],
                    'uom': {
                        'type': 'UnitReference',
                        'code': parameter_metadata['units']
                    }
                }
            ]
        }

        return rangetype



    def query(self, bbox=[], datetime_=None, subsets = {}, format_= "json"):
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
        if subsets == {} and bbox == [] and datetime_ == None:
            for i in reversed(range(1,DEFAULT_LIMIT_JSON+1)):
                for dim in var_dims:
                    query_return[dim] = i
                data_vals = self._data[var_name].head(**query_return)
                if format_ == "zarr":
                    new_dataset = data_vals.to_dataset()
                    new_dataset.attrs['CRS'] = self.crs
                    return _get_zarr_data_stream(new_dataset)
                elif data_vals.data.nbytes < MAX_DASK_BYTES:
                    return _gen_covjson(self, the_data=data_vals)
            
            #LOGGER.info("THE INFO:",data_vals.nbytes, data_vals.shape)
            #data_vals = self._data[var_name]

        else:

            if subsets != {}:
                for dim, value in subsets.items():
                    if dim in var_dims:
                        if len(value) == 2 and (value[0] is int or float) and (value[1] is int or float):
                            query_return[dim] = slice(value[0], value[1])

                        else:
                            msg = "Invalid subset value, values must be well-defined range"
                            LOGGER.error(msg)
                            raise Exception(msg)
                    else: #redundant check (done in api.py)
                        msg = f"Invalid Dimension name (Dimension {dim} not found)"
                        LOGGER.error(msg)
                        raise Exception(msg)
                


            if bbox != []:
                if bbox[0] < self._coverage_properties['extent']['minx'] or bbox[1] < self._coverage_properties['extent']['miny'] or bbox[2] > self._coverage_properties['extent']['maxx'] or bbox[3] > self._coverage_properties['extent']['maxy']:
                    msg = "Invalid bounding box (Values must fit within the coverage extent)"
                    LOGGER.error(msg)
                    raise Exception(msg)
                elif "lat" in query_return or "lon" in query_return:
                    msg = "Invalid subset (Cannot subset by both 'lat' and 'lon' and 'bbox')"
                    LOGGER.error(msg)
                    raise Exception(msg)
                else:
                    query_return["lat"] = slice(bbox[1], bbox[3])
                    query_return["lon"] = slice(bbox[0], bbox[2])
                
            if datetime_ is not None:
            
                if '/' not in datetime_: #single date
                    query_return["time"] = datetime_
                    
                else:
                    start_date = datetime_.split('/')[0]
                    end_date = datetime_.split('/')[1]
                    query_return["time"] = slice(start_date, end_date)
            

        #is a xarray data-array
        data_vals = self._data[var_name].sel(**query_return)
            

        if format_ == "zarr":
            new_dataset = data_vals.to_dataset()
            new_dataset.attrs['CRS'] = self.crs
            return _get_zarr_data_stream(new_dataset)

        if 0 not in data_vals.shape:
            d_max = float(data_vals.max())
            d_min = float(data_vals.min())

            if ((str(d_max)[0].isnumeric()) and (str(d_min)[0].isnumeric())) or ((str(d_max)[0] == '-') and (str(d_min)[0] == '-')):
                da_max = str(abs(d_max))
                da_min = str(abs(d_min))

                if (da_max[0] != '0') or (da_min[0] != '0'):
                    if float(da_max) <= 65504: #NOTE: float16 can only represent numbers up to 65504 (+/-), useful info: `numpy.finfo(numpy.float16).max`
                        data_vals = self._data[var_name].astype('float16').sel(**query_return) #NOTE: float16 only has 3 decimal places of precision, but it saves a lot of memory (uses half as much as float32)

        if data_vals.data.nbytes > MAX_DASK_BYTES:
            raise ProviderDataSizeError("Data size exceeds maximum allowed size")

        return _gen_covjson(self,the_data=data_vals)

        

    def _load_and_prepare_item(self, item, identifier=None,
                               raise_if_exists=True):
        """
        Helper function to load a record, detect its idenfier and prepare
        a record item

        :param item: `str` of incoming item data
        :param identifier: `str` of item identifier (optional)
        :param raise_if_exists: `bool` of whether to check if record
                                 already exists

        :returns: `tuple` of item identifier and item data/payload
        """

        identifier2 = None
        msg = None

        LOGGER.debug('Loading data')
        LOGGER.debug('Data: {}'.format(item))
        try:
            json_data = json.loads(item)
        except TypeError as err:
            LOGGER.error(err)
            msg = 'Invalid data'
        except json.decoder.JSONDecodeError as err:
            LOGGER.error(err)
            msg = 'Invalid JSON data'

        if msg is not None:
            raise ProviderInvalidDataError(msg)

        LOGGER.debug('Detecting identifier')
        if identifier is not None:
            identifier2 = identifier
        else:
            try:
                identifier2 = json_data['id']
            except KeyError:
                LOGGER.debug('Cannot find id; trying properties.identifier')
                try:
                    identifier2 = json_data['properties']['identifier']
                except KeyError:
                    LOGGER.debug('Cannot find properties.identifier')

        if identifier2 is None:
            msg = 'Missing identifier (id or properties.identifier)'
            LOGGER.error(msg)
            raise ProviderInvalidDataError(msg)

        if 'geometry' not in json_data or 'properties' not in json_data:
            msg = 'Missing core GeoJSON geometry or properties'
            LOGGER.error(msg)
            raise ProviderInvalidDataError(msg)

        if raise_if_exists:
            LOGGER.debug('Querying database whether item exists')
            try:
                _ = self.get(identifier2)

                msg = 'record already exists'
                LOGGER.error(msg)
                raise ProviderInvalidDataError(msg)
            except ProviderItemNotFoundError:
                LOGGER.debug('record does not exist')

        return identifier2, json_data

    def __repr__(self):
        return '<BaseProvider> {}'.format(self.type)


class ProviderGenericError(Exception):
    """provider generic error"""
    pass


class ProviderConnectionError(ProviderGenericError):
    """provider connection error"""
    pass


class ProviderTypeError(ProviderGenericError):
    """provider type error"""
    pass


class ProviderInvalidQueryError(ProviderGenericError):
    """provider invalid query error"""
    pass


class ProviderQueryError(ProviderGenericError):
    """provider query error"""
    pass


class ProviderItemNotFoundError(ProviderGenericError):
    """provider item not found query error"""
    pass


class ProviderNoDataError(ProviderGenericError):
    """provider no data error"""
    pass


class ProviderNotFoundError(ProviderGenericError):
    """provider not found error"""
    pass


class ProviderVersionError(ProviderGenericError):
    """provider incorrect version error"""
    pass


class ProviderInvalidDataError(ProviderGenericError):
    """provider invalid data error"""
    pass
class ProviderDataSizeError(ProviderGenericError):
    """provider data size error"""
    pass



def _get_zarr_data_stream(data):
    """
    Helper function to convert a xarray dataset to zip file in memory
    :param data: Xarray dataset of coverage data
    :returns: bytes of zip (zarr) data 
    """

    mem_bytes = (os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')) * 0.75

    try:
        with tempfile.SpooledTemporaryFile(max_size= int((mem_bytes*mem_bytes)+1), suffix= 'zip') as f:
            with tempfile.NamedTemporaryFile() as f2:
                data.to_zarr(zarr.ZipStore(f2.name), mode='w')
                return f2.read()
    except:
        try:
            f2.close()
            f.close()
        except:
            pass
        raise ProviderDataSizeError('Data size is too large to be processed')
        
 







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

    minx, miny, maxx, maxy=props['extent']['minx'],props['extent']['miny'], props['extent']['maxx'], props['extent']['maxy']

    cov_json = {
        'type': 'Coverage',
        'domain': {
            'type': 'Domain',
            'domainType': 'Grid',
            'axes': {
                'x': {
                    'start': minx,
                    'stop': maxx,
                    'num': props['size']['width']
                },
                'y': {
                    'start': maxy,
                    'stop': miny,
                    'num': props['size']['height']
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
        f"{parameter_metadata['long_name']}": {
        'type': 'Parameter',
        'description': {
            'en': parameter_metadata['long_name']
        },
        'unit': {
            'symbol': parameter_metadata['units']
        },
        'observedProperty': {
            'id': parameter_metadata['id'],
            'label': {
                'en': parameter_metadata['long_name']
            }
        }
    }}

        
    cov_json['parameters'] = parameter

    the_range = {
        f"{parameter_metadata['long_name']}": {
                                                'type': 'NdArray',
                                                'dataType': str(the_data.dtype),
                                                'axisNames': self._coverage_properties['axis'],
                                                'shape': the_data.shape
                                                }
    }

    if 0 in the_data.shape:
        the_range[f"{parameter_metadata['long_name']}"]['values'] = []
    else:
        the_range[f"{parameter_metadata['long_name']}"]['values'] = the_data.data.flatten().compute().tolist() 

    cov_json['ranges'] = the_range


    return cov_json