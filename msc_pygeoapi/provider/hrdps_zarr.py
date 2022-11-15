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
import shutil
import tempfile
import numpy
import xarray
import sys
import zarr
import dask
import pandas
import os
import zipfile
from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderNoDataError,
                                    ProviderQueryError)

LOGGER = logging.getLogger(__name__)
DEFAULT_LIMIT = 10
MAX_ZARR_GB_SIZE = 1

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
            #self.data = glob(f"{provider_def['data']}/*.zarr")
            #self.data= glob(f'{provider_def["data"]}/*.zarr')
            self.data = provider_def['data']
            #self._data = xarray.open_mfdataset(self.data, engine='zarr')
            self._data = xarray.open_zarr(self.data)

            LOGGER.info(self._data)
            self._coverage_properties = self._get_coverage_properties()

            # for coverage providers
            self.axes = self._coverage_properties['dimensions']
            self.crs = self._coverage_properties['crs']
            #self.num_bands = None #TODO Am I setting the num_bands as the number of variables?

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
            'crs': self._data.attrs["_CRS"],
            'axis': all_axis,
            'extent': {
                'minx': float(self._data.lon.min().values),
                'miny': float(self._data.lat.min().values),
                'maxx': float(self._data.lon.max().values),
                'maxy': float(self._data.lat.max().values),
                'coordinate_reference_system': "http://www.opengis.net/def/crs/ECCC-MSC/-/ob_tran-longlat-weong"  #TODO: Is this the right link?  
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
                'axisLabels': self.axes,#self.axes
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
        #at 0 becuase we are only dealing with one variable (thats the way the data is structured, 1 zarr file per variable)
        #TODO: make this more general (for multiple variables, run a for loop)
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
        #TODO: add 'application/zip' for mimetype under format in the config file
        #NOTE: do we want to add if query will use 'method' 'nearest' when not given a range to slice (given points instead)
        #NOTE: If 'lat' and 'lon' in 'subsets' and 'bbox' exists, throws ERROR
        #NOTE: format_ = zarr, will return a zip file containg zarr not a zarr itself
        #TODO: antimeridian bbox
        #TODO: What is the deafult limit of dataset (set it to a first 10 values of the dataset along each dimension)
        """
        var_name = self._coverage_properties['variables'][0]
        var_metadata = self._get_parameter_metadata(var_name)
        var_dims = self._coverage_properties['dimensions']
        var_time = self._data[var_name]["time"].values

        query_return = {}
        #return LOGGER.error(type(datetime_), datetime_)
        if subsets == {} and bbox == [] and datetime_ == None:
            for dim in var_dims:
                query_return[dim] = DEFAULT_LIMIT
            data_vals = self._data[var_name].head(**query_return)
            #data_vals = self._data[var_name]

        else:

            if subsets != {}:
                for dim, value in subsets.items():
                    if dim in var_dims:
                        if len(value) == 2 and (value[0] is int or float) and (value[1] is int or float):
                            query_return[dim] = slice(value[0], value[1])
                        #elif len(value) == 1:
                            #query_return[dim] = value[0]
                        else:
                            msg = "Invalid subset value, values must be well-defined range"
                            LOGGER.error(msg)
                            raise Exception(msg)
                    else: #redundant check (done in api.py)
                        msg = f"Invalid Dimension name (Dimension {dim} not found)"
                        LOGGER.error(msg)
                        raise Exception(msg)
                #data_vals = _nummpyarray_to_json(self._data[var_name].sel(**slice_coords).values)


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
                #data_vals = self._data[var_name].sel(**query_return)
            if datetime_ is not None:
            
                if '/' not in datetime_: #single date
                    query_return["time"] = datetime_
                    #data_vals = self._data[var_name].sel(**query_return)
                else:
                    start_date = datetime_.split('/')[0]
                    end_date = datetime_.split('/')[1]
                    query_return["time"] = slice(start_date, end_date)
            one_small_ds = self._data[var_name].isel(lat=0, lon=0, time=slice(0,1), level = 0)

            #is a xarray data-array
            data_vals = self._data[var_name].sel(**query_return)
        #return LOGGER.error("the type",type(data_vals))

        if format_ == "zarr":
            #new_dataset = self._data[var_name].to_dataset()
            new_dataset = data_vals.to_dataset()
            data_size = sys.getsizeof(new_dataset)
            MB_data_size = data_size / 1048576
            if MB_data_size > MAX_ZARR_GB_SIZE:
                msg = "Data size too large to return as zarr"
                LOGGER.error(msg)
                raise Exception(msg)
            else:
                return _get_zarr_data(new_dataset)

        lst_of_dataJSON = []
        for i in _gennumpy(data_vals.values):
            lst_of_dataJSON.append(i)
            


        dict_to_return = {

            "type": "Coverage",
            "domain": self.get_coverage_domainset(),
            "range": self.get_coverage_rangetype(),
            "properties" : self._coverage_properties,
            #"data values":  _nummpyarray_to_json(data_vals.values)
            "data values":  lst_of_dataJSON
            #"data values" : data_vals.values.tolist()
            #"data values": data_vals.to_dict()['data']
        }
        return dict_to_return


        raise NotImplementedError()

        

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




def _get_zarr_data(data):
    """
    Helper function to convert a xarray dataset to zip of zarr
       Returns bytes to read from Zarr directory zip
       :param data: Xarray dataset of coverage data

       :returns: byte array of zip data
       """

    #TODO: set the temp directory path
    with tempfile.TemporaryDirectory(dir='/users/dor/afsw/adb/ADANtmp') as tmp_dir:
        data.to_zarr(f'{tmp_dir}/final.zarr', mode='w')
        #_zip_dir(f'{tmp_dir}', root_dir = f'{tmp_dir}.zarr')
        shutil.make_archive(f'{tmp_dir}/finally', 'zip', f'{tmp_dir}/final.zarr')
        #TODO: _CRS missing becuase that needs to be added to the .zattrs file for variables
        return open(f'{tmp_dir}/finally.zip', 'rb').read()

def _nummpyarray_to_json(data_array):
    """
    Helper function to convert numpy array to json
    Converts numpy array to list (which is json serializable)
    :param data: numpy array
    :returns: list
    """

    lst_to_return = []
    #checks to make sure values exist in the array
    if 0 in data_array.shape:
        return lst_to_return
    data_len = len(data_array)
    for i in range(data_len):
        lst_to_return.append(data_array[0].tolist())
        data_array = numpy.delete(data_array, 0)
    return lst_to_return


def _gennumpy(data_array):
    """
    Helper function to convert numpy array to json
    Converts numpy array to list (which is json serializable)
    :param data: numpy array
    :returns: generator
    """

    #checks to make sure values exist in the array
    if 0 in data_array.shape:
        return []
    data_len = len(data_array)
    for i in range(data_len):
        yield (data_array[0].tolist())
        data_array = numpy.delete(data_array, 0)
    