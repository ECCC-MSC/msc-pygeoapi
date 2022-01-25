# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#           <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2022 Louis-Philippe Rousseau-Lambert
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
import glob
import logging
import os
from parse import search
import pathlib

import rasterio
from rasterio.crs import CRS
from rasterio.io import MemoryFile
import rasterio.mask

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)

LOGGER = logging.getLogger(__name__)


# TODO: use RasterioProvider once pyproj is updated on bionic
class RDPAProvider(BaseProvider):
    """RDPA Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cangrdrasterio.CanGRDProvider
        """

        super().__init__(provider_def)

        try:
            self.file_list = []

            pattern = 'CMC_RDPA_{}cutoff'
            self.var = search(pattern, self.data)[0]
            self.get_file_list(self.var)

            if '*' in self.data:
                self.data = self.file_list[-1]

            self._data = rasterio.open(self.data)
            self._coverage_properties = self._get_coverage_properties()
            self.axes = self._coverage_properties['axes']
            self.axes.append('time')
            self.crs = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=249 +x_0=0 +y_0=0 +R=6371229 +units=m +no_defs' # noqa
            self.transform = (-4556441.403315245, 10000.0,
                              0.0, 920682.1411659503, 0.0, -10000.0)
            self._data._crs = self.crs
            self._data._transform = self.transform
            self.num_bands = self._coverage_properties['num_bands']
            self.fields = [str(num) for num in range(1, self.num_bands+1)]
            self.native_format = provider_def['format']['name']

            # Needed to set the variable for each collection
            # We intialize the collection matadata through this function
            self.coverage = self.get_coverage_domainset()
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    # TODO: update domainset initialization with super
    # once pyproj is updated on bionic
    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = {
            'type': 'DomainSet',
            'generalGrid': {
                'type': 'GeneralGridCoverage',
                'srsName': self._coverage_properties['bbox_crs'],
                'axisLabels': [
                    self._coverage_properties['x_axis_label'],
                    self._coverage_properties['y_axis_label']
                ],
                'axis': [{
                    'type': 'RegularAxis',
                    'axisLabel': self._coverage_properties['x_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][0],
                    'upperBound': self._coverage_properties['bbox'][2],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resx']
                }, {
                    'type': 'RegularAxis',
                    'axisLabel': self._coverage_properties['y_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][1],
                    'upperBound': self._coverage_properties['bbox'][3],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resy']
                }],
                'gridLimits': {
                    'type': 'GridLimits',
                    'srsName': 'http://www.opengis.net/def/crs/OGC/0/Index2D',
                    'axisLabels': ['i', 'j'],
                    'axis': [{
                        'type': 'IndexAxis',
                        'axisLabel': 'i',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['width']
                    }, {
                        'type': 'IndexAxis',
                        'axisLabel': 'j',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['height']
                    }]
                }
            },
            '_meta': {
                'tags': self._coverage_properties['tags']
            }
        }

        new_axis_name = []
        new_axis = []

        time_axis = {
                'type': 'RegularAxis',
                'axisLabel': 'time',
                'lowerBound': '',
                'upperBound': '',
                'uomLabel': 'hour',
                'resolution': 24
            }

        # 15km archive, these time values will never change
        if '15km' in self.data:
            if 'APCP-024' in self.data:
                begin = '2011-04-06T12Z'
                end = '2012-10-02T12Z'
            else:
                begin = '2011-04-06T00Z'
                end = '2012-10-03T00Z'
                time_axis['resolution'] = 6

        # 10km  RDPA, begin time is always the same
        elif '0700cutoff' in self.data:
            if 'APCP-024-0700' in self.data:
                begin = '2012-10-03T12Z'
                end = self.get_end_time_from_file()[-1]
            elif 'APCP-006-0700' in self.data:
                begin = '2012-10-03T06Z'
                end = self.get_end_time_from_file()[-1]
                time_axis['resolution'] = 6

        # 10km preliminary (0100cutoff) data
        else:
            if 'APCP-006-0100' in self.data:
                time_axis['resolution'] = 6
            begin, end = self.get_end_time_from_file()

        time_axis['lowerBound'] = begin
        time_axis['upperBound'] = end
        new_axis_name.append('time')
        new_axis.extend([time_axis])

        domainset['generalGrid']['axisLabels'].extend(new_axis_name)
        domainset['generalGrid']['axis'].extend(new_axis)

        return domainset

    # TODO: remove once pyproj is updated on bionic
    def get_coverage_rangetype(self, *args, **kwargs):
        """
        Provide coverage rangetype
        :returns: CIS JSON object of rangetype metadata
        """

        rangetype = {
            'type': 'DataRecord',
            'field': []
        }

        for i, dtype, nodataval in zip(self._data.indexes, self._data.dtypes,
                                       self._data.nodatavals):
            LOGGER.debug('Determing rangetype for band {}'.format(i))

            name, units = None, None
            if self._data.units[i-1] is None:
                parameter = _get_parameter_metadata(
                    self._data.profile['driver'], self._data.tags(i))
                name = parameter['description']
                units = parameter['unit_label']

            rangetype['field'].append({
                'id': i,
                'type': 'Quantity',
                'name': name,
                'encodingInfo': {
                    'dataType': 'http://www.opengis.net/def/dataType/OGC/0/{}'.format(dtype)  # noqa
                },
                'nodata': nodataval,
                'uom': {
                    'id': 'http://www.opengis.net/def/uom/UCUM/{}'.format(
                         units),
                    'type': 'UnitReference',
                    'code': units
                },
                '_meta': {
                    'tags': self._data.tags(i)
                }
            })

        return rangetype

    def query(self, range_subset=[1], subsets={}, bbox=[],
              datetime_=None, format_='json', **kwargs):
        """
        Extract data from collection collection
        :param range_subset: variable
        :param subsets: dict of subset names with lists of ranges
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param format_: data format of output
        :returns: coverage data as dict of CoverageJSON or native format
        """

        bands = range_subset
        LOGGER.debug('Bands: {}, subsets: {}'.format(bands, subsets))

        args = {
            'indexes': None
        }
        shapes = []

        if all([self._coverage_properties['x_axis_label'] in subsets,
                self._coverage_properties['y_axis_label'] in subsets,
                len(bbox) > 0]):
            msg = 'bbox and subsetting by coordinates are exclusive'
            LOGGER.warning(msg)
            raise ProviderQueryError(msg)

        if len(bbox) > 0:
            minx, miny, maxx, maxy = bbox

            crs_src = CRS.from_epsg(4326)
            crs_dest = CRS.from_string(self.crs)

            LOGGER.debug('source bbox CRS and data CRS are different')
            LOGGER.debug('reprojecting bbox into native coordinates')

            temp_geom_min = {"type": "Point", "coordinates": [minx, miny]}
            temp_geom_max = {"type": "Point", "coordinates": [maxx, maxy]}

            min_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                     temp_geom_min)
            minx2, miny2 = min_coord['coordinates']

            max_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                     temp_geom_max)
            maxx2, maxy2 = max_coord['coordinates']

            LOGGER.debug('Source coordinates: {}'.format(
                [minx, miny, maxx, maxy]))
            LOGGER.debug('Destination coordinates: {}'.format(
                [minx2, miny2, maxx2, maxy2]))

            shapes = [{
                'type': 'Polygon',
                'coordinates': [[
                    [minx2, miny2],
                    [minx2, maxy2],
                    [maxx2, maxy2],
                    [maxx2, miny2],
                    [minx2, miny2],
                ]]
            }]

        elif (self._coverage_properties['x_axis_label'] in subsets and
                self._coverage_properties['y_axis_label'] in subsets):
            LOGGER.debug('Creating spatial subset')

            x = self._coverage_properties['x_axis_label']
            y = self._coverage_properties['y_axis_label']

            shapes = [{
               'type': 'Polygon',
               'coordinates': [[
                   [subsets[x][0], subsets[y][0]],
                   [subsets[x][0], subsets[y][1]],
                   [subsets[x][1], subsets[y][1]],
                   [subsets[x][1], subsets[y][0]],
                   [subsets[x][0], subsets[y][0]]
               ]]
            }]

        date_file_list = False

        if datetime_:

            if '/' not in datetime_:
                period = datetime.strptime(datetime_,
                                           '%Y-%m-%dT%HZ').strftime('%Y%m%d%H')
                self.data = [v for v in self.file_list if period in v][0]
            else:
                self.get_file_list(self.var, datetime_)
                date_file_list = self.file_list

        if bands:
            LOGGER.debug('Selecting bands')
            args['indexes'] = list(map(int, bands))

        with rasterio.open(self.data) as _data:
            LOGGER.debug('Creating output coverage metadata')
            _data._crs = self.crs
            _data._transform = self.transform

            out_meta = _data.meta

            if self.options is not None:
                LOGGER.debug('Adding dataset options')
                for key, value in self.options.items():
                    out_meta[key] = value

            if shapes:  # spatial subset
                try:
                    LOGGER.debug('Clipping data with bbox')
                    out_image, out_transform = rasterio.mask.mask(
                        _data,
                        filled=False,
                        shapes=shapes,
                        crop=True,
                        indexes=args['indexes'])
                except ValueError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError(err)

                out_meta.update({'driver': self.native_format,
                                 'height': out_image.shape[1],
                                 'width': out_image.shape[2],
                                 'transform': out_transform})
            else:  # no spatial subset
                LOGGER.debug('Creating data in memory with band selection')
                out_image = _data.read(indexes=args['indexes'])

            if bbox:
                out_meta['bbox'] = [bbox[0], bbox[1], bbox[2], bbox[3]]
            elif shapes:
                out_meta['bbox'] = [
                    subsets[x][0], subsets[y][0],
                    subsets[x][1], subsets[y][1]
                ]
            else:
                out_meta['bbox'] = [
                    _data.bounds.left,
                    _data.bounds.bottom,
                    _data.bounds.right,
                    _data.bounds.top
                ]

            out_meta['units'] = _data.units

            self.filename = self.data.split('/')[-1].replace(
                '*', '')

            # CovJSON output does not support multiple bands yet
            # Only the first timestep is returned
            if format_ == 'json':

                if date_file_list:
                    err = 'Date range not yet supported for CovJSON output'
                    LOGGER.error(err)
                    raise ProviderQueryError(err)
                else:
                    LOGGER.debug('Creating output in CoverageJSON')
                    out_meta['bands'] = [1]
                    return self.gen_covjson(out_meta, out_image)
            else:
                if date_file_list:
                    out_meta.update(count=len(date_file_list))

                    LOGGER.debug('Serializing data in memory')
                    with MemoryFile() as memfile:
                        with memfile.open(**out_meta) as dest:
                            for id, layer in enumerate(date_file_list,
                                                       start=1):
                                with rasterio.open(layer) as src1:
                                    src1._crs = self.crs
                                    src1._transform = self.transform
                                    if shapes:  # spatial subset
                                        try:
                                            LOGGER.debug('Clipping data')
                                            out_image, out_transform = \
                                                rasterio.mask.mask(
                                                    src1,
                                                    filled=False,
                                                    shapes=shapes,
                                                    crop=True,
                                                    indexes=args['indexes'])
                                        except ValueError as err:
                                            LOGGER.error(err)
                                            raise ProviderQueryError(err)
                                    else:
                                        out_image = src1.read(
                                            indexes=args['indexes'])

                                    dest.write_band(id, out_image[0])

                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()
                else:
                    LOGGER.debug('Serializing data in memory')
                    out_meta.update(count=len(args['indexes']))
                    with MemoryFile() as memfile:
                        with memfile.open(**out_meta) as dest:
                            dest.write(out_image)

                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()

    # TODO: remove once pyproj is updated on bionic
    def gen_covjson(self, metadata, data):
        """
        Generate coverage as CoverageJSON representation
        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')
        minx, miny, maxx, maxy = metadata['bbox']

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

        if metadata['bands'] is None:  # all bands
            bands_select = range(1, len(self._data.dtypes) + 1)
        else:
            bands_select = metadata['bands']

        LOGGER.debug('bands selected: {}'.format(bands_select))
        for bs in bands_select:
            pm = _get_parameter_metadata(
                self._data.profile['driver'], self._data.tags(bs))

            parameter = {
                'type': 'Parameter',
                'description': pm['description'],
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

        try:
            for key in cj['parameters'].keys():
                cj['ranges'][key] = {
                    'type': 'NdArray',
                    # 'dataType': metadata.dtypes[0],
                    'dataType': 'float',
                    'axisNames': ['y', 'x'],
                    'shape': [metadata['height'], metadata['width']],
                }
                # TODO: deal with multi-band value output
                cj['ranges'][key]['values'] = data.flatten().tolist()
        except IndexError as err:
            LOGGER.warning(err)
            raise ProviderQueryError('Invalid query parameter')

        return cj

    # TODO: remove once pyproj is updated on bionic
    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties
        :returns: `dict` of coverage properties
        """

        properties = {
            'bbox': [
                self._data.bounds.left,
                self._data.bounds.bottom,
                self._data.bounds.right,
                self._data.bounds.top
            ],
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'bbox_units': 'deg',
            'x_axis_label': 'Long',
            'y_axis_label': 'Lat',
            'width': self._data.width,
            'height': self._data.height,
            'resx': self._data.res[0],
            'resy': self._data.res[1],
            'num_bands': self._data.count,
            'tags': self._data.tags()
        }

        if self._data.crs is not None:
            if self._data.crs.is_projected:
                properties['bbox_crs'] = '{}/{}'.format(
                    'http://www.opengis.net/def/crs/OGC/1.3/',
                    self._data.crs.to_epsg())

                properties['x_axis_label'] = 'x'
                properties['y_axis_label'] = 'y'
                properties['bbox_units'] = self._data.crs.linear_units
                properties['crs_type'] = 'ProjectedCRS'

        properties['axes'] = [
            properties['x_axis_label'], properties['y_axis_label']
        ]

        return properties

    def get_file_list(self, variable, datetime_=None):
        """
        Generate list of datetime from the query datetime_

        :param variable: variable from query
        :param datetime_: datetime from the query

        :returns: True
        """

        try:
            file_path = pathlib.Path(self.data).parent.resolve()

            file_path = str(file_path).split('/')
            file_path[-1] = '*'
            file_path[-2] = '*'

            file_path_ = glob.glob(os.path.join('/'.join(file_path),
                                                '*{}*'.format(variable)))
            file_path_.sort()

            if datetime_:
                begin, end = datetime_.split('/')

                begin = datetime.strptime(begin, '%Y-%m-%dT%HZ').\
                    strftime('%Y%m%d%H')
                end = datetime.strptime(end, '%Y-%m-%dT%HZ').\
                    strftime('%Y%m%d%H')

                begin_file_idx = [file_path_.index(i) for i
                                  in file_path_ if begin in i]
                end_file_idx = [file_path_.index(i) for i
                                in file_path_ if end in i]

                self.file_list = file_path_[
                    begin_file_idx[0]:end_file_idx[0] + 1]
                return True
            else:
                self.file_list = file_path_
                return True

        except ValueError as err:
            LOGGER.error(err)
            return False

    def get_end_time_from_file(self):
        """
        Get last file of list and set end time from file name

        :returns: list of begin and end time as string
        """

        pattern = 'ps10km_{}_000.grib2'

        begin = search(pattern, self.file_list[0])[0]
        begin = datetime.strptime(begin, '%Y%m%d%H').strftime('%Y-%m-%dT%HZ')

        end = search(pattern, self.file_list[-1])[0]
        end = datetime.strptime(end, '%Y%m%d%H').strftime('%Y-%m-%dT%HZ')

        return [begin, end]


# TODO: remove once pyproj is updated on bionic
def _get_parameter_metadata(driver, band):
    """
    Helper function to derive parameter name and units
    :param driver: rasterio/GDAL driver name
    :param band: int of band number
    :returns: dict of parameter metadata
    """

    parameter = {
        'id': None,
        'description': None,
        'unit_label': None,
        'unit_symbol': None,
        'observed_property_id': None,
        'observed_property_name': None
    }

    if driver == 'GRIB':
        parameter['id'] = band['GRIB_ELEMENT']
        parameter['description'] = band['GRIB_COMMENT']
        parameter['unit_label'] = band['GRIB_UNIT']
        parameter['unit_symbol'] = band['GRIB_UNIT']
        parameter['observed_property_id'] = band['GRIB_SHORT_NAME']
        parameter['observed_property_name'] = band['GRIB_COMMENT']

    return parameter
