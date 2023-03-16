# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#          Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2022 Louis-Philippe Rousseau-Lambert
# Copyright (c) 2022 Tom Kralidis
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
class CanGRDProvider(BaseProvider):
    """CanGRD Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cangrdrasterio.CanGRDProvider
        """

        super().__init__(provider_def)

        try:
            self._data = rasterio.open(self.data)
            self._coverage_properties = self._get_coverage_properties()
            self.axes = self._coverage_properties['axes']
            if 'season' in self.data:
                self.axes.append('season')
            self.crs = self._coverage_properties['bbox_crs']
            self.num_bands = self._coverage_properties['num_bands']
            # list of variables are not in metadata
            # we need to have them in the code
            self.fields = ['tmean', 'tmax', 'tmin', 'pcp']
            self.native_format = provider_def['format']['name']
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
                'uomLabel': '',
                'resolution': 1
            }

        if 'trend' not in self.data:
            file_path = pathlib.Path(self.data).parent.resolve()
            file_path_ = glob.glob(os.path.join(file_path, '*TMEAN*'))
            file_path_.sort()
            begin_file, end_file = file_path_[0], file_path_[-1]

            if 'monthly' not in self.data:
                begin = search('_{:d}.tif', begin_file)[0]
                end = search('_{:d}.tif', end_file)[0]
                time_axis['uomLabel'] = 'year'
            else:
                begin = search('_{:d}-{:d}.tif', begin_file)
                begin = '{}-{}'.format(begin[0], str(begin[1]).zfill(2))

                end = search('_{:d}-{:d}.tif', end_file)
                end = '{}-{}'.format(end[0], str(end[1]).zfill(2))
                time_axis['uomLabel'] = 'month'

            time_axis['lowerBound'] = begin
            time_axis['upperBound'] = end
            new_axis_name.append('time')
            new_axis.extend([time_axis])

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

    def get_coverage_rangetype(self, *args, **kwargs):
        """
        Provide coverage rangetype
        :returns: CIS JSON object of rangetype metadata
        """

        rangetype = {
            'type': 'DataRecord',
            'field': []
        }

        dtype = self._data.dtypes[0]
        nodataval = self._data.nodatavals[0]

        var_dict = {'TMEAN': {'units': '[C]',
                              'name': 'Mean temperature [C]',
                              'id': 'tmean'},
                    'TMAX': {'units': '[C]',
                             'name': 'Maximum temperature [C]',
                             'id': 'tmax'},
                    'TMIN': {'units': '[C]',
                             'name': 'Minimum temperature [C]',
                             'id': 'tmin'},
                    'PCP': {'units': '[%]',
                            'name': 'Total precipitation [%]',
                            'id': 'pcp'},
                    }

        if 'trend' in self.data:
            var_key = ['TMEAN', 'PCP']
        else:
            var_key = var_dict.keys()

        for var in var_key:
            rangetype['field'].append({
                'id': var_dict[var]['id'],
                'type': 'Quantity',
                'name': var_dict[var]['name'],
                'encodingInfo': {
                    'dataType': 'http://www.opengis.net/def/dataType/OGC/0/{}'.format(dtype)  # noqa
                },
                'nodata': nodataval,
                'uom': {
                    'id': 'http://www.opengis.net/def/uom/UCUM/{}'.format(
                         var_dict[var]['units']),
                    'type': 'UnitReference',
                    'code': var_dict[var]['units']
                },
                '_meta': {
                    'tags': {
                        'long_name': var_dict[var]['name']
                    }
                }
            })

        return rangetype

    def query(self, properties=['TMEAN'], subsets={}, bbox=[],
              datetime_=None, format_='json', **kwargs):
        """
        Extract data from collection collection
        :param properties: variable
        :param subsets: dict of subset names with lists of ranges
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param format_: data format of output
        :returns: coverage data as dict of CoverageJSON or native format
        """

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
            crs_dest = self._data.crs

            if crs_src == crs_dest:
                LOGGER.debug('source bbox CRS and data CRS are the same')
                shapes = [{
                   'type': 'Polygon',
                   'coordinates': [[
                       [minx, miny],
                       [minx, maxy],
                       [maxx, maxy],
                       [maxx, miny],
                       [minx, miny],
                   ]]
                }]
            else:
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

        if properties[0].upper() != 'TMEAN':
            var = properties[0].upper()
            try:
                self.data = self.get_file_list(var)[-1]
            except IndexError as err:
                LOGGER.error(err)
                raise ProviderQueryError(err)

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

        if datetime_ and 'trend' in self.data:
            msg = 'Datetime is not supported for trend'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        date_file_list = False

        if datetime_:
            if '/' not in datetime_:
                if 'month' in self.data:
                    month = search('_{:d}-{:d}.tif', self.data)
                    period = '{}-{}'.format(month[0], str(month[1]).zfill(2))
                    self.data = self.data.replace(str(month), str(datetime_))
                else:
                    period = search('_{:d}.tif', self.data)[0]
                self.data = self.data.replace(str(period), str(datetime_))
            else:
                date_file_list = self.get_file_list(properties[0].upper(),
                                                    datetime_)
                args['indexes'] = list(range(1, len(date_file_list) + 1))

        if not os.path.isfile(self.data):
            msg = 'No such file'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        with rasterio.open(self.data) as _data:
            LOGGER.debug('Creating output coverage metadata')
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
                        indexes=None)
                except ValueError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError(err)

                out_meta.update({'driver': self.native_format,
                                 'height': out_image.shape[1],
                                 'width': out_image.shape[2],
                                 'transform': out_transform})
            else:  # no spatial subset
                LOGGER.debug('Creating data in memory with band selection')
                out_image = _data.read(indexes=[1])

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

            self.filename = self.data.split('/')[-1]
            if 'trend' not in self.data and datetime_:
                self.filename = self.filename.split('_')
                self.filename[-1] = '{}.tif'.format(
                    datetime_.replace('/', '-'))
                self.filename = '_'.join(self.filename)

            # CovJSON output does not support multiple bands yet
            # Only the first timestep is returned
            if format_ == 'json':
                if date_file_list:
                    err = 'Date range not yet supported for CovJSON output'
                    LOGGER.error(err)
                    raise ProviderQueryError(err)
                else:
                    LOGGER.debug('Creating output in CoverageJSON')
                    out_meta['bands'] = args['indexes']
                    return self.gen_covjson(out_meta, shapes, out_image)
            else:
                if date_file_list:
                    LOGGER.debug('Serializing data in memory')
                    with MemoryFile() as memfile:

                        out_meta.update(count=len(date_file_list))

                        with memfile.open(**out_meta) as dest:
                            for id, layer in enumerate(date_file_list,
                                                       start=1):
                                with rasterio.open(layer) as src1:
                                    if shapes:  # spatial subset
                                        try:
                                            LOGGER.debug('Clipping data')
                                            out_image, out_transform = \
                                                rasterio.mask.mask(
                                                    src1,
                                                    filled=False,
                                                    shapes=shapes,
                                                    crop=True,
                                                    indexes=1)
                                        except ValueError as err:
                                            LOGGER.error(err)
                                            raise ProviderQueryError(err)
                                    else:
                                        out_image = src1.read(indexes=1)
                                    dest.write_band(id, out_image)

                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()
                else:
                    LOGGER.debug('Serializing data in memory')
                    with MemoryFile() as memfile:
                        with memfile.open(**out_meta) as dest:
                            dest.write(out_image)

                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()

    # TODO: remove once pyproj is updated on bionic
    def gen_covjson(self, metadata, shapes, data):
        """
        Generate coverage as CoverageJSON representation
        :param metadata: coverage metadata
        :param shapes: bbox in the data projection
        :param data: rasterio DatasetReader object
        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')

        # in the file we have http://www.opengis.net/def/crs/OGC/1.3//3995
        # which is not valid, but we can update it to
        # http://www.opengis.net/def/crs/EPSG/0/3995
        self._coverage_properties['bbox_crs'] = \
            'http://www.opengis.net/def/crs/EPSG/0/3995'

        if shapes:
            coordinates = shapes[0]['coordinates'][0]
            minx = coordinates[0][0]
            maxy = coordinates[0][1]
            maxx = coordinates[2][0]
            miny = coordinates[2][1]
        else:
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
                'description': {
                    'en': str(pm['description'])
                },
                'unit': {
                    'symbol': str(pm['unit_label'])
                },
                'observedProperty': {
                    'id': str(pm['observed_property_id']),
                    'label': {
                        'en': str(pm['observed_property_name'])
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

        :param datetime_: datetime from the query
        :param variable: variable from query

        :returns: sorted list of files
        """

        file_path = pathlib.Path(self.data).parent.resolve()
        file_path_ = glob.glob(os.path.join(file_path,
                                            '*{}*'.format(variable)))
        file_path_.sort()

        if datetime_:
            begin, end = datetime_.split('/')

            begin_file_idx = [file_path_.index(
                i) for i in file_path_ if begin in i]
            end_file_idx = [file_path_.index(
                i) for i in file_path_ if end in i]

            query_file = file_path_[begin_file_idx[0]:end_file_idx[0] + 1]

            return query_file
        else:
            return file_path_


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
