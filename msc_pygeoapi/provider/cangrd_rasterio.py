# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#           <louis-philippe.rousseaulambert@ec.gc.ca>
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

import glob
import logging
import os
from parse import search
import pathlib

from pyproj import CRS, Transformer
import rasterio
from rasterio.io import MemoryFile
import rasterio.mask

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)
from pygeoapi.provider.rasterio_ import RasterioProvider

LOGGER = logging.getLogger(__name__)


class CanGRDProvider(RasterioProvider):
    """CanGRD Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cangrdrasterio.CanGRDProvider
        """

        BaseProvider.__init__(self, provider_def)

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

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = super().get_coverage_domainset(self)

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

        dtype, nodataval = self._data.dtypes, self._data.nodatavals

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

    def query(self, range_subset=['TMEAN'], subsets={}, bbox=[],
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

                t = Transformer.from_crs(crs_src, crs_dest, always_xy=True)
                minx2, miny2 = t.transform(minx, miny)
                maxx2, maxy2 = t.transform(maxx, maxy)

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

        if range_subset[0].upper() != 'TMEAN':
            try:
                self.data = self.data.replace('TMEAN', range_subset[0].upper())
            except ValueError as err:
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
        elif datetime_ and 'month' not in self.data:
            if '/' not in datetime_:
                year = search('_{:d}.tif', self.data)[0]
                self.data = self.data.replace(str(year), str(datetime_))
        elif datetime_ and 'month' in self.data:
            if '/' not in datetime_:
                month = search('_{:d}-{:d}.tif', self.data)
                month = '{}-{}'.format(month[0], str(month[1]).zfill(2))
                self.data = self.data.replace(str(month), str(datetime_))

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

            LOGGER.debug('Serializing data in memory')
            with MemoryFile() as memfile:
                with memfile.open(**out_meta) as dest:
                    dest.write(out_image)

                if format_ == 'json':
                    LOGGER.debug('Creating output in CoverageJSON')
                    out_meta['bands'] = args['indexes']
                    return self.gen_covjson(out_meta, out_image)

                else:  # return data in native format
                    LOGGER.debug('Returning data in native format')
                    return memfile.read()
