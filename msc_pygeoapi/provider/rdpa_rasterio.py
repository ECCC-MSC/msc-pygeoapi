# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#           <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2023 Tom Kralidis
# Copyright (c) 2025 Louis-Philippe Rousseau-Lambert
#
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

from pygeoapi.provider.base import (BaseProvider,
                                    ProviderConnectionError,
                                    ProviderQueryError)
from pygeoapi.provider.rasterio_ import RasterioProvider

LOGGER = logging.getLogger(__name__)


class RDPAProvider(RasterioProvider):
    """RDPA Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.rdpa_rasterio.RDPAProvider
        """

        BaseProvider.__init__(self, provider_def)

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

            # Rasterio does not read the crs and transform function
            # properly from the file, we have to set them manually
            # The CRS is the same for both RDPA resolution
            # the transform array is different for the 15 km and 10 km files
            self.crs = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=249 +x_0=0 +y_0=0 +R=6371229 +units=m +no_defs' # noqa
            if '10km' in self.data:
                self.transform = (-4556441.403315245, 10000.0,
                                  0.0, 920682.1411659503, 0.0, -10000.0)
            else:
                self.transform = (-2618155.4458640157, 15000.0,
                                  0.0, 7508.80818105489, 0.0, -15000.0)
            self._data._crs = self.crs
            self._data._transform = self.transform
            self.num_bands = self._coverage_properties['num_bands']

            self.get_fields()
            self.native_format = provider_def['format']['name']

        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def query(self, properties=[1], subsets={}, bbox=[],
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

        nbits = 16

        bands = properties
        LOGGER.debug(f'Bands: {bands}, subsets: {subsets}')

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
            temp_geom_minup = {"type": "Point", "coordinates": [minx, maxy]}
            temp_geom_maxdown = {"type": "Point", "coordinates": [maxx, miny]}

            min_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                     temp_geom_min)
            minx2, miny2 = min_coord['coordinates']

            max_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                     temp_geom_max)
            maxx2, maxy2 = max_coord['coordinates']

            upleft_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                        temp_geom_minup)
            minx2up, maxy2up = upleft_coord['coordinates']

            downright_coord = rasterio.warp.transform_geom(crs_src, crs_dest,
                                                           temp_geom_maxdown)
            maxx2down, miny2down = downright_coord['coordinates']

            LOGGER.debug(f'Source coordinates: {minx}, {miny}, {maxx}, {maxy}')
            LOGGER.debug(f'Destination coordinates: {minx2}, {miny2}, {maxx2}, {maxy2}')  # noqa

            shapes = [{
                'type': 'Polygon',
                'coordinates': [[
                    [minx2, miny2],
                    [minx2up, maxy2up],
                    [maxx2, maxy2],
                    [maxx2down, miny2down],
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
                try:
                    period = datetime.strptime(
                        datetime_, '%Y-%m-%dT%HZ').strftime('%Y%m%d%H')
                    self.data = [v for v in self.file_list if period in v][0]
                except IndexError as err:
                    msg = 'Datetime value invalid or out of time domain'
                    LOGGER.error(err)
                    raise ProviderQueryError(msg)

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
                        with memfile.open(**out_meta, nbits=nbits) as dest:
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
                        with memfile.open(**out_meta, nbits=nbits) as dest:
                            dest.write(out_image)

                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties
        :returns: `dict` of coverage properties
        """

        properties = super()._get_coverage_properties()

        properties['restime'] = 'YYYY-MM-DDT06Z/PT24H, YYYY-MM-DDT12Z/PT24H'

        # 15km archive, these time values will never change
        if '15km' in self.data:
            if 'APCP-024' in self.data:
                begin = '2011-04-06T12Z'
                end = '2012-10-02T12Z'
            else:
                begin = '2011-04-06T00Z'
                end = '2012-10-03T00Z'
                properties['restime'] = 'PT6H'

        # 10km  RDPA, begin time is always the same
        elif '0700cutoff' in self.data:
            if 'APCP-024-0700' in self.data:
                begin = '2012-10-03T12Z'
                end = self.get_end_time_from_file()[-1]
            elif 'APCP-006-0700' in self.data:
                begin = '2012-10-03T06Z'
                end = self.get_end_time_from_file()[-1]
                properties['restime'] = 'PT6H'

        # 10km preliminary (0100cutoff) data
        else:
            if 'APCP-006-0100' in self.data:
                properties['restime'] = 'PT6H'
            begin, end = self.get_end_time_from_file()

        properties['time_range'] = [begin, end]

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

            file_path_ = glob.glob(os.path.join('/'.join(file_path), f'*{variable}*'))  # noqa
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
