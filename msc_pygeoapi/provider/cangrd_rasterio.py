# =================================================================
#
# Authors: Louis-Philippe Rousseau-Lambert
#          <louis-philippe.rousseaulambert@ec.gc.ca>
#          Tom Kralidis <tom.kralidis@ec.gc.ca>
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

import glob
import logging
import os
from parse import search
import pathlib

import rasterio
from rasterio.crs import CRS
from rasterio.io import MemoryFile
import rasterio.mask

from pygeoapi.provider.base import (ProviderConnectionError,
                                    ProviderQueryError)

from pygeoapi.provider.rasterio_ import RasterioProvider

LOGGER = logging.getLogger(__name__)


# TODO: use RasterioProvider once pyproj is updated on bionic
class CanGRDProvider(RasterioProvider):
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
            self.native_format = provider_def['format']['name']
            self.get_fields()
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = {}

        if 'season' in self.data:
            domainset['season'] = {
                'definition': 'Seasons - IrregularAxis',
                'interval': [['DJF', 'MAM', 'JJA', 'SON']]
                }

        return domainset

    def get_fields(self):
        """
        Provide coverage rangetype
        :returns: CIS JSON object of rangetype metadata
        """

        self.var_dict = {'TMEAN': {'units': '[C]',
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
            var_key = self.var_dict.keys()

        for var in var_key:
            self.data = self.data.replace('TMEAN*', f'{var}*')
            with rasterio.open(self.data) as _data:

                units = self.var_dict[var]['units']
                dtype = self._data.dtypes[0]
                nodataval = self._data.nodatavals[0]
                dtype2 = dtype

                if dtype.startswith('float'):
                    dtype2 = 'number'
                elif dtype.startswith('int'):
                    dtype2 = 'integer'
                self._fields[self.var_dict[var]['id']] = {
                    'title': self.var_dict[var]['name'],
                    'type': dtype2,
                    "x-ogc-unit": units,
                    '_meta': {
                        'tags': _data.tags(1),
                        'uom': {
                            'id': 'http://www.opengis.net/def/uom/UCUM/{units}', # noqa
                            'type': 'UnitReference',
                            'code': units
                        },
                        'nodata': nodataval,
                    }
                }

        self._fields

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

                LOGGER.debug(f'Source coordinates: {minx}, {miny}, {maxx}, {maxy}')  # noqa
                LOGGER.debug(f'Destination coordinates: {minx2}, {miny2}, {maxx2}, {maxy2}')  # noqa

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
                    period = f'{month[0]}-{month[1]:02}'
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
                self.filename[-1] = f"{datetime_.replace('/', '-')}.tif"
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
                    if len(bbox) > 0:
                        out_meta['bbox'] = [minx2,
                                            miny2,
                                            maxx2,
                                            maxy2]
                    return self.gen_covjson(out_meta, out_image, var)
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

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties
        :returns: `dict` of coverage properties
        """

        properties = super()._get_coverage_properties()

        if 'trend' not in self.data:
            file_path = pathlib.Path(self.data).parent.resolve()
            file_path_ = glob.glob(os.path.join(file_path, '*TMEAN*'))
            file_path_.sort()
            begin_file, end_file = file_path_[0], file_path_[-1]

            if 'monthly' not in self.data:
                begin = search('_{:d}.tif', begin_file)[0]
                end = search('_{:d}.tif', end_file)[0]
                period = 'P1Y'
            else:
                begin = search('_{:d}-{:d}.tif', begin_file)
                begin = f'{begin[0]}-{begin[1]:02}'

                end = search('_{:d}-{:d}.tif', end_file)
                end = f'{end[0]}-{end[1]:02}'
                period = 'P1M'

            properties['restime'] = period
            properties['time_range'] = [begin, end]

        properties['uad'] = self.get_coverage_domainset()

        return properties

    def get_file_list(self, variable, datetime_=None):
        """
        Generate list of datetime from the query datetime_

        :param datetime_: datetime from the query
        :param variable: variable from query

        :returns: sorted list of files
        """

        file_path = pathlib.Path(self.data).parent.resolve()
        file_path_ = glob.glob(os.path.join(file_path, f'*{variable}*'))
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

    def gen_covjson(self, metadata, data, var):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """

        cj = super().gen_covjson(metadata, data)

        pm = self.var_dict[var]
        parameter = {}

        parameter[pm['id']] = {
            'type': 'Parameter',
            'description': {'en': pm['name']},
            'unit': {
                'symbol': pm['units']
            },
            'observedProperty': {
                'id': pm['id'],
                'label': {
                    'en': pm['name']
                }
            }
        }

        cj['parameters'] = parameter

        return cj
