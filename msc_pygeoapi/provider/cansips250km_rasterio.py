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

from datetime import date, datetime
import glob
import logging
import os
from parse import search
import pathlib

from dateutil.relativedelta import relativedelta
import numpy as np
import rasterio
from rasterio.io import MemoryFile
import rasterio.mask
from rasterio.transform import from_bounds

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)
from pygeoapi.provider.rasterio_ import (RasterioProvider,
                                         _get_parameter_metadata)

LOGGER = logging.getLogger(__name__)


class CanSIPS250kmProvider(RasterioProvider):
    """RDPA Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cansips_rasterio.CanSIPS250kmProvider
        """

        BaseProvider.__init__(self, provider_def)

        try:
            self.file_list = []
            self.member = []
            self.parameter = ''

            self.var_list = ['TMP_TGL_2m',
                             'HGT_ISBL_0500',
                             'PRATE_SFC_0',
                             'PRMSL_MSL_0',
                             'TMP_ISBL_0850',
                             'WTMP_SFC_0']
            self.var = self.var_list[0]
            self.get_file_list(self.var)

            self.data = self.file_list[0]

            self._data = rasterio.open(self.data)
            self._coverage_properties = self._get_coverage_properties()
            self.axes = self._coverage_properties['axes']
            self.axes.extend(['time', 'reference_time', 'member'])
            self.num_bands = self._coverage_properties['num_bands']
            self.crs = self._coverage_properties['bbox_crs']
            self.get_fields()
            self.native_format = provider_def['format']['name']

        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)

    def get_coverage_domainset(self):
        """
        Provide coverage domainset

        :returns: CIS JSON object of domainset metadata
        """

        domainset = {}
        domainset['member'] = {
                'definition': 'Members - RegularAxis',
                'interval': [[1, 20]],
                'grid': {
                    'resolution': 1
                }
            }

        return domainset

    def get_fields(self):
        """
        Provide coverage schema

        :returns: CIS JSON object of schema metadata
        """

        self._fields = {}

        for var in self.var_list:
            self._data = rasterio.open(self.data.replace(
                'TMP_TGL_2m', var))

            i, dtype, _ = self._data.indexes[0], \
                self._data.dtypes[0], self._data.nodatavals[0]

            LOGGER.debug(f'Determing rangetype for band {i}')

            tags = self._data.tags(i)

            name, units = None, None
            if self._data.units[i-1] is None:
                self.parameter = _get_parameter_metadata(
                    self._data.profile['driver'], self._data.tags(i))
                name = self.parameter['description']
                units = self.parameter['unit_label']

            if 'TMP_ISBL_0850' in var:
                name = 'Temperature [C] at 850 mb'

            dtype2 = dtype
            if dtype.startswith('float'):
                dtype2 = 'number'
            elif dtype.startswith('int'):
                dtype2 = 'integer'

            self._fields[var] = {
                'title': name,
                'type': dtype2,
                '_meta': {
                    'tags': tags
                },
                "x-ogc-unit": units
            }

        return self._fields

    def query(self, properties=['TMP_TGL_2m'], subsets={}, bbox=[],
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

        nbits = 20

        if len(properties) > 1:
            err = 'Only one range-subset value is supported'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        if properties[0] not in self.var_list:
            err = 'Not a supported property (variable)'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        self.get_file_list(properties[0])

        try:
            self.member = subsets['member']
        except KeyError:
            self.member = [1]

        args = {
            'indexes': None
        }

        bands = []

        drt_dict = self._coverage_properties['uad']['reference_time']
        dt_begin = self._coverage_properties['time_range'][0]
        if datetime_:
            if 'reference_time' in subsets:
                year, month = subsets['reference_time'][0].split('-')
            # check if datetime requested later than the latest default time
            elif datetime_.split('/')[0] > dt_begin:
                year, month = drt_dict['interval'][0][-1].split('-')
            else:
                year, month = datetime_.split('/')[0].split('-')
                if month == '01':
                    year = str(int(year) - 1)
                    month = '12'
                else:
                    month = str(int(month) - 1).zfill(2)
            bands = self.get_band_datetime(datetime_, year, month)
        else:
            if 'reference_time' in subsets:
                year, month = subsets['reference_time'][0].split('-')
            else:
                year, month = drt_dict['interval'][0][-1].split('-')
            num_months_1 = 1 + 12 * (self.member[0] - 1)
            num_months_2 = 12 + 12 * (self.member[0] - 1)
            bands = list(range(num_months_1, num_months_2 + 1))

        self.data = self.data.replace('2013', year)
        self.data = self.data.replace('04', month)

        LOGGER.debug('Selecting bands')
        args['indexes'] = bands

        self.data = self.data.replace(
            'TMP_TGL_2m', properties[0])

        if not os.path.isfile(self.data):
            msg = 'No such file'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        with rasterio.Env(GRIB_ADJUST_LONGITUDE_RANGE=True):
            with rasterio.open(self.data) as self._data:
                # Get original transform and bounds
                left, bottom, right, top = self._data.bounds
                width, height = self._data.width, self._data.height

                # Read the raster data
                data = self._data.read()
                self.parameter = _get_parameter_metadata(self._data.driver,
                                                         self._data.tags(1))

                # Shift left half (-180 to 0) and right half (0 to 180)
                mid_idx = width // 2  # Find the middle pixel column
                left_half = data[:, :, mid_idx:]
                right_half = data[:, :, :mid_idx]

                # Pad right_half to match left_half width
                right_half = np.pad(right_half,
                                    ((0, 0), (0, 0), (0, 1)),
                                    mode='constant',
                                    constant_values=np.nan)

                # Merge both halves
                data_shifted = np.concatenate((left_half, right_half), axis=2)
                # **Remove the padding (last column)**
                # data_shifted = data_shifted[:, :, :-1]  # Trim last column

                # New longitude bounds
                new_left, new_right = left - 180, right - 180

                # Create new transform
                new_transform = from_bounds(new_left,
                                            bottom,
                                            new_right,
                                            top,
                                            width,
                                            height)

                # Update profile for output
                profile = self._data.profile
                profile.update(transform=new_transform,
                               nodata=9999.0)

                with MemoryFile() as tmp_memfile:
                    with tmp_memfile.open(**profile, nbits=nbits) as mem_data:
                        mem_data.write(data_shifted)

                    # TODO: uncomment, once we have rasterio 1.3.11
                    # with rasterio.open(self.data) as src:
                    #     LOGGER.debug('Creating output coverage metadata')
                    #     #dst_crs = "EPSG:4326"
                    #     dst_crs = CRS.from_epsg(4326)
                    #     left, bottom, right, top = transform_bounds(src.crs,
                    #                                                 dst_crs,
                    #                                                 *src.bounds)
                    #     dst_bounds=(left, bottom, right, top)

                        out_meta = mem_data.meta

                        if len(bbox) > 0:
                            minx, miny, maxx, maxy = bbox
                        else:
                            minx, miny, maxx, maxy = (-178.75,
                                                      -91.25,
                                                      181.25,
                                                      91.25)
                        shapes = [{
                            'type': 'Polygon',
                            'coordinates': [[
                                [minx, miny],
                                [minx, maxy],
                                [maxx, maxy],
                                [maxx, miny],
                                [minx, miny]
                            ]]
                        }]
                        out_meta['bbox'] = [minx, miny, maxx, maxy]

                        # TODO: uncomment, fix indentation
                        # once we have rasterio 1.3.11
                        # reprojection of the original file
                        # from 0,360 to -180,180
                        # with WarpedVRT(src,
                        #                crs=dst_crs,
                        #                dst_bounds=dst_bounds) as self._data:

                        if self.options is not None:
                            LOGGER.debug('Adding dataset options')
                            for key, value in self.options.items():
                                out_meta[key] = value

                        try:
                            LOGGER.debug('Clipping data with bbox')
                            out_image, out_transform = rasterio.mask.mask(
                                mem_data,
                                filled=False,
                                shapes=shapes,
                                crop=True,
                                nodata=9999.0,
                                indexes=args['indexes'])
                        except ValueError as err:
                            LOGGER.error(err)
                            raise ProviderQueryError(err)

                        out_meta.update({'driver': self.native_format,
                                         'height': out_image.shape[1],
                                         'width': out_image.shape[2],
                                         'transform': out_transform})

                        # CovJSON output does not support multiple bands yet
                        # Only the first timestep is returned
                        if format_ == 'json':

                            if datetime_ and '/' in datetime_:
                                err = 'Date range not supported for CovJSON'
                                LOGGER.error(err)
                                raise ProviderQueryError(err)
                            else:
                                LOGGER.debug('Creating output in CoverageJSON')
                                out_meta['bands'] = [1]
                                return self.gen_covjson(out_meta, out_image)
                        else:
                            LOGGER.debug('Serializing data in memory')
                            out_meta.update(count=len(args['indexes']))
                            self.filename = self.data.split('/')[-1]
                            with MemoryFile() as _memfile:
                                with _memfile.open(**out_meta, nbits=nbits) as dst: # noqa
                                    dst.write(out_image)

                                # return data in native format
                                LOGGER.debug('Returning data in native format')
                                return _memfile.read()

    def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """

        properties = super()._get_coverage_properties()
        domainset = self.get_coverage_domainset()

        restime = 'P1M'

        _, end = self.get_end_time_from_file()

        domainset['reference_time'] = {
            'definition': 'reference_time - Temporal',
            'interval': [['2013-04', end]],
            'grid': {
                'resolution': restime
                }
        }

        properties['uad'] = domainset

        begin = self.get_time_from_dim(end, 1)
        end = self.get_time_from_dim(end, 13)

        properties['time_range'] = [begin, end]
        properties['restime'] = restime

        return properties

    def gen_covjson(self, metadata, data):
        """
        Helper function to normalize coverage properties

        :returns: `dict` of coverage properties
        """

        cj = super().gen_covjson(metadata, data)

        pm = self.parameter
        parameter = {}

        parameter[pm['id']] = {
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

        cj['parameters'] = parameter

        return cj

    def get_file_list(self, variable, datetime_=None):
        """
        Generate list of datetime from the query datetime_

        :param variable: variable from query
        :param datetime_: forecast time from the query

        :returns: True
        """

        try:
            file_path = pathlib.Path(self.data).parent.resolve()

            file_path = str(file_path).split('/')
            file_path[-1] = '*'
            file_path[-2] = '*'

            file_ = f'cansips_forecast_raw_latlon2.5x2.5_{variable}*'
            file_path_ = glob.glob(os.path.join('/'.join(file_path), file_))
            file_path_.sort()

            if datetime_:
                begin, end = datetime_.split('/')

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
        Get first and last file of list and set end time from file name

        :returns: list of begin and end time as string
        """

        pattern = 'TMP_TGL_2m_{}_allmembers.grib2'

        begin = search(pattern, self.file_list[0])[0]
        end = search(pattern, self.file_list[-1])[0]

        return begin, end

    def get_time_from_dim(self, ref_time, months):
        """
        Get last file of list and set end time from file name

        :param ref_time: reference_time as string (yyyy-mm)
        :param months: number of months to add to ref_time

        :returns: relative time as string
        """

        year, month = ref_time.split('-')

        forecast_time = date(int(year), int(month), 1) + \
            relativedelta(months=+months)
        forecast_time = datetime.strftime(forecast_time, '%Y-%m')

        return forecast_time

    def get_months_number(self, possible_time, year, month, datetime_):
        """
        Get the difference in number of months between
        reference_time (year, month) and datetime_

        :param possible_time: list of possible time from dim_refenrence_time
        :param year: year from dim_refenrence_time
        :param month: month from dim_refenrence_time
        :param datetime_: forecast time from the query

        :returns: number of months as integer
        """

        if datetime_ not in possible_time:
            err = 'Not a valid datetime'
            LOGGER.error(err)
            raise ProviderQueryError(err)
        else:
            # from dim_ref_time
            begin_date = datetime(int(year), int(month), 1)
            # from datetime_
            year2, month2 = datetime_.split('-')
            end_date = datetime(int(year2), int(month2), 1)
            num_months = (end_date.year - begin_date.year) \
                * 12 + (end_date.month - begin_date.month)
            return num_months

    def get_band_datetime(self, datetime_, year, month):
        """
        generate list of bands from dim_refenrece_time and datetime_

        :param datetime_: forecast time from the query
        :param year: year from reference_time
        :param month: month from reference_time

        :returns: list of bands
        """

        # making a list of the datetime for the given dim_ref_time
        possible_time = []
        for i in range(1, 13):
            possible_time.append(self.get_time_from_dim(f'{year}-{month}', i))

        if '/' not in datetime_:
            if datetime_ not in possible_time:
                err = 'Not a valid datetime'
                LOGGER.error(err)
                raise ProviderQueryError(err)
            else:
                num_months = self.get_months_number(
                    possible_time, year, month, datetime_)
                return [num_months + 12 * (self.member[0] - 1)]

        else:
            datetime1, datetime2 = datetime_.split('/')
            if datetime1 not in possible_time or \
                    datetime2 not in possible_time:
                err = 'Not a valid datetime'
                LOGGER.error(err)
                raise ProviderQueryError(err)
            num_months_1 = self.get_months_number(
                possible_time, year, month, datetime1)
            num_months_2 = self.get_months_number(
                possible_time, year, month, datetime2)

            num_months_1 = num_months_1 + 12 * (self.member[0] - 1)
            num_months_2 = num_months_2 + 12 * (self.member[0] - 1)
            return (list(range(num_months_1, num_months_2 + 1)))
