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
import logging
import os
from parse import search
from pathlib import Path

from dateutil.relativedelta import relativedelta
import rasterio
from rasterio.io import MemoryFile
import rasterio.mask

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)
from pygeoapi.provider.rasterio_ import (RasterioProvider,
                                         _get_parameter_metadata)

LOGGER = logging.getLogger(__name__)


class CanSIPSProvider(RasterioProvider):
    """RDPA Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cansips_rasterio.CanSIPSProvider
        """

        BaseProvider.__init__(self, provider_def)

        try:
            self.file_list = []
            self.member = []
            self.parameter = ''

            self.var_list = ['AirTemp_AGL-2m',
                             'GeopotentialHeight_ISBL-0500',
                             'PrecipRate_Sfc',
                             'Pressure_MSL',
                             'AirTemp_ISBL-0850',
                             'WaterTemp_Sfc']
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
                'interval': [[1, 40]],
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
                'AirTemp_AGL-2m', var))

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

    def query(self, properties=['AirTemp_AGL-2m'], subsets={}, bbox=[],
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

        try:
            self.member = subsets['member']
        except KeyError:
            self.member = [1]

        args = {
            'indexes': None
        }

        bands = list(map(int, self.member))

        drt_dict = self._coverage_properties['uad']['reference_time']
        dt_begin = self._coverage_properties['time_range'][0]
        month_list = ['00']

        if datetime_:
            if '/' in datetime_:  # Date range scenario
                month_list = []
                reference_time_key = 'reference_time' in subsets
                if reference_time_key:
                    sub_rf = subsets['reference_time'][0]
                    model_year, model_month = sub_rf.split('-')
                else:
                    dt_split = datetime_.split('/')[0]
                    model_year, model_month = dt_split.split('-')

                min_date, max_date = datetime_.split('/')
                min_forecast_year, min_forecast_month = min_date.split('-')
                max_forecast_year, max_forecast_month = max_date.split('-')

                # Compute month differences and generate list
                min_diff = self.get_month_difference(model_year,
                                                     model_month,
                                                     min_forecast_year,
                                                     min_forecast_month)
                max_diff = self.get_month_difference(model_year,
                                                     model_month,
                                                     max_forecast_year,
                                                     max_forecast_month)
                month_list = self.generate_month_list(min_diff, max_diff)

            else:  # Single date scenario
                reference_time_key = 'reference_time' in subsets
                if reference_time_key:
                    sub = subsets['reference_time']
                    model_year, model_month = sub[0].split('-')
                elif datetime_ > dt_begin:
                    drtd = drt_dict['interval']
                    model_year, model_month = drtd[0][-1].split('-')
                else:
                    model_year, model_month = datetime_.split('-')

                forecast_year, forecast_month = datetime_.split('-')
                diff_in_months = self.get_month_difference(model_year,
                                                           model_month,
                                                           forecast_year,
                                                           forecast_month)
                month_list = [str(diff_in_months).zfill(2)]

        else:  # No datetime provided
            model_year, model_month = (
                subsets['reference_time'][0].split('-')
                if 'reference_time' in subsets
                else drt_dict['interval'][0][-1].split('-')
            )

        # Call the function with computed values
        if any(int(i) > 11 for i in month_list):
            msg = 'time interval is invalid'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)
        self.get_file_list(properties[0],
                           f'{model_year}{model_month}',
                           month_list)

        LOGGER.debug('Selecting bands')
        args['indexes'] = bands

        for _file in self.file_list:
            if not os.path.isfile(_file):
                msg = 'No such file'
                LOGGER.error(msg)
                raise ProviderQueryError(msg)

        with rasterio.open(self.file_list[0]) as self._data:

            out_meta = self._data.meta
            out_meta.update(nodata=9999.0)
            tags = [self._data.tags(args['indexes'][0])]

            if len(bbox) > 0:
                minx, miny, maxx, maxy = bbox
            else:
                minx, miny, maxx, maxy = self._data.bounds
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

            if self.options is not None:
                LOGGER.debug('Adding dataset options')
                for key, value in self.options.items():
                    out_meta[key] = value

            try:
                LOGGER.debug('Clipping data with bbox')
                out_image, out_transform = rasterio.mask.mask(
                    self._data,
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
                    err = 'Date range not yet supported for CovJSON'
                    LOGGER.error(err)
                    raise ProviderQueryError(err)
                else:
                    LOGGER.debug('Creating output in CoverageJSON')
                    out_meta['bands'] = [1]
                    self.parameter = _get_parameter_metadata(self._data.driver,
                                                             tags[0])
                    return self.gen_covjson(out_meta, out_image)
            else:
                fn = f'{model_year}{model_month}_MSC_CanSIPS_{properties[0]}_LatLon1.0.grib2' # noqa
                self.filename = fn
                if len(self.file_list) > 1:
                    out_meta.update(count=len(self.file_list))
                    LOGGER.debug('Serializing data in memory')
                    with MemoryFile() as memfile:
                        with memfile.open(**out_meta, nbits=nbits) as dest:
                            # first out_image is used above
                            # no need to reclip it
                            dest.update_tags(1, **tags[0])
                            dest.write_band(1, out_image[0])
                            for id, layer in enumerate(self.file_list[1:],
                                                       start=2):
                                with rasterio.open(layer) as src1:
                                    tags.append(src1.tags(args['indexes'][0]))
                                    if shapes:  # spatial subset
                                        try:
                                            LOGGER.debug('Clipping data')
                                            out_image, out_transform = \
                                                rasterio.mask.mask(
                                                    src1,
                                                    filled=False,
                                                    shapes=shapes,
                                                    crop=True,
                                                    nodata=9999.0,
                                                    indexes=args['indexes'])
                                        except ValueError as err:
                                            LOGGER.error(err)
                                            raise ProviderQueryError(err)
                                    else:
                                        out_image = src1.read(
                                            indexes=args['indexes'])
                                    dest.update_tags(id, **tags[id-1])
                                    dest.write_band(id, out_image[0])
                        # return data in native format
                        LOGGER.debug('Returning data in native format')
                        return memfile.read()
                else:
                    LOGGER.debug('Serializing data in memory')
                    out_meta.update(count=len(args['indexes']))
                    with MemoryFile() as _memfile:
                        with _memfile.open(**out_meta, nbits=nbits) as dst:
                            dst.update_tags(1, **tags[0])
                            print(self.file_list[0])
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
            'interval': [['2023-11', end]],
            'grid': {
                'resolution': restime
                }
        }

        properties['uad'] = domainset

        begin = self.get_time_from_dim(end, 0)
        end = self.get_time_from_dim(end, 12)

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

    def get_file_list(
        self,
        variable: str = '*',
        reference_time: str = '*',
        forecast_months: list = ['00'],
    ) -> list[str]:
        """
        Get list of files for given variable, probability and specified model
        run (if provided)

        :param variable: variable name
        :param reference_time: model run datetime
        :param forecast_months: forecast months for datetime
        :returns: list of files
        """

        LOGGER.debug(
            f'Getting files list for variable: {variable}, reference_time: {reference_time}, forecast_months: {forecast_months}'  # noqa
        )

        # if reference_time is a datetime object, we will format it to match
        # the file name
        if reference_time != '*':
            reference_time = f'{reference_time}*'

        files = []
        for month in forecast_months:
            filter_ = f'{reference_time}_MSC_CanSIPS_{variable}_LatLon1.0_P{month}M.grib2' # noqa

            # Find files for each month and add to the overall list
            month_files = [
                str(file)
                for file in Path(
                    '/data/geomet/local/cansips-archives/100km/forecast/'
                ).rglob(filter_)
            ]
            files.extend(month_files)

        # Sort all collected files
        self.file_list = sorted(files)

        return self.file_list

    def get_end_time_from_file(self):
        """
        Get first and last file of list and set end time from file name

        :returns: list of begin and end time as string
        """

        pattern = '/data/geomet/local/cansips-archives/100km/forecast/{year}/{month}/{model_run}_MSC_CanSIPS_AirTemp_AGL-2m_LatLon1.0_P00M.grib2' # noqa

        begin = search(pattern, self.file_list[0])['model_run']
        begin = f'{begin[:4]}-{begin[4:]}'
        end = search(pattern, self.file_list[-1])['model_run']
        end = f'{end[:4]}-{end[4:]}'

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

    def get_month_difference(self, start_y, start_m, end_y, end_m):
        """Calculate the number of months between two dates."""
        start_date = datetime(int(start_y), int(start_m), 1)
        end_date = datetime(int(end_y), int(end_m), 1)
        final_month1 = (end_date.year - start_date.year) * 12
        final_month2 = (end_date.month - start_date.month)
        return final_month1 + final_month2

    def generate_month_list(self, start_diff, end_diff):
        """Generate a list of month differences as zero-padded strings."""
        return [str(i).zfill(2) for i in range(int(start_diff),
                                               int(end_diff) + 1)]
