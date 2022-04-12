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

from datetime import date, datetime
import glob
import logging
import os
from parse import search
import pathlib

from dateutil.relativedelta import relativedelta
import rasterio
from rasterio.io import MemoryFile
import rasterio.mask

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError)

LOGGER = logging.getLogger(__name__)


# TODO: use RasterioProvider once pyproj is updated on bionic
class CanSIPSProvider(BaseProvider):
    """RDPA Provider"""

    def __init__(self, provider_def):
        """
        Initialize object
        :param provider_def: provider definition
        :returns: pygeoapi.provider.cansips_rasterio.CanSIPSProvider
        """

        super().__init__(provider_def)

        try:
            self.file_list = []
            self.member = []

            self.var = 'cansips_forecast_raw_latlon2.5x2.5_TMP_TGL_2m_'
            self.get_file_list(self.var)

            self.data = self.file_list[0]

            self.var_list = ['cansips_forecast_raw_latlon2.5x2.5_TMP_TGL_2m',
                             'cansips_forecast_raw_latlon2.5x2.5_HGT_ISBL_0500', # noqa
                             'cansips_forecast_raw_latlon2.5x2.5_PRATE_SFC_0',
                             'cansips_forecast_raw_latlon2.5x2.5_PRMSL_MSL_0',
                             'cansips_forecast_raw_latlon2.5x2.5_TMP_ISBL_0850', # noqa
                             'cansips_forecast_raw_latlon2.5x2.5_WTMP_SFC_0']

            self._data = rasterio.open(self.data)
            self._coverage_properties = self._get_coverage_properties()
            self.axes = self._coverage_properties['axes']
            self.axes.extend(['time', 'dim_reference_time', 'member'])
            self.num_bands = self._coverage_properties['num_bands']
            self.crs = self._coverage_properties['bbox_crs']
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
                'uomLabel': 'month',
                'resolution': 1
            }

        dim_ref_time_axis = {
                'type': 'RegularAxis',
                'axisLabel': 'dim_reference_time',
                'lowerBound': '2013-04',
                'upperBound': '',
                'uomLabel': 'month',
                'resolution': 1
            }

        member_axis = {
                'type': 'RegularAxis',
                'axisLabel': 'member',
                'lowerBound': 20,
                'upperBound': 1,
                'uomLabel': '',
                'resolution': 1
            }

        _, end = self.get_end_time_from_file()
        dim_ref_time_axis['upperBound'] = end

        time_axis['lowerBound'] = self.get_time_from_dim(end, 1)
        time_axis['upperBound'] = self.get_time_from_dim(end, 13)

        new_axis_name.extend(['time', 'dim_reference_time', 'member'])
        new_axis.extend([time_axis, dim_ref_time_axis, member_axis])

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

        for var in self.var_list:

            self._data = rasterio.open(self.data.replace(
                'cansips_forecast_raw_latlon2.5x2.5_TMP_TGL_2m', var))

            i, dtype, nodataval = self._data.indexes[0], \
                self._data.dtypes[0], self._data.nodatavals[0]

            LOGGER.debug('Determing rangetype for band {}'.format(i))

            tags = self._data.tags(i)
            keys_to_remove = ['GRIB_FORECAST_SECONDS',
                              'GRIB_IDS',
                              'GRIB_PDS_TEMPLATE_ASSEMBLED_VALUES',
                              'GRIB_REF_TIME',
                              'GRIB_VALID_TIME']

            for keys in keys_to_remove:
                tags.pop(keys)

            name, units = None, None
            if self._data.units[i-1] is None:
                parameter = _get_parameter_metadata(
                    self._data.profile['driver'], self._data.tags(i))
                name = parameter['description']
                units = parameter['unit_label']

            if 'TMP_ISBL_0850' in var:
                name = 'Temperature [C] at 850 mb'

            rangetype['field'].append({
                'id': self.var_list.index(var) + 1,
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
                    'tags': tags
                }
            })

        return rangetype

    def query(self, range_subset=[1], subsets={'member': [1]}, bbox=[],
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

        nbits = 20

        if len(range_subset) > 1:
            err = 'Only one range-subset value is supported'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        range_subset[0] = int(range_subset[0])
        try:
            var_list = self.var_list[range_subset[0] - 1]
        except IndexError as err:
            LOGGER.error(err)
            raise ProviderQueryError(err)

        self.get_file_list(var_list)
        self.member = subsets['member']

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

            LOGGER.debug('Source coordinates: {}'.format(
                [minx, miny, maxx, maxy]))

            # because cansips long is from 0 to 360
            minx += 180
            maxx += 180

            LOGGER.debug('Destination coordinates: {}'.format(
                [minx, miny, maxx, maxy]))

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

        bands = []

        if 'dim_reference_time' in subsets:
            year, month = subsets['dim_reference_time'][0].split('-')
        else:
            year, month = self.get_latest_dim_reference_time()

        self.data = self.data.replace('2013', year)
        self.data = self.data.replace('04', month)

        if datetime_:
            bands = self.get_band_datetime(datetime_, year, month)
        else:
            num_months_1 = 1 + 12 * (self.member[0] - 1)
            num_months_2 = 12 + 12 * (self.member[0] - 1)
            bands = list(range(num_months_1, num_months_2 + 1))

        LOGGER.debug('Selecting bands')
        args['indexes'] = bands

        var = self.var_list[range_subset[0] - 1]
        self.data = self.data.replace(
            'cansips_forecast_raw_latlon2.5x2.5_TMP_TGL_2m', var)

        if not os.path.isfile(self.data):
            msg = 'No such file'
            LOGGER.error(msg)
            raise ProviderQueryError(msg)

        with rasterio.open(self.data) as self._data:
            LOGGER.debug('Creating output coverage metadata')

            out_meta = self._data.meta

            if self.options is not None:
                LOGGER.debug('Adding dataset options')
                for key, value in self.options.items():
                    out_meta[key] = value

            if shapes:  # spatial subset
                try:
                    LOGGER.debug('Clipping data with bbox')
                    out_image, out_transform = rasterio.mask.mask(
                        self._data,
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
                out_image = self._data.read(indexes=args['indexes'])

            if bbox:
                out_meta['bbox'] = [bbox[0], bbox[1], bbox[2], bbox[3]]
            elif shapes:
                out_meta['bbox'] = [
                    subsets[x][0], subsets[y][0],
                    subsets[x][1], subsets[y][1]
                ]
            else:
                out_meta['bbox'] = [
                    self._data.bounds.left,
                    self._data.bounds.bottom,
                    self._data.bounds.right,
                    self._data.bounds.top
                ]

            out_meta['units'] = self._data.units

            self.filename = self.data.split('/')[-1]

            # CovJSON output does not support multiple bands yet
            # Only the first timestep is returned
            if format_ == 'json':

                if '/' in datetime_:
                    err = 'Date range not yet supported for CovJSON output'
                    LOGGER.error(err)
                    raise ProviderQueryError(err)
                else:
                    LOGGER.debug('Creating output in CoverageJSON')
                    out_meta['bands'] = [1]
                    return self.gen_covjson(out_meta, out_image)
            else:
                LOGGER.debug('Serializing data in memory')
                out_meta.update(count=len(args['indexes']))
                with MemoryFile() as memfile:
                    with memfile.open(**out_meta, nbits=nbits) as dest:
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
        :param datetime_: forecast time from the query

        :returns: True
        """

        try:
            file_path = pathlib.Path(self.data).parent.resolve()

            file_path = str(file_path).split('/')
            file_path[-1] = '*'
            file_path[-2] = '*'

            file_path_ = glob.glob(os.path.join('/'.join(file_path),
                                                '{}*'.format(variable)))
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

        :param ref_time: dim_reference_time as string (yyyy-mm)
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
        dim_reference_time (year, month) and datetime_

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
        :param year: year from dim_refenrence_time
        :param month: month from dim_refenrence_time

        :returns: list of bands
        """

        # making a list of the datetime for the given dim_ref_time
        possible_time = []
        for i in range(1, 13):
            possible_time.append(self.get_time_from_dim(
                '{}-{}'.format(year, month), i))

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

    def get_latest_dim_reference_time(self):
        """
        Get year and month for latests available
        dim_reference_time

        :returns: [year, month] as string
        """

        dict = self.coverage['generalGrid']['axis']

        drt_dict = next(item for item in dict if item[
            "axisLabel"] == "dim_reference_time")

        return drt_dict['upperBound'].split('-')


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
