# =================================================================
#
# Authors: Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2025 Etienne Pelletier
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

import logging
from datetime import datetime
from pathlib import Path
import re

from pygeoapi.provider.base import ProviderConnectionError, ProviderQueryError
from pygeoapi.provider.rasterio_ import (
    RasterioProvider,
    _get_parameter_metadata
)
import rasterio
from rasterio.io import MemoryFile
import rasterio.mask

from msc_pygeoapi.env import GEOMET_LOCAL_BASEPATH

LOGGER = logging.getLogger(__name__)

NODATA_VALUE = 9999.0

CANSIPS_ARCHIVES_FORECAST_BASEPATH = (
    f'{GEOMET_LOCAL_BASEPATH}/cansips-archives/100km/forecast/'
)


class CanSIPSProductsProvider(RasterioProvider):
    """CanSIPS Products Provider"""

    def __init__(self, provider_def: dict) -> None:
        """
        Initialize object

        :param provider_def: provider definition
        :returns: `CanSIPSProductsProvider`
        """
        self.filepath_pattern = f'{CANSIPS_ARCHIVES_FORECAST_BASEPATH}/{{year}}/{{month}}/{{YYYYMM}}_MSC_CanSIPS_{{wx_variable}}_{{elevation}}_LatLon1.0_{{period}}.grib2'  # noqa

        # using regex check if P.*M-P.*M in the filename
        if re.search(r'P\d{2}M-P\d{2}M', provider_def['data']):
            self.product_type = 'seasonal'
        else:
            self.product_type = 'monthly'

        self.file_list = self._get_files_list()

        try:
            self.data = self.file_list['AirTemp-ProbAboveNormal'][0].absolute()
        except IndexError:
            raise ProviderConnectionError('No associated data files found.')

        self.variables = [
            'AirTemp-ProbAboveNormal',
            'AirTemp-ProbNearNormal',
            'AirTemp-ProbBelowNormal',
            'PrecipAccum-ProbAboveNormal',
            'PrecipAccum-ProbNearNormal',
            'PrecipAccum-ProbBelowNormal'
        ]
        self._data = rasterio.open(self.data)

        super().__init__(provider_def)
        self._coverage_properties = self._get_coverage_properties()

    def _get_files_list(
        self,
        variable: str = '*',
        probability: str = '*',
        reference_time: datetime | str = '*'
    ) -> dict[str, list[Path]]:
        """
        Get list of files for given variable, probability and specified model
        run (if provided)

        :param variable: variable name
        :param probability: probability name
        :param model_run: model run datetime
        :returns: list of files
        """

        LOGGER.debug(
            f'Getting files list for variable: {variable}, probability: {probability}, reference_time: {reference_time}'  # noqa
        )

        # if reference_time is a datetime object, we will format it to match
        # the file name
        if isinstance(reference_time, datetime):
            reference_time = reference_time.strftime('%Y%m')
        reference_time = f'{reference_time}*' if reference_time != '*' else '*'

        if reference_time == '*':
            variable = f'{variable}*' if variable != '*' else ''

        probability = f'*{probability}*' if probability != '*' else '*'

        filter_ = f'{reference_time}{variable}Prob{probability}.grib2'

        if self.product_type == 'seasonal':
            pattern = re.compile(r'_P\d{2}M-P\d{2}M.grib2')
        else:
            pattern = re.compile(r'_P\d{2}M.grib2')

        files = sorted(
            file_
            for file_ in Path(
                CANSIPS_ARCHIVES_FORECAST_BASEPATH
            ).rglob(filter_)
            if pattern.search(file_.name)
        )

        # create files dict where for each variable, we have a list of files
        files_dict: dict[str, list[Path]] = {}
        for file_ in files:
            variable = file_.name.split('_')[3]
            if variable not in files_dict:
                files_dict[variable] = []
            files_dict[variable].append(file_)

        return files_dict

    def _get_coverage_domainset(self) -> dict:
        """
        Get coverage domain set

        :returns: `dict` of coverage domain set
        """

        LOGGER.debug('Getting coverage domain set')
        domain_set = {}

        start_dt = datetime.strptime(
            self.file_list['AirTemp-ProbAboveNormal'][0].name.split('_')[0],
            '%Y%m'
        )
        end_dt = datetime.strptime(
            self.file_list['AirTemp-ProbAboveNormal'][-1].name.split('_')[0],
            '%Y%m'
        )

        domain_set['reference_time'] = {
            'definition': 'reference_time - Temporal',
            'interval': [
                [start_dt.strftime('%Y-%m'), end_dt.strftime('%Y-%m')]
            ],
            'grid': {'resolution': 'P1M'}
        }

        if self.product_type == 'seasonal':
            domain_set['period'] = {
                'definition': 'Period - IrregularAxis',
                'interval': [
                    'P00M-P02M',
                    'P01M-P03M',
                    'P02M-P04M',
                    'P03M-P05M',
                    'P04M-P06M',
                    'P05M-P07M',
                    'P06M-P08M',
                    'P07M-P09M',
                    'P08M-P10M',
                    'P09M-P11M'
                ]
            }

        if self.product_type == 'monthly':
            domain_set['period'] = {
                'definition': 'Period - IrregularAxis',
                'interval': [
                    'P00M',
                    'P01M',
                    'P02M',
                    'P03M',
                    'P04M',
                    'P05M',
                    'P06M',
                    'P07M',
                    'P08M',
                    'P09M',
                    'P10M',
                    'P11M'
                ]
            }

        return domain_set

    def _get_coverage_properties(self) -> dict:
        """
        Get coverage properties

        :returns: `dict` of coverage properties
        """

        properties = super()._get_coverage_properties()

        LOGGER.debug('Getting coverage temporal dimensions')

        # get file list for AirTemp-ProbAboveNormal variable and get the
        # first file

        properties['uad'] = self._get_coverage_domainset()

        # add uad keys to available coverage axes to allow subsetting
        for key in properties['uad']:
            self.axes.append(key)

        return properties

    def get_fields(self) -> dict:
        """
        Get fields

        :returns: `dict` of fields
        """

        LOGGER.debug('Getting fields')

        for variable in self.variables:
            self._data = rasterio.open(self.file_list[variable][0])
            for i, dtype in zip(self._data.indexes, self._data.dtypes):
                LOGGER.debug(f'Adding field for band {i}')
                i2 = str(variable)

                parameter = _get_parameter_metadata(
                    self._data.profile['driver'], self._data.tags(i)
                )

                tags = self._data.tags(i)

                keys_to_remove = [
                    'GRIB_FORECAST_SECONDS',
                    'GRIB_IDS',
                    'GRIB_PDS_TEMPLATE_ASSEMBLED_VALUES',
                    'GRIB_REF_TIME',
                    'GRIB_VALID_TIME'
                ]

                for key in keys_to_remove:
                    tags.pop(key)

                name = parameter['description']
                units = parameter.get('unit_label')

                dtype2 = dtype
                if dtype.startswith('float'):
                    dtype2 = 'number'
                elif dtype.startswith('int'):
                    dtype2 = 'integer'

                self._fields[variable] = {
                    'title': name,
                    'type': dtype2,
                    '_meta': tags
                }
                if units is not None:
                    self._fields[i2]['x-ogc-unit'] = units

        return self._fields

    def query(
        self,
        properties: list[str] = ['AirTemp-ProbAboveNormal'],
        subsets: dict[str, list[str]] = {},
        bbox: list[float | int] = [],
        datetime_: str | None = None,
        format_: str = 'json',
        **kwargs
    ) -> dict | bytes:
        """
        Query the provider

        :param properties: list of properties to query
        :param subsets: dictionary of subsets
        :param bbox: bounding box
        :param datetime_: query datetime
        :param kwargs: keyword-value pairs to filter the query
        :returns: query result
        """

        if len(properties) > 1:
            err = 'Only a single property value is supported.'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        property_ = properties[0]
        shapes = []

        # get period from subsets, default to first value if not provided
        try:
            self.period = subsets['period'][0]
            if (
                self.period
                not in self._coverage_properties['uad']['period']['interval']
            ):
                err = 'Invalid period value provided.'
                LOGGER.error(err)
                raise ProviderQueryError(err)
        except (KeyError, IndexError):
            self.period = (
                'P00M-P02M' if self.product_type == 'seasonal' else 'P00M'
            )

        # get reference_time from subsets, default to last value if
        # not provided
        try:
            self.reference_time = datetime.strptime(
                subsets['reference_time'][0], '%Y-%m'
            )
        except (KeyError, IndexError):
            if self.product_type == 'seasonal':
                self.reference_time = datetime.strptime(
                    self._coverage_properties['uad']['reference_time']['interval'][0][-1], # noqa
                    '%Y-%m'
                )
            else:
                self.reference_time = datetime.strptime('2025-03', '%Y-%m')
        except ValueError:
            err = 'Invalid reference_time value provided.'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        # piece together the filepath based on the requested
        # property reference_time and period
        filepath_elements = {
            'year': self.reference_time.strftime('%Y'),
            'month': self.reference_time.strftime('%m'),
            'YYYYMM': self.reference_time.strftime('%Y%m'),
            'wx_variable': property_,
            'elevation': 'AGL-2m' if 'AirTemp' in property_ else 'Sfc',
            'period': self.period
        }

        self.data = Path(self.filepath_pattern.format(**filepath_elements))

        if not self.data.exists():
            LOGGER.debug(f'File {self.data} does not exist.')
            err = 'No data found for the specified query.'
            LOGGER.error(err)
            raise ProviderQueryError(err)

        # set the data bbox from bbox query param or xy axis subsets
        if len(bbox) > 0:
            minx, miny, maxx, maxy = bbox
            shapes = [
                {
                    'type': 'Polygon',
                    'coordinates': [
                        [
                            [minx, miny],
                            [minx, maxy],
                            [maxx, maxy],
                            [maxx, miny],
                            [minx, miny]
                        ]
                    ],
                }
            ]

        with rasterio.open(self.data) as _data:
            # set self._data to the opened file so rasterio_ provider generates
            # the correct coverage metadata when generating the output
            self._data = _data
            LOGGER.debug('Creating output coverage metadata')
            out_meta = _data.meta
            if self.options is not None:
                for key, value in self.options.items():
                    out_meta[key] = value

            if shapes:  # spatial subset
                try:
                    LOGGER.debug('Clipping data with bbox')
                    out_image, out_transform = rasterio.mask.mask(
                        _data,
                        shapes,
                        filled=False,
                        crop=True,
                        indexes=[1],
                        nodata=NODATA_VALUE
                    )
                except ValueError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError(err)
                out_meta.update(
                    {
                        'driver': self.native_format,
                        'height': out_image.shape[1],
                        'width': out_image.shape[2],
                        'transform': out_transform,
                        'nodata': NODATA_VALUE
                    }
                )

            else:  # no spatial subset
                LOGGER.debug('Creating data in memory')
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

            LOGGER.debug('Serializing data in memory')
            if format_ == 'json':
                LOGGER.debug('Creating output in CoverageJSON')
                out_meta['bands'] = [1]
                cj = self.gen_covjson(out_meta, out_image)
                for param in cj['parameters']:
                    # ensure description is a dict to adhere to
                    # CoverageJSON spec
                    cj['parameters'][param]['description'] = {
                        'en': cj['parameters'][param]['description']
                    }
                return cj

            else:
                with MemoryFile() as memfile:
                    with memfile.open(**out_meta, nbits=30) as dest:
                        dest.write(out_image)
                        LOGGER.debug(
                            'Adding source GRIB metadata to outputted file'
                        )
                        dest.update_tags(1, **_data.tags(1))

                    LOGGER.debug('Returning data in native format')
                    return memfile.read()

    def __repr__(self) -> str:
        return '<CanSIPSProductsProvider>'
