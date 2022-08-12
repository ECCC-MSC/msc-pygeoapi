# =================================================================
#
# Author: Philippe Theroux <Philippe.Theroux@ec.gc.ca>
#
# Copyright (c) 2022 Philippe Theroux
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the 'Software'), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import datetime
import glob
import json
import logging
import os.path
import re

import click
from osgeo import gdal, osr
from pyproj import Proj, transform
import xarray

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'extract-sounding-data',
    'title': 'Extract sounding data',
    'description': 'extract sounding data by point',
    'keywords': ['extract sounding'],
    'links': [
        {
            'type': 'text/html',
            'rel': 'canonical',
            'title': 'information',
            'href': 'https://eccc-msc.github.io/open-data/readme_en',
            'hreflang': 'en-CA'
        },
        {
            'type': 'text/html',
            'rel': 'alternate',
            'title': 'information',
            'href': 'https://eccc-msc.github.io/open-data/readme_fr',
            'hreflang': 'fr-CA'
        }
    ],
    'inputs': {
        'model': {
            'title': 'Model',
            'description': 'RDPS, GDPS or HRDPS.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'model_run': {
            'title': 'Model run',
            'description': 'Model run in %Y-%m-%dTH:M:SZ format.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'forecast_hour': {
            'title': 'Forecast hour',
            'description': 'Forecast hour in XXX format.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'lat': {
            'title': 'Latitude',
            'description': 'Latitude in wgs84.',
            'schema': {
                'type': 'float'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'lon': {
            'title': 'Longitude',
            'description': 'Longitude in wgs84.',
            'schema': {
                'type': 'float'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'convection_indices': {
            'title': 'Return convection indices.',
            'description': '[OPTIONAL: default is True] '
                           'Set it to False if you won\'t be using '
                           'convection indices\' values so that the '
                           'process takes less time to complete.',
            'schema': {
                'type': 'bool'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'temperature_data': {
            'title': 'Return temperature and dewpoint sounding data.',
            'description': '[OPTIONAL: default is True] '
                           'Set it to False if you won\'t be using '
                           'temperature or dewpoint temperature/depression '
                           'so that the process takes less time to complete.',
            'schema': {
                'type': 'bool'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'wind_data': {
            'title': 'Return wind sounding data.',
            'description': '[OPTIONAL: default is True] '
                           'Set it to False if you won\'t be using '
                           'wind speed or wind direction so that the '
                           'process takes less time to complete.',
            'schema': {
                'type': 'bool'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'novalues_above_100mbar': {
            'title': 'Discard values above 100mbar.',
            'description': '[OPTIONAL: default is False] '
                           'Set it to True if values for pressure levels '
                           'above 100mbar are not going to be used so '
                           'that the process takes less time to complete.',
            'schema': {
                'type': 'bool'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'extract_sounding_data_response': {
            'title': 'extract_sounding_data_response',
            'schema': {'contentMediaType': 'application/json'}
        }
    },
    'example': {
        'inputs': {
            'model': 'RDPS',
            'model_run': f'{(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}T12:00:00Z',  # noqa
            'forecast_hour': '003',
            'lat': 56.1303,
            'lon': -106.3468,
            'convection_indices': True,
            'temperature_data': True,
            'wind_data': True,
            'novalues_above_100mbar': False
        }
    }
}

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def geo2xy(ds, x, y):
    """
    transforms geographic coordinate to x/y pixel values

    :param ds: GDAL dataset object
    :param x: x coordinate
    :param y: y coordinate

    :returns: list of x/y pixel values
    """

    LOGGER.debug('Running affine transformation')
    geotransform = ds.GetGeoTransform()

    origin_x = geotransform[0]
    origin_y = geotransform[3]

    width = geotransform[1]
    height = geotransform[5]

    x = int((x - origin_x) / width)
    y = int((y - origin_y) / height)

    return x, y


def numerical_sort(value):
    """
    Splits out any digits in a filename, turns it into
    an actual number, and returns the result for sorting.
    Used to sort file names by the pressure level contained in them.
    """
    numbers = re.compile(r'(\d+)')
    parts = numbers.split(value)
    parts[1::2] = map(int, parts[1::2])
    return parts


def extract_sounding_data(
    model,
    model_run,
    forecast_hour,
    lon,
    lat,
    convection_indices=True,
    temperature_data=True,
    wind_data=True,
    novalues_above_100mbar=False
):
    """
    Creates a json containing pressure, temperature, dew point temperature,
    dew point depression, wind speed, wind direction for each pressure level.
    Also gives other useful sounding parameters.

    :param model: model name
    :param model_run: date and hour at which the model is ran
    :param forecast_hour: valid hour
    :param lon: longitude
    :param lat: latitude
    :param convection_indices: True or False
    :param temperature_data: True or False
    :param wind_data: True or False
    :param novalues_above_100mbar: If True, stop at 100mbar

    :returns: json containing data for the given params
    """

    from msc_pygeoapi.env import (
        GEOMET_DDI_BASEPATH,
        GEOMET_SCIENCE_BASEPATH
    )

    data_basepath = GEOMET_DDI_BASEPATH

    date = datetime.datetime.strptime(model_run, DATE_FORMAT)
    run_hour = f'{date.hour:02}'
    date_formatted = date.strftime(f'%Y%m%d{run_hour}')

    # Pre-build paths and add projection for each model
    if model == 'GDPS':
        inter_path = (
            f'/model_gem_global/15km/grib2/lat_lon/{run_hour}/{forecast_hour}'
        )
        file_name = f'/CMC_glb_{{info}}_ISBL_*_latlon.15x.15_{date_formatted}_P{forecast_hour}.grib2'  # noqa
        first_value = f'/CMC_glb_{{info}}_TGL_{{height}}_latlon.15x.15_{date_formatted}_P{forecast_hour}.grib2'  # noqa
        out_proj = '+proj=longlat +datum=WGS84 +no_defs'
    elif model == 'RDPS':
        inter_path = (
            f'/model_gem_regional/10km/grib2/{run_hour}/{forecast_hour}'
        )
        file_name = f'/CMC_reg_{{info}}_ISBL_*_ps10km_{date_formatted}_P{forecast_hour}.grib2'  # noqa
        first_value = f'/CMC_reg_{{info}}_TGL_{{height}}_ps10km_{date_formatted}_P{forecast_hour}.grib2'  # noqa
        out_proj = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=249 +x_0=0 +y_0=0 +R=6371229 +units=m +no_def'  # noqa
    elif model == 'HRDPS':
        inter_path = (
            f'/model_hrdps/continental/grib2/{run_hour}/{forecast_hour}'
        )
        file_name = f'/CMC_hrdps_continental_{{info}}_ISBL_*_ps2.5km_{date_formatted}_P{forecast_hour}-00.grib2'  # noqa
        first_value = f'/CMC_hrdps_continental_{{info}}_TGL_{{height}}_ps2.5km_{date_formatted}_P{forecast_hour}-00.grib2'  # noqa
        out_proj = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=252 +x_0=0 +y_0=0 +R=6371229 +units=m +no_def'  # noqa
    else:
        msg = 'Not a valid model: {}'.format(model)
        LOGGER.error(msg)
        raise ValueError(msg)

    output = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
        'properties': {},
    }

    ABBR = {}
    if temperature_data or wind_data:
        output['properties']['pressure_unit'] = 'mbar'
        if temperature_data:
            ABBR.update({
                'TMP': 'air_temperature',
                'DEPR': 'dew_point_depression',
                'DPT': 'dew_point_temperature',
            })
            output['properties'].update(
                    {
                        'temperature_unit': '°C',
                        'dew_point_temperature_unit': '°C',
                        'dew_point_depression_unit': '°C',
                    }
                )
        if wind_data:
            ABBR.update({
                'WDIR': 'wind_direction',
                'WIND': 'wind_speed',
            })
            output['properties'].update(
                    {
                        'wind_speed_unit': 'knots',
                        'wind_direction_unit': '°',
                    }
                )

    # Reproject depending on the model
    proj_4326 = '+proj=longlat +datum=WGS84 +no_defs'
    srs_in = osr.SpatialReference()
    srs_in.ImportFromProj4(proj_4326)

    srs_out = osr.SpatialReference()
    srs_out.ImportFromProj4(out_proj)

    inProj = Proj(srs_in.ExportToProj4())
    outProj = Proj(srs_out.ExportToProj4())
    _x, _y = transform(inProj, outProj, lon, lat)

    # Find surface pressure level
    first_pressure_level = f'{data_basepath}{inter_path}{file_name.replace("{info}_ISBL_*", "PRES_SFC_0")}'  # noqa
    ds = gdal.Open(first_pressure_level)
    # Since all files will be opened on the same latlon,
    # xy is only calculated once here
    x, y = geo2xy(ds, _x, _y)
    min_pressure_level = (
        xarray.open_rasterio(first_pressure_level)[0, y, x].data.item() / 100
    )

    # Add additionnal useful informations if they're present
    # for that model and if they are required
    if convection_indices:
        output['properties'].update(
            {
                'CAPE_unit': 'J/kg',
                'CIN_unit': 'J/kg',
                'LCL_unit': 'm AGL',
                'LI_unit': '--',
                'LFC_unit': 'm AGL',
                'EL_unit': 'm AGL'
            }
        )
        conv_dict = {
            'CAPE': {
                'type': 'SFC_0',
                'content': {
                    'surface_based_parcel': 'CAPE',
                    'parcel_mean_layer': 'ML-CAPE',
                    'most_unstable_parcel': 'MU-CAPE'
                }
            },
            'CIN': {
                'type': 'SFC_0',
                'content': {
                    'surface_based_parcel': 'CIN',
                    'parcel_mean_layer': 'ML-CIN',
                    'most_unstable_parcel': 'MU-CIN'
                }
            },
            'LCL': {
                'type': 'SFC_0',
                'content': {
                    'surface_based_parcel': 'SFC-LCL-HGT',
                    'parcel_mean_layer': 'ML-LCL-HGT',
                    'most_unstable_parcel': 'MU-LCL-HGT'
                }
            },
            'LI': {
                'type': 'ISBL_500',
                'content': {
                    'parcel_lifted_from_surface_ref500mbar': 'SFC-LI',
                    'parcel_mean_layer_ref500mbar': 'ML-LI',
                    'most_unstable_parcel_ref500mbar': 'MU-LI'
                }
            },
            'LFC': {
                'type': 'SFC_0',
                'content': {
                    'surface_based_parcel': 'SFC-LFC-HGT',
                    'parcel_mean_layer': 'ML-LFC-HGT',
                    'most_unstable_parcel': 'MU-LFC-HGT'
                }
            },
            'EL': {
                'type': 'SFC_0',
                'content': {
                    'surface_based_parcel': 'SFC-EL-HGT',
                    'parcel_mean_layer': 'ML-EL-HGT',
                    'most_unstable_parcel': 'MU-EL-HGT'
                }
            }
        }
        science_basepath = GEOMET_SCIENCE_BASEPATH
        for index, val in conv_dict.items():
            filename = file_name.replace('ISBL_*', val['type'])
            output['properties'][index] = {}
            for desc, info in val['content'].items():
                full_path = f'{science_basepath}{inter_path}{filename.format(info=info)}'  # noqa
                if os.path.exists(full_path):
                    output['properties'][index][desc] = xarray.open_rasterio(
                        full_path
                    )[0, y, x].data.item()
                    if index in ['CAPE', 'LFC', 'EL'] and output['properties'][index][desc] < 0:  # noqa
                        output['properties'][index][desc] = '-'
                    elif index == 'CIN' and output['properties'][index][desc] > 0:  # noqa
                        output['properties'][index][desc] = '-'
                else:
                    output['properties'][index][desc] = 'N/A'

    if temperature_data or wind_data:
        # Add the first pressure level(min_pressure_level) to the dict and
        # the values will come from the 2m(temps) and 10m(winds) files.
        first_value_path = f'{data_basepath}{inter_path}{first_value}'
        output['properties'][f'{min_pressure_level}mbar'] = {
            'pressure': min_pressure_level
        }
        if temperature_data:
            for info in ['TMP', 'DEPR', 'DPT']:
                output['properties'][f'{min_pressure_level}mbar'][ABBR[info]] = (  # noqa
                    xarray.open_rasterio(first_value_path.format(info=info, height=2))[0, y, x].data.item()  # noqa
                )
        if wind_data:
            for info in ['WDIR', 'WIND']:
                output['properties'][f'{min_pressure_level}mbar'][ABBR[info]] = (  # noqa
                    xarray.open_rasterio(first_value_path.format(info=info, height=10))[0, y, x].data.item()  # noqa
                )

    # Build the dict by adding pressure, temperature, dewpoint depression,
    # dewpoint temperature, wind speed and wind direction for every pressure
    for info in ABBR:
        # Calculate dewpoint temperature with (tempature - dewpoint depression)
        if info == 'DPT':
            for field, data in output['properties'].items():
                if re.search(r'(\d+)mbar', field):
                    try:
                        dew_point = data[ABBR['TMP']] - data[ABBR['DEPR']]
                        data['dew_point_temperature'] = dew_point
                    except KeyError:
                        # missing data for TMP or DEPR so can't calc DPT
                        pass
            continue
        full_path = f'{data_basepath}{inter_path}{file_name.format(info=info)}'
        paths = sorted(glob.glob(full_path), key=numerical_sort, reverse=True)
        for p in paths:
            pressure_level = int(
                re.search(full_path.replace('*', '(.*)'), p).group(1)
            )
            # Take only what's over min_pressure_level
            if pressure_level >= min_pressure_level:
                continue
            # Add flag if you don't need pressure levels over 100mbar
            elif novalues_above_100mbar and pressure_level < 100:
                break
            value = xarray.open_rasterio(p)[0, y, x].data.item()

            if f'{pressure_level}mbar' not in output['properties']:
                output['properties'][f'{pressure_level}mbar'] = {
                    'pressure': pressure_level
                }
            # from m/s to knots
            if info == 'WIND':
                value *= 1.943844
            output['properties'][f'{pressure_level}mbar'][ABBR[info]] = value

    return output


@click.group('execute')
def extract_sounding_data_execute():
    pass


@click.command('extract-sounding-data')
@click.pass_context
@click.option('--model', help='GDPS, RDPS or HRDPS', required=True)
@click.option(
    '-mr',
    '--model_run',
    help='model run in %Y-%m-%dT%H:%M:%SZ format',
    required=True
)
@click.option(
    '-fh',
    '--forecast_hour',
    help='forecast hour 3 digits format',
    required=True
)
@click.option(
    '--lon',
    help='longitude in number format, i.e. -85.000, not 85.000W',
    required=True
)
@click.option(
    '--lat',
    help='latitude in number format, i.e. -85.000, not 85.000S',
    required=True
)
@click.option(
    '-c',
    '--convection_indices',
    is_flag=True,
    help=(
        'True to return convection indices, '
        'False otherwise. Defaults to True. '
    ),
    required=False,
    default=True
)
@click.option(
    '-t',
    '--temperature_data',
    is_flag=True,
    help=(
        'True to return temperature and dewpoint sounding data, '
        'False otherwise. Defaults to True. '
    ),
    required=False,
    default=True
)
@click.option(
    '-w',
    '--wind_data',
    is_flag=True,
    help=(
        'True to return wind sounding data, '
        'False otherwise. Defaults to True. '
    ),
    required=False,
    default=True
)
@click.option(
    '-n',
    '--novalues_above_100mbar',
    is_flag=True,
    help='Set flag if you don\'t need values for pressure levels above 100mb',
    required=False
)
def extract_sounding_data_cli(
    ctx,
    model,
    model_run,
    forecast_hour,
    lon,
    lat,
    convection_indices,
    temperature_data,
    wind_data,
    novalues_above_100mbar
):
    import rasterio
    import time

    start = time.time()
    with rasterio.Env():
        output = extract_sounding_data(
            model,
            model_run,
            forecast_hour,
            float(lon),
            float(lat),
            convection_indices,
            temperature_data,
            wind_data,
            novalues_above_100mbar
        )
    end = time.time()
    print('Time elapsed array:', end - start)
    click.echo(json.dumps(output, ensure_ascii=False))


extract_sounding_data_execute.add_command(extract_sounding_data_cli)

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class ExtractSoundingDataProcessor(BaseProcessor):
        """Extract Sounding Data Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.weather.extract_sounding_data.ExtractSoundingDataProcessor  # noqa
            """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            mimetype = 'application/json'

            required = ['model', 'model_run', 'forecast_hour', 'lon', 'lat']
            if not all([param in data for param in required]):
                msg = 'Missing required parameters.'
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            model = data.get('model')
            mr = data.get('model_run')
            fh = data.get('forecast_hour')
            lon = float(data.get('lon'))
            lat = float(data.get('lat'))
            conv = data.get('convection_indices')
            temperature_data = data.get('temperature_data')
            wind_data = data.get('wind_data')
            noval_above_100 = data.get('novalues_above_100mbar') or False

            conv, temperature_data, wind_data = [
                arg if arg is not None else True
                for arg in [conv, temperature_data, wind_data]
            ]

            try:
                output = extract_sounding_data(
                    model,
                    mr,
                    fh,
                    lon,
                    lat,
                    conv,
                    temperature_data,
                    wind_data,
                    noval_above_100
                )
            except ValueError as err:
                msg = 'Process execution error: {}'.format(err)
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            return mimetype, output

        def __repr__(self):
            return '<ExtractSoundingDataProcessor> {}'.format(self.name)


except (ImportError, RuntimeError) as err:
    LOGGER.warning('Import errors: {}'.format(err))
