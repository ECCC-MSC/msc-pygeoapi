# =================================================================
#
# Author: Gabriel de Courval-Paré
#
# Copyright (c) 2023 Tom Kralidis
# Copyright (c) 2025 Gabriel de Courval-Paré
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

import datetime
import json
import logging

import click
from osgeo import gdal
from pyproj import Transformer
import math

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wind-data',
    'title': 'GeoMet-Weather Wind Data process',
    'description': 'GeoMet-Weather Wind Data process',
    'keywords': ['wind data'],
    'links': [],
    'inputs': {
        'model': {
            'title': 'model name',
            'description': 'GDWPS, GDPS, GEPS, RDWPS, HRDPS, REPS',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'model_run': {
            'title': 'Model run',
            'description': 'Model run in %Y-%m-%dTH:M:SZ format.',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'forecast_start_hour': {
            'title': 'Forecast start hour',
            'description': 'Forecast start hour.',
            'schema': {'type': 'number'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'forecast_step': {
            'title': 'Forecast time step',
            'description': 'Forecast temporal step, e.g. 1, 3, 6, 12 or 24',
            'schema': {'type': 'number'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'forecast_end_hour': {
            'title': 'Forecast end hour',
            'description': 'Forecast end hour.',
            'schema': {'type': 'number'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'lon': {
            'title': 'lon coordinate',
            'description': 'Longitude of the requested location (EPSG:4326)',
            'schema': {
                'type': 'number',
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'lat': {
            'title': 'lat coordinate',
            'description': 'Latitude of the requested location (EPSG:4326)',
            'schema': {
                'type': 'number',
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
    },
    "outputs": {
        "extract_wind_data_response": {
            "title": "extract_wind_data_response",
            "schema": {"contentMediaType": "application/json"},
        }
    },
    'example': {
        'inputs': {
            'model': 'GDWPS',
            'model_run': f'{(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}T12:00:00Z',  # noqa
            'forecast_start_hour': '003',
            'lon': -28.75,
            'lat': 39.25,
        }
    }
}

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
MS_TO_KNOTS = 1.943844


def get_norm(*components):
    norm = 0
    for component in components:
        norm += component*component
    return math.sqrt(norm)


def get_dir(u, v):
    phi = 180 + (180 / math.pi) * math.atan2(u, v)
    phi = phi % 360

    return phi


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

    return (x, y)


def get_single_wind_data(model, date_formatted, run_hour, forecast_hour, data_basepath, lon, lat):
    match(model):
        case "GDWPS":
            inter_path = f"/model_gdwps/25km/{run_hour}/"
            file_name_u = f"{date_formatted}T{run_hour}Z_MSC_GDWPS_UGRD_AGL-10m_LatLon0.25_PT{forecast_hour}H.grib2"
            file_name_v = f"{date_formatted}T{run_hour}Z_MSC_GDWPS_VGRD_AGL-10m_LatLon0.25_PT{forecast_hour}H.grib2"

        case "GDPS":
            inter_path = f"/model_gem_global/15km/grib2/lat_lon/{run_hour}/{forecast_hour}/"
            file_name_u = f"CMC_glb_UGRD_TGL_10_latlon.15x.15_{date_formatted}{run_hour}_P{forecast_hour}.grib2"
            file_name_v = f"CMC_glb_VGRD_TGL_10_latlon.15x.15_{date_formatted}{run_hour}_P{forecast_hour}.grib2"

        case "HRDPS":
            inter_path = f"/model_hrdps/continental/2.5km/{run_hour}/{forecast_hour}/"
            file_name_wind = f"{date_formatted}T{run_hour}Z_MSC_HRDPS_WIND_AGL-10m_RLatLon0.0225_PT{forecast_hour}H.grib2"
            file_name_wdir = f"{date_formatted}T{run_hour}Z_MSC_HRDPS_WDIR_AGL-10m_RLatLon0.0225_PT{forecast_hour}H.grib2"

        case "REPS":
            inter_path = f"/ensemble/reps/10km/grib2/{run_hour}/{forecast_hour}/"
            file_name_u = f"{date_formatted}T{run_hour}Z_MSC_REPS_UGRD_AGL-10m_RLatLon0.09x0.09_PT{forecast_hour}H.grib2"
            file_name_v = f"{date_formatted}T{run_hour}Z_MSC_REPS_VGRD_AGL-10m_RLatLon0.09x0.09_PT{forecast_hour}H.grib2"

        case _:
            raise ValueError(f"Unknown model: {model}")

    if model in ("GDWPS", "GDPS", "GEPS", "REPS"):
        full_path_u = f"{data_basepath}{inter_path}{file_name_u}"
        full_path_v = f"{data_basepath}{inter_path}{file_name_v}"

        ds_u = gdal.Open(full_path_u, gdal.GA_ReadOnly)
        ds_v = gdal.Open(full_path_v, gdal.GA_ReadOnly)

        if ds_u is None:
            raise NameError(f"Couldn't open {full_path_u}, check if file exists")

        if ds_v is None:
            raise NameError(f"Couldn't open {full_path_v}, check if file exists")

        out_proj = ds_u.GetProjection()
        transformer = Transformer.from_crs("EPSG:4326", out_proj, always_xy=True)
        _x, _y = transformer.transform(lon, lat)
        x, y = geo2xy(ds_u, _x, _y)

        band_u = ds_u.GetRasterBand(1)
        bitmap_u = band_u.GetMaskBand().ReadAsArray()

        try:
            _ = bitmap_u[y, x]
        except IndexError:
            raise IndexError("ERROR: no data at requested latitude and longitude - point outside of model grid")

        if not bitmap_u[y, x]:
            raise IndexError("ERROR: no data at requested latitude and longitude - data at point is masked")

        band_y = ds_v.GetRasterBand(1)

        data_array_u = band_u.ReadAsArray()
        data_array_v = band_y.ReadAsArray()

        wind_speed_u = data_array_u[y, x]
        wind_speed_v = data_array_v[y, x]

        norm = get_norm(wind_speed_u, wind_speed_v) * MS_TO_KNOTS
        dir = get_dir(wind_speed_u, wind_speed_v)

    else:
        full_path_wind = f"{data_basepath}{inter_path}{file_name_wind}"
        full_path_wdir = f"{data_basepath}{inter_path}{file_name_wdir}"

        ds_wind = gdal.Open(full_path_wind, gdal.GA_ReadOnly)
        ds_wdir = gdal.Open(full_path_wdir, gdal.GA_ReadOnly)

        if ds_wind is None:
            raise NameError(f"Couldn't open {full_path_wind}, check if file exists")

        if ds_wdir is None:
            raise NameError(f"Couldn't open {full_path_wdir}, check if file exists")

        out_proj = ds_wind.GetProjection()
        transformer = Transformer.from_crs("EPSG:4326", out_proj, always_xy=True)
        _x, _y = transformer.transform(lon, lat)
        x, y = geo2xy(ds_wind, _x, _y)

        band_wind = ds_wind.GetRasterBand(1)
        bitmap_wind = band_wind.GetMaskBand().ReadAsArray()

        try:
            _ = bitmap_wind[y, x]
        except IndexError:
            raise IndexError("ERROR: no data at requested latitude and longitude - point outside of model grid")

        if not bitmap_wind[y, x]:
            raise IndexError("ERROR: no data at requested latitude and longitude - data at point is masked")

        band_wdir = ds_wdir.GetRasterBand(1)

        data_array_wind = band_wind.ReadAsArray()
        data_array_wdir = band_wdir.ReadAsArray()

        norm = data_array_wind[y, x] * MS_TO_KNOTS
        dir = data_array_wdir[y, x]

    output = {}

    output['wind_speed'] = norm
    output['wind_dir'] = dir

    return output


def extract_wind_data(
    model,
    model_run,
    forecast_start_hour,
    forecast_step,
    forecast_end_hour,
    lon,
    lat,
):
    from msc_pygeoapi.env import GEOMET_HPFX_BASEPATH

    data_basepath = GEOMET_HPFX_BASEPATH

    date = datetime.datetime.strptime(model_run, DATE_FORMAT)
    run_hour = f"{date.hour:02}"
    date_formatted = date.strftime("%Y%m%d")

    output = {
        'type': "Feature",
        'geometry': {"type": "Point", "coordinates": [lon, lat]},
        'properties': {
            'wind_speed_unit': "knots",
            'wind_direction_unit': "°",
        },
    }

    n = (forecast_end_hour - forecast_start_hour) / forecast_step
    n = int(n) + 1

    for i in range(n):
        forecast_hour = forecast_start_hour + (i * forecast_step)
        forecast_hour = f'{forecast_hour:03}'
        output['properties'][forecast_hour] = get_single_wind_data(model, date_formatted, run_hour, forecast_hour, data_basepath, lon, lat)

    return output


@click.group("execute")
def extract_wind_data_execute():
    pass


@click.command("extract-wind-data")
@click.pass_context
@click.option(
              "-m",
              "--model",
              help="GDWPS, GDPS, GEPS, RDWPS, HRDPS or REPS",
              required=True)
@click.option(
    "-mr",
    "--model_run",
    help="model run in %Y-%m-%dT%H:%M:%SZ format",
    required=True
)
@click.option(
    "-fh",
    "--forecast_start_hour",
    help="forecast hour 3 digits format",
    required=True
)
@click.option(
    "--lon",
    help="longitude in number format, i.e. -85.000, not 85.000W",
    required=True
)
@click.option(
    "--lat",
    help="latitude in number format, i.e. -85.000, not 85.000S",
    required=True
)
def extract_wind_data_cli(
    ctx,
    model,
    model_run,
    forecast_start_hour,
    lon,
    lat,
):
    import rasterio
    import time

    start = time.time()
    with rasterio.Env():
        output = extract_wind_data(
            model,
            model_run,
            forecast_start_hour,
            float(lon),
            float(lat),
        )
    end = time.time()
    print("Time elapsed array:", end - start)
    click.echo(json.dumps(output, ensure_ascii=False))


extract_wind_data_execute.add_command(extract_wind_data_cli)

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class ExtractWindDataProcessor(BaseProcessor):
        """Extract Wind Data Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.weather.extract_wind_data.ExtractWindDataProcessor  # noqa
            """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data, outputs=None):
            mimetype = "application/json"

            required = ["model", "model_run", "forecast_start_hour", "forecast_step", "forecast_end_hour", "lon", "lat"]
            if not all([param in data for param in required]):
                msg = "Missing required parameters."
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            model = data.get("model")
            mr = data.get("model_run")
            fsh = data.get("forecast_start_hour")
            fs = data.get("forecast_step")
            fse = data.get("forecast_end_hour")
            lon = float(data.get("lon"))
            lat = float(data.get("lat"))

            try:
                output = extract_wind_data(
                    model,
                    mr,
                    fsh,
                    fs,
                    fse,
                    lon,
                    lat,
                )
            except ValueError as err:
                msg = f'Process execution error: {err}'
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            return mimetype, output

        def __repr__(self):
            return f'<ExtractWindDataProcessor> {self.name}'

except (ImportError, RuntimeError) as err:
    LOGGER.warning(f'Import errors: {err}')
