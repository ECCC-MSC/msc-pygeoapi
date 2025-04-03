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
from osgeo import gdal
from pyproj import Transformer

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wind-data',
    'title': 'GeoMet-Weather Wind Data process',
    'description': 'GeoMet-Weather Wind Data process',
    'keywords': ['quartile data'],
    'links': [],
    'inputs': {
        'model': {
            'title': 'model name',
            'description': 'GEPS, REPS',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'variables_and_levels': {
            'title': 'variables and levels',
            'description': 'ex: [[TMP, AGL-2m], [WIND, AGL-10m]]',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'items': {
                        'type': 'string'
                    }
                }
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'products': {
            'title': 'products',
            'description': 'ex: ["min_all_mem", "prct_10", "prct_25", "prct_50", "prct_75", "prct_90", "max_all_mem"]',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'latitude': {
            'title': 'level',
            'description': 'ex: 45.234',
            'schema': {
                'type': 'float',
            },
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'longitude': {
            'title': 'level',
            'description': 'ex: -80.65',
            'schema': {
                'type': 'float',
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
    },
    'outputs':{
        "extract_quartile_response": {
            "title": "extract_wind_data_response",
            "schema": {"contentMediaType": "application/json"},
        }
    },
    'example': {
        'inputs': {
                'model': 'REPS',
                'variables_and_levels': [['TMP', 'AGL-2m'],['WIND', 'AGL-10m']],
                'products': ['min_all_mem', 'prct_10', 'prct_25', 'prct_50', 'prct_75', 'prct_90', 'max_all_mem'],
                'lat': 45,
                'lon': -82,
                'model_run': "2025-04-03T00:00:00Z",
                'forecast_start_hour': 3,
                'forecast_step': 3,
                'forecast_end_hour': 3
        }
    }
}

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
HPFX_BASEPATH = "/datasan/geomet/feeds/hpfx/ensemble/"

BANDS = {
    "prct_10": 1,
    "prct_25": 2,
    "prct_50": 3,
    "prct_75": 4,
    "prct_90": 5,
    "ens_spread": 6,
    "ens_mean": 7,
    "min_all_mem": 8,
    "max_all_mem": 9
}

def geo2xy(ds, x, y):
    """
    transforms geographic coordinate to x/y pixel values

    :param ds: GDAL dataset object
    :param x: x coordinate
    :param y: y coordinate

    :returns: list of x/y pixel values
    """

    LOGGER.debug("Running affine transformation")
    geotransform = ds.GetGeoTransform()

    origin_x = geotransform[0]
    origin_y = geotransform[3]

    width = geotransform[1]
    height = geotransform[5]

    x = int((x - origin_x) / width)
    y = int((y - origin_y) / height)

    return x, y

def r_geo2xy(ds, x, y):
    raise NotImplementedError("to do")

def get_unit(path, prod):
    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise NameError(f"Couldn't open {path}, check if file exists")
    
    band = ds.GetRasterBand(BANDS[prod])

    return band.GetMetadataItem('GRIB_UNIT')

def get_var(path, prod, lon, lat):
    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise NameError(f"Couldn't open {path}, check if file exists")
    
    out_proj = ds.GetProjection()
    transformer = Transformer.from_crs("EPSG:4326", out_proj, always_xy=True)
    _x, _y = transformer.transform(lon, lat)
    x, y = geo2xy(ds, _x, _y)
    band = ds.GetRasterBand(BANDS[prod])
    data_array = band.ReadAsArray()

    try:
        _ = data_array[y, x]
    except IndexError:
        raise IndexError("ERROR: no data at requested latitude and longitude - point outside of model grid")

    return data_array[y, x]

def get_path(model, run_hour, forecast_start_hour, date_formatted, var, level):
    forecast_start_hour = f"{forecast_start_hour:03}"
    if model == "geps":
        path = f"{HPFX_BASEPATH}{model}/grib2/products/{run_hour}/{forecast_start_hour}/CMC_geps-prob_{var}_{level}_latlon0p5x0p5_{date_formatted}{run_hour}_P{forecast_start_hour}_all-products.grib2"
    elif model == "reps":
        path = f"{HPFX_BASEPATH}{model}/10km/grib2/{run_hour}/{forecast_start_hour}/{date_formatted}T{run_hour}Z_MSC_REPS_{var}-Prob_{level}_RLatLon0.09x0.09_PT{forecast_start_hour}H.grib2"
    else:
        raise NameError(f"Invalid model {model}")
    
    return path

def extract_quartiles(
        model,
        variables_and_levels,
        products,
        lat,
        lon,
        model_run,
        forecast_start_hour,
        forecast_step,
        forecast_end_hour,
):
    output = {
        'time_steps': []
    }

    n = (forecast_end_hour - forecast_start_hour) / forecast_step
    n = int(n) + 1

    for i in range(n):
        forecast_hour = forecast_start_hour + (i * forecast_step)
        #forecast_hour = f'{forecast_hour:03}'
        output['time_steps'].append(forecast_hour)

    date = datetime.datetime.strptime(model_run, DATE_FORMAT)
    run_hour = f"{date.hour:02}"
    date_formatted = date.strftime("%Y%m%d")
    forecast_start_hour = f"{forecast_start_hour:03}"
    model = model.lower()

    for var, level in variables_and_levels:
        output[var] = {}
        paths = [get_path(model, run_hour, fsh, date_formatted, var, level) for fsh in output['time_steps']]
        for prod in products:
            output[var][prod] = {
                'units': get_unit(paths[0], prod),
                'vals': [get_var(path, prod, lon, lat) for path in paths]
            }

    return output

if __name__ == "__main__":
    print(extract_quartiles(
        model="REPS",
        variables_and_levels=[["TMP", "AGL-2m"], ["WIND", "AGL-10m"]],
        products=["min_all_mem", "prct_10", "prct_25", "prct_50", "prct_75", "prct_90", "max_all_mem"],
        lat=45,
        lon=-82,
        model_run=f'{(datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}T00:00:00Z',
        forecast_start_hour=3,
        forecast_step=3,
        forecast_end_hour=9
    ))

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class ExtractQuartileProcessor(BaseProcessor):
        """Extract Quartile Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.weather.extract_quartile.ExtractQuartileProcessor  # noqa
            """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data, outputs=None):
            mimetype = "application/json"

            required = ["model", "variables_and_levels", "products", "lat", "lon", "model_run", "forecast_start_hour", "forecast_step", "forecast_end_hour"]
            if not all([param in data for param in required]):
                msg = "Missing required parameters."
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            model = data.get("model")
            vars_and_levels = data.get("variables_and_levels")
            prod = data.get("products")
            lat = data.get("lat")
            lon = data.get("lon")
            mr = data.get("model_run")
            fsh = data.get("forecast_start_hour")
            fs = data.get("forecast_step")
            feh = data.get("forecast_end_hour")

            try:
                output = extract_quartiles(
                    model,
                    vars_and_levels,
                    prod,
                    lat,
                    lon,
                    mr,
                    fsh,
                    fs,
                    feh,
                )
            except ValueError as err:
                msg = f'Process execution error: {err}'
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            return mimetype, output

        def __repr__(self):
            return f'<ExtractQuartileProcessor> {self.name}'

except (ImportError, RuntimeError) as err:
    LOGGER.warning(f'Import errors: {err}')