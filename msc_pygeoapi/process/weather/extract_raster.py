# =================================================================
#
# Author: Tom Cooney <tom.cooney@canada.ca>
#
# Copyright (c) 2021 Tom Cooney
# Copyright (c) 2023 Tom Kralidis
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

import click
import json
import logging

from elasticsearch import Elasticsearch, exceptions
import rasterio
import rasterio.mask
from rasterio.crs import CRS
from rasterio.io import MemoryFile

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'extract-raster',
    'title': 'Extract raster data',
    'description': 'extract raster data by point, line, polygon',
    'keywords': ['extract raster'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://eccc-msc.github.io/open-data/readme_en',
        'hreflang': 'en-CA'
    }, {
        'type': 'text/html',
        'rel': 'alternate',
        'title': 'information',
        'href': 'https://eccc-msc.github.io/open-data/readme_fr',
        'hreflang': 'fr-CA'

    }],
    'inputs': [{
        'id': 'model',
        'title': 'model',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'forecast_hours_',
        'title': 'forecast_hours_',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'model_run',
        'title': 'model_run',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'input_geojson',
        'title': 'input_geojson',
        'input': {
            'formats': [{
                'mimeType': 'application/json'
            }]
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }],
    'outputs': [{
        'id': 'extract_raster_response',
        'title': 'extract_raster_response',
        'output': {
            'formats': [{
                'mimeType': 'application/json'
            }]
        }
    }],

}

ES_INDEX = 'hackathon-lp'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def get_files(layers, fh, mr):
    """
    ES search to find files names

    :param layers: arrays of three layers
    :param fh: forcast hour datetime
    :param mr: model run

    :returns: list of three file paths
    """
    es = Elasticsearch()
    list_files = []

    for layer in layers:
        for time_ in fh.split(','):
            files = {}
            s_object = {
                'query': {
                    'bool': {
                        'must': {
                            'match': {'properties.layer': layer}
                        },
                        'filter': [
                            {'term': {'properties.forecast_hour_datetime':
                                      time_}},
                            {'term': {'properties.reference_datetime': mr}}
                        ]
                    }
                }
            }

            try:
                res = es.search(index=ES_INDEX, body=s_object)
                try:
                    filepath = (res['hits']['hits'][0]
                                ['_source']['properties']['filepath'])
                    fh_ = (res['hits']['hits'][0]['_source']
                           ['properties']['forecast_hour_datetime'])
                    mr = (res['hits']['hits'][0]['_source']
                          ['properties']['reference_datetime'])

                    files['filepath'] = filepath
                    files['forecast_hour'] = fh_
                    files['model_run'] = mr

                    list_files.append(files)

                except IndexError as err:
                    msg = f'invalid input value: {err}'
                    LOGGER.error(msg)
                    return None, None

            except exceptions.ElasticsearchException as err:
                msg = f'ES search failed: {err}'
                LOGGER.error(msg)
                return None, None

    return list_files


def get_point(raster_list, input_geojson):
    """
    clips a raster by a point

    :param raster_list: list of paths to queried raster file on disk
    :param input_geojson: geojson file containing geometry of line

    :returns: dict with values of clipped points, original point
    geometry, and query type
    """
    to_return = {}
    i = 0

    in_coords = input_geojson['features'][0]['geometry']['coordinates']

    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"
        with rasterio.open(raster_path) as src:
            geom = input_geojson['features']
            if "type" in geom[0].keys():
                geom[0].pop("type")
            if "id" in geom[0].keys():
                geom[0].pop("id")
            shapes = [
                rasterio.warp.transform_geom(
                    CRS.from_string('EPSG:4326'),
                    src.crs,
                    geom[0]['geometry'])]
            out_image, out_transform = rasterio.mask.mask(
                src, shapes, crop=True)
            with MemoryFile() as memfile:
                with memfile.open(driver="GTiff",
                                  height=out_image.shape[1],
                                  width=out_image.shape[2], count=1,
                                  dtype=rasterio.float64,
                                  transform=out_transform) as dataset:
                    dataset.write(out_image)
                    ds = dataset.read()

                    to_return[i] = [in_coords[0],
                                    in_coords[1],
                                    ds[0][0][0],
                                    data_type]
                    ds = None
        i += 1

    return to_return


def get_line(raster_list, input_geojson):
    """
    clips a raster by a line

    :param raster_list: list of paths to queried raster file on disk
    :param input_geojson: geojson file containing geometry of line

    :returns: dict with values along clipped lines, original line
    geometry, and query type
    """
    to_return = {}
    iterat = 0
    input_line = input_geojson['features'][0]['geometry']['coordinates']

    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"

        try:
            with rasterio.open(raster_path) as src:
                geom = input_geojson['features']
                if "type" in geom[0].keys():
                    geom[0].pop("type")
                if "id" in geom[0].keys():
                    geom[0].pop("id")
                shapes = [
                    rasterio.warp.transform_geom(
                        CRS.from_string('EPSG:4326'),
                        src.crs,
                        geom[0]['geometry'])]
                out_image, out_transform = rasterio.mask.mask(
                    src, shapes, crop=True)
                with MemoryFile() as memfile:
                    with memfile.open(driver="GTiff",
                                      height=out_image.shape[1],
                                      width=out_image.shape[2], count=1,
                                      dtype=rasterio.float64,
                                      transform=out_transform) as dataset:
                        dataset.write(out_image)
                        ds = dataset.read()

                        x_to_mod = list(range(0, ds.shape[1]))
                        y_to_mod = list(range(0, ds.shape[2]))
                        xs, ys = rasterio.transform.xy(
                            out_transform, x_to_mod, y_to_mod, offset='center')
                        xy_array = rasterio.warp.transform(
                            src.crs, CRS.from_string('EPSG:4326'), xs, ys)
                        ds = ds[ds != src.nodata]
                        to_ret = []
                        for i in range(0, len(xy_array[0])):
                            to_ret.append([xy_array[0][i],
                                           xy_array[1][i],
                                           ds[i],
                                           data_type,
                                           input_line])
                        to_return[iterat] = to_ret
                        iterat += 1
        except FileNotFoundError as err:
            LOGGER.debug(err)

    return to_return


def summ_stats_poly(raster_list, input_geojson):
    """
    clips a raster by a polygon

    :param input_geojson: geojson file containing geometry of line
    :param input_geojson: geojson file containing geometry of polygon

    :returns: dict with min, max, and mean value for each query type
    and each forecast hour
    """
    to_return = {}
    iterat = 0
    input_poly = input_geojson['features'][0]['geometry']['coordinates']

    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"

        try:
            with rasterio.open(raster_path) as src:
                geom = input_geojson['features']
                if "type" in geom[0].keys():
                    geom[0].pop("type")
                if "id" in geom[0].keys():
                    geom[0].pop("id")
                shapes = [
                    rasterio.warp.transform_geom(
                        CRS.from_string('EPSG:4326'),
                        src.crs,
                        geom[0]['geometry'])]
                out_image, out_transform = rasterio.mask.mask(
                    src, shapes, crop=True)

                with MemoryFile() as memfile:
                    with memfile.open(driver="GTiff",
                                      height=out_image.shape[1],
                                      width=out_image.shape[2], count=1,
                                      dtype=rasterio.float64,
                                      transform=out_transform) as dataset:
                        dataset.write(out_image)
                        ds = dataset.read()
                        ds = ds[ds != src.nodata]
                        min_val = ds.min()
                        max_val = ds.max()
                        mean_val = ds.mean()
                        to_return[iterat] = [min_val,
                                             max_val,
                                             mean_val,
                                             data_type,
                                             input_poly]
                        iterat += 1
        except FileNotFoundError as err:
            LOGGER.debug(err)
    return to_return


def write_output(features, forecast_hours, poly, line, point):
    """
    writes output to OUTDATA dict depending on query type

    :param features: output from clipping function
    :param forecast_hours: list of all queried forecast hours
    :param poly: boolean to identify a polygon query
    :param line: boolean to identify a line query
    :param point: boolean to identify a point query

    :returns: dict with all queried forecast hours and clipping results
    """
    i = 0

    if line and not poly and not point:
        OUTDATA = {"type": "FeatureCollection",
                   "features": {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": features[0][0][0][4],
                        },
                        "properties": {
                            "Forecast Hours": []
                        }
                    }
                   }

        temp_line = []
        dir_line = []
        speed_line = []
        for hour in forecast_hours:
            OUTDATA["features"]['properties']["Forecast Hours"].append({
                "Forecast Hour": hour,
            })
        for i in features[0]:
            if 'Temperature Data' in features[0][i][3]:
                for x in features[0][0]:
                    temp_line.append([[x[0], x[1]], x[2]])
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Temperature"]) = {
                    "Observations Along Line": temp_line
                }
            if 'Wind Direction Data' in features[0][i][3]:
                for x in features[0][1]:
                    dir_line.append([[x[0], x[1]], x[2]])
                (OUTDATA["features"]['properties']["Forecast Hours"][
                 int(i/3)]["Wind Direction"]) = {
                    "Observations Along Line": dir_line
                }
            if 'Wind Speed Data' in features[0][i][3]:
                for x in features[0][2]:
                    speed_line.append([[x[0], x[1]], x[2]])
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Wind Speed"]) = {
                    "Observations Along Line": speed_line
                }
        return OUTDATA

    if poly:
        OUTDATA = {"type": "FeatureCollection",
                   "features": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": features[0][0][4],
                        },
                        "properties": {
                            "Forecast Hours": []
                        }
                    }
                   }

        for hour in forecast_hours:
            OUTDATA["features"]['properties']["Forecast Hours"].append({
                "Forecast Hour": hour,
            })
        for i in features[0]:
            if 'Temperature Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Temperature"]) = {
                    "Min Temperature": features[0][i][0],
                    "Max Temperature": features[0][i][1],
                    "Mean Temperature": features[0][i][2]
                }
            if 'Wind Direction Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Wind Direction"]) = {
                    "Min Wind Direction": features[0][i][0],
                    "Max Wind Direction": features[0][i][1],
                    "Mean Wind Direction": features[0][i][2]
                }
            if 'Wind Speed Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Wind Speed"]) = {
                    "Min Wind Speed": features[0][i][0],
                    "Max Wind Speed": features[0][i][1],
                    "Mean Wind Speed": features[0][i][2]
                }
        return OUTDATA

    if point:
        OUTDATA = {"type": "FeatureCollection",
                   "features": {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [features[0][0][0],
                                            features[0][0][1]],
                        },
                        "properties": {
                            "Forecast Hours": []
                        }
                    }
                   }
        for hour in forecast_hours:
            OUTDATA["features"]['properties']["Forecast Hours"].append({
                "Forecast Hour": hour,
            })
        for i in features[0]:
            if 'Temperature Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Temperature"]) = {
                    "Temperature": features[0][i][2],
                }
            if 'Wind Direction Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Wind Direction"]) = {
                    "Wind Direction": features[0][i][2],
                }
            if 'Wind Speed Data' in features[0][i][3]:
                (OUTDATA["features"]['properties']["Forecast Hours"]
                 [int(i/3)]["Wind Speed"]) = {
                    "Wind Speed": features[0][i][2],
                }

        return OUTDATA


@click.command()
@click.pass_context
@click.option('--model', 'model', help='which type of raster')
@click.option('--forecast_hours_', 'forecast_hours_',
              help='Forecast hours to extract from')
@click.option('--model_run', 'model_run',
              help='model run to use for the time series')
@click.option('--input_geojson', 'input_geojson', help='shape to clip by')
def extract_raster(ctx, model, forecast_hours_, model_run, input_geojson):
    output_geojson = extract_raster_main(
        model, forecast_hours_, model_run, input_geojson)

    return output_geojson

    if output_geojson is not None:
        click.echo(json.dumps(output_geojson))


def extract_raster_main(model, forecast_hours_, model_run, input_geojson):
    """
    gets list of files and calls appropriate clipping function
    based on query type
    :param forecast_hours_: forcast hour datetime
    :param model_run: model run
    :param input_geojson: geojson file containing geometry of query
    :returns: dict with values as clipped points, original point geometry,
    and query type
    """
    var_list = ['TT', 'WD', 'WSPD']
    layers = []

    if model.upper() == 'HRDPS':
        model = 'HRDPS.CONTINENTAL_{}'

    for layer in var_list:
        layers.append(model.format(layer))

    result = get_files(layers, forecast_hours_, model_run)

    raster_list = []
    forecast_hours = set()
    for element in result:
        raster_list.append(element["filepath"])
        forecast_hours.add(element["forecast_hour"])
    forecast_hours = list(forecast_hours)
    forecast_hours.sort()
    raster_list.sort()

    features = []
    poly = False
    line = False
    point = False

    for feature in input_geojson['features']:
        if feature['geometry']['type'] in ["Polygon", "MultiPolygon"]:
            poly = True
            features.append(summ_stats_poly(raster_list, input_geojson))
            break
        elif feature['geometry']['type'] in ["LineString", "MultiLineString"]:
            line = True
            features.append(get_line(raster_list, input_geojson))
            break
        elif feature['geometry']['type'] in ["Point", "MultiPoint"]:
            point = True
            features.append(get_point(raster_list, input_geojson))

    output_geojson = write_output(features, forecast_hours, poly, line, point)

    return output_geojson


try:
    from pygeoapi.process.base import BaseProcessor

    class ExtractRasterProcessor(BaseProcessor):
        """Extract Raster Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.cccs.raster_drill.RasterDrillProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data), outputs=None):
            model = data["model"]
            forecast_hours_ = data["forecast_hours_"]
            model_run = data["model_run"]
            input_geojson = data["input_geojson"]

            output_geojson = extract_raster_main(
                model, forecast_hours_, model_run, input_geojson)
            return 'application/json', output_geojson

        def __repr__(self):
            return f'<ExtractRasterProcessor> {self.name}'

except (ImportError, RuntimeError) as err:
    LOGGER.warning(f'Import errors: {err}')
