# =================================================================
#
# Authors: Tom Cooney <tom.cooney@canada.ca>
#
# Copyright (c) 2020 Tom Cooney
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
import numpy as np
from osgeo import gdal, osr
from pyproj import Transformer
import rasterio
import rasterio.mask
from rasterio.io import MemoryFile

import dask
import dask.distributed
import dask.array as da

DASK_N_WORKERS = 10
DASK_N_THREADS = 1

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
        'href': 'https://canada.ca/climate-services',
        'hreflang': 'en-CA'
    }, {
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://canada.ca/services-climatiques',
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

OUTDATA = {}
OUTDATA['metadata'] = []
OUTDATA['metadata'].append({
    "Temporal Resolution": "1",
    "Temporal Units": "1",
    "Temperature Units": "degrees C",
    "Wind Direction Units": "Wind direction (from which blowing) [deg true]",
    "Wind Speed Units": "m/s"
})


def get_files(layers, fh, mr):
    """
    ES search to find files names
    :param layers: arrays of three layers
    :param fh: forcast hour datetime
    :param mr: model run
    :return: files : arrays of threee file paths
    """
    es = Elasticsearch(['localhost:9200'])
    list_files = []

    for layer in layers:
        for time_ in fh.split(','):
            files = {}
            s_object = {
                'query': {
                    'bool': {
                        'must': {
                            'match': {'properties.layer.raw': layer}
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

                except IndexError as error:
                    msg = 'invalid input value: {}' .format(error)
                    LOGGER.error(msg)
                    return None, None

            except exceptions.ElasticsearchException as error:
                msg = 'ES search failed: {}' .format(error)
                LOGGER.error(msg)
                return None, None
    return list_files


def xy2geo(_x, _y, ds):
    """
    transforms x/y pixel values to geographic coordinate
    :param _x: x coordinate
    :param _y: y coordinate
    :param ds: GDAL dataset object
    :returns: list of geographic coordinate
    """
    geotransform = ds.GetGeoTransform()
    origin_x = geotransform[0]
    origin_y = geotransform[3]
    width = geotransform[1]
    height = geotransform[5]
    final_x = (_x - origin_x) / width
    final_y = (_y - origin_y) / height
    return [final_x, final_y]


def setup_xy2geo(x, y, inputSRS_wkt, ds):
    """
    does the pre-processing for xy2geo transformations
    :param x: x coordinate
    :param y: y coordinate
    :param inputSRS_wkt: well known text crs decsription
    :param ds: GDAL dataset object
    :returns: list of geographic coordinate
    """
    srs = osr.SpatialReference()
    srs.ImportFromWkt(inputSRS_wkt)
    transformer = Transformer.from_crs("epsg:4326", srs.ExportToProj4())
    _x, _y = transformer.transform(x, y)
    final_x, final_y = xy2geo(_x, _y, ds)
    return [final_x, final_y]


def reproject_line(input_geojson, raster_path):
    """
    transform xy coordinates to geo coordinates and wrap in line format
    :param input_geojson: geojson file containing geometry of line
    :param raster_path: path to queried raster file on disk
    :returns: list of geographic coordinate
    """
    to_return = []
    try:
        ds = gdal.Open(raster_path)
    except FileNotFoundError as err:
        LOGGER.debug(err)
    inputSRS_wkt = ds.GetProjection()

    for geom in range(0, len(input_geojson['features'])):
        for point in (input_geojson['features'][geom]
                      ['geometry']['coordinates']):
            x = point[1]
            y = point[0]

            to_return.append(setup_xy2geo(x, y, inputSRS_wkt, ds))
    return to_return


def reproject_poly(input_geojson, raster_path):
    """
    transform xy coordinates to geo coordinates and wrap in polygon format
    :param input_geojson: geojson file containing geometry of polygon
    :param raster_path: path to queried raster file on disk
    :returns: list of geographic coordinate
    """
    ret = []
    to_return = []
    try:
        ds = gdal.Open(raster_path)
    except FileNotFoundError as err:
        LOGGER.debug(err)
    inputSRS_wkt = ds.GetProjection()

    for geom in range(0, len(input_geojson['features'])):
        for line in range(
            0, len(
                input_geojson['features'][geom]['geometry']['coordinates'])):
            for point in range(0, len((input_geojson['features'][geom]
                                       ['geometry']['coordinates'][line]))):
                x = (input_geojson['features'][geom]['geometry']
                     ['coordinates'][line][point][1])
                y = (input_geojson['features'][geom]['geometry']
                     ['coordinates'][line][point][0])

                ret.append(setup_xy2geo(x, y, inputSRS_wkt, ds))
    to_return.append(ret)
    return to_return


def reproject_point(input_geojson, raster_path):
    """
    transform xy coordinates to geo coordinates and wrap in point format
    :param input_geojson: geojson file containing geometry of point
    :param raster_path: path to queried raster file on disk
    :returns: list of geographic coordinate
    """
    to_return = []
    try:
        ds = gdal.Open(raster_path)
    except FileNotFoundError as err:
        LOGGER.debug(err)
    inputSRS_wkt = ds.GetProjection()

    for geom in range(0, len(input_geojson['features'])):
        y, x = input_geojson['features'][geom]['geometry']['coordinates']
        to_return.append(setup_xy2geo(x, y, inputSRS_wkt, ds))
    return to_return


def geo2xy(ds, x, y):
    """
    transforms geographic coordinate to x/y pixel values
    :param ds: GDAL dataset object
    :param x: x coordinate
    :param y: y coordinate
    :returns: list of x/y pixel values
    """
    geotransform = ds.GetGeoTransform()
    origin_x = geotransform[0]
    origin_y = geotransform[3]
    width = geotransform[1]
    height = geotransform[5]
    _x = int((x * width) + origin_x)
    _y = int((y * height) + origin_y)
    return [_x, _y]


def setup_geo2xy(x, y, raster_path):
    """
    does the pre-processing for geo2xy transformations
    :param ds: GDAL dataset object
    :param x: x coordinate
    :param y: y coordinate
    :returns: list of x/y pixel values
    """
    try:
        ds = gdal.Open(raster_path)
    except FileNotFoundError as err:
        LOGGER.debug(err)
    inputSRS_wkt = ds.GetProjection()
    srs = osr.SpatialReference()
    srs.ImportFromWkt(inputSRS_wkt)
    _x, _y = geo2xy(ds, x, y)
    transformer = Transformer.from_proj(srs.ExportToProj4(), "epsg:4326")
    final_x, final_y = transformer.transform(_x, _y)
    return [final_x, final_y]


def get_point(raster_list, input_geojson):
    """
    clips a raster by a point
    :param input_geojson: geojson file containing geometry of point
    :param raster_path: path to queried raster file on disk
    :returns: dict with values of clipped points, original point
    geometry, and query type
    """
    in_coords = input_geojson['features'][0]['geometry']['coordinates']
    coords = reproject_point(input_geojson, raster_list[0])

    files_point = { i: get_file_point(raster_list[i], in_coords, coords) for i in range(len(raster_list)) }
    result = dask.compute(files_point)[0]

    return result

    """
    to_return = {}
    i = 0

    in_coords = input_geojson['features'][0]['geometry']['coordinates']

    coords = reproject_point(input_geojson, raster_list[0])

    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"
        try:
            x = int(coords[0][0])
            y = int(coords[0][1])
            ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
            band = ds.GetRasterBand(1)
            arr = band.ReadAsArray()
            to_return[i] = [in_coords[0], in_coords[1], arr[y][x], data_type]
        except FileNotFoundError as err:
            LOGGER.debug(err)
        i += 1

    return to_return
    """
    

@dask.delayed
def get_file_point(raster_path, in_coords, coords):
    if "TMP" in raster_path:
        data_type = "Temperature Data"
    if "WDIR" in raster_path:
        data_type = "Wind Direction Data"
    if "WIND" in raster_path:
        data_type = "Wind Speed Data"
    try:
        x = int(coords[0][0])
        y = int(coords[0][1])
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray()
        return [in_coords[0], in_coords[1], arr[y][x], data_type]
    except FileNotFoundError as err:
        LOGGER.debug(err)


def get_line(raster_list, input_geojson):
    """
    clips a raster by a line
    :param input_geojson: geojson file containing geometry of line
    :param raster_path: path to queried raster file on disk
    :returns: dict with values along clipped lines, original line
    geometry, and query type
    """
    input_line = input_geojson['features'][0]['geometry']['coordinates']
    input_line = str(input_line).replace(" ", "")

    shapes = []
    shapes.append({
        'type': 'LineString',
        'coordinates': reproject_line(input_geojson, raster_list[0])
    })

    files_line = { i: get_file_line(raster_list[i], input_line, shapes) for i in range(len(raster_list)) }
    result = dask.compute(files_line)[0]

    return result

    """
    to_return = {}
    i = 0
    input_line = input_geojson['features'][0]['geometry']['coordinates']
    input_line = str(input_line).replace(" ", "")
    shapes = []
    shapes.append({
        'type': 'LineString',
        'coordinates': reproject_line(input_geojson, raster_list[0])
    })
    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"

        try:
            with rasterio.open(raster_path) as src:
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
                        to_return[i] = [ds, data_type, input_line]
                        i += 1
        except FileNotFoundError as err:
            LOGGER.debug(err)
    return to_return
    """


@dask.delayed
def get_file_line(raster_path, input_line, shapes):
    if "TMP" in raster_path:
        data_type = "Temperature Data"
    if "WDIR" in raster_path:
        data_type = "Wind Direction Data"
    if "WIND" in raster_path:
        data_type = "Wind Speed Data"

    try:
        with rasterio.open(raster_path) as src:
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
                    return [ds, data_type, input_line]
    except FileNotFoundError as err:
        LOGGER.debug(err)


def summ_stats_poly(raster_list, input_geojson):
    """
    clips a raster by a polygon
    :param input_geojson: geojson file containing geometry of polygon
    :param raster_path: path to queried raster file on disk
    :returns: dict with min, max, and mean value for each query type
    and each forecast hour
    """
    shapes = []
    shapes.append({
        'type': 'Polygon',
        'coordinates': reproject_poly(input_geojson, raster_list[0])
    })

    files_poly = { i: get_file_poly(raster_list[i], shapes) for i in range(len(raster_list)) }
    result = dask.compute(files_poly)[0]

    return result

    """
    to_return = {}
    i = 0

    shapes = []
    shapes.append({
        'type': 'Polygon',
        'coordinates': reproject_poly(input_geojson, raster_list[0])
    })

    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"

        try:
            with rasterio.open(raster_path) as src:
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
                        min_val = np.min(ds, axis=None)
                        max_val = np.max(ds, axis=None)
                        mean_val = np.mean(ds, axis=None)
                        to_return[i] = [min_val, max_val, mean_val, data_type]
                        i += 1
        except FileNotFoundError as err:
            LOGGER.debug(err)
    return to_return
    """


@dask.delayed
def get_file_poly(raster_path, shapes):
    if "TMP" in raster_path:
        data_type = "Temperature Data"
    if "WDIR" in raster_path:
        data_type = "Wind Direction Data"
    if "WIND" in raster_path:
        data_type = "Wind Speed Data"

    try:
        with rasterio.open(raster_path) as src:
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
                    min_val = np.min(ds, axis=None)
                    max_val = np.max(ds, axis=None)
                    mean_val = np.mean(ds, axis=None)
                    return [min_val, max_val, mean_val, data_type]
    except FileNotFoundError as err:
        LOGGER.debug(err)


def poly_out(string_name, forecast_hour, value):
    """
    formats polygon output
    :param string_name: name of output field
    :param forecast_hour: forecast hour of output
    :param value: value of output
    :returns: dict forecast hour, field name, and value
    """
    to_return = []
    to_return.append({
        'Forecast Hour': forecast_hour,
        string_name: value
    })
    return to_return


def point_out(string_name, forecast_hour, key):
    """
    formats point output
    :param string_name: name of output field
    :param forecast_hour: forecast hour of output
    :param key: value of output
    :returns: dict forecast hour, field name, and key
    """
    to_return = []
    to_return.append({
        "Forecast Hour": forecast_hour,
        string_name: key
    })
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
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": features[0][1][2]
        })
        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []

        for item in features:
            for key in item.keys():
                if "Temperature Data" in item[key][1]:
                    OUTDATA['Temperature Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Temperature Observation': item[key][0].tolist()
                    })
                if "Wind Direction Data" in item[key][1]:
                    OUTDATA['Wind Direction Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Wind Direction Observation': item[key][0].tolist()
                    })
                if "Wind Speed Data" in item[key][1]:
                    OUTDATA['Wind Speed Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Wind Speed Observation': item[key][0].tolist()
                    })
                i += 1

        return OUTDATA

    if poly:
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": None
        })

        OUTDATA['Min Temperature Data'] = []
        OUTDATA['Max Temperature Data'] = []
        OUTDATA['Mean Temperature Data'] = []

        OUTDATA['Min Wind Direction Data'] = []
        OUTDATA['Max Wind Direction Data'] = []
        OUTDATA['Mean Wind Direction Data'] = []

        OUTDATA['Min Wind Speed Data'] = []
        OUTDATA['Max Wind Speed Data'] = []
        OUTDATA['Mean Wind Speed Data'] = []
        for item in features:
            for key in item.keys():
                if 'Temperature Data' in item[key][3]:
                    OUTDATA['Min Temperature Data'].append(
                        poly_out("Min Temperature", forecast_hours[i],
                                 item[key][0]))
                    OUTDATA['Max Temperature Data'].append(
                        poly_out("Max Temperature", forecast_hours[i],
                                 item[key][1]))
                    OUTDATA['Mean Temperature Data'].append(
                        poly_out("Mean Temperature", forecast_hours[i],
                                 item[key][2]))
                if 'Wind Direction Data' in item[key][3]:
                    OUTDATA['Min Wind Direction Data'].append(
                        poly_out("Min Wind Direction", forecast_hours[i],
                                 item[key][0]))
                    OUTDATA['Max Wind Direction Data'].append(
                        poly_out("Max Wind Direction", forecast_hours[i],
                                 item[key][1]))
                    OUTDATA['Mean Wind Direction Data'].append(
                        poly_out("Mean Wind Direction", forecast_hours[i],
                                 item[key][2]))
                if 'Wind Speed Data' in item[key][3]:
                    OUTDATA['Min Wind Speed Data'].append(
                        poly_out("Min Wind Speed", forecast_hours[i],
                                 item[key][0]))
                    OUTDATA['Max Wind Speed Data'].append(
                        poly_out("Max Wind Speed", forecast_hours[i],
                                 item[key][1]))
                    OUTDATA['Mean Wind Speed Data'].append(
                        poly_out("Mean Wind Speed", forecast_hours[i],
                                 item[key][2]))
                i += 1
        return OUTDATA

    if point:
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": (features[0][1][0], features[0][1][1])
        })

        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []
        for item in features:
            for key in item.keys():
                if i >= len(item.keys()):
                    break
                if 'Temperature Data' in item[key][3]:
                    OUTDATA['Temperature Data'].append(
                        point_out(
                            "Temperature Observation",
                            forecast_hours[i],
                            item[key][2]))
                if 'Wind Direction Data' in item[key][3]:
                    OUTDATA['Wind Direction Data'].append(
                        point_out(
                            "Wind Direction Observation",
                            forecast_hours[i],
                            item[key][2]))
                if 'Wind Speed Data' in item[key][3]:
                    OUTDATA['Wind Speed Data'].append(
                        point_out(
                            "Wind Speed Observation",
                            forecast_hours[i],
                            item[key][2]))
                i += 1
        return OUTDATA


@click.command('extract-raster')
@click.pass_context
@click.option('--model', 'model', help='which type of raster')
@click.option('--forecast_hours_', 'forecast_hours_',
              help='Forecast hours to extract from')
@click.option('--model_run', 'model_run',
              help='model run to use for the time series')
@click.option('--input_geojson', 'input_geojson', help='shape to clip by')
def cli(ctx, model, forecast_hours_, model_run, input_geojson):
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
    forecast_hours = []
    for element in result:
        raster_list.append(element["filepath"])
        forecast_hours.append(element["forecast_hour"])

    features = []
    poly = False
    line = False
    point = False

    cluster = dask.distributed.LocalCluster(processes=True, n_workers=DASK_N_WORKERS, threads_per_worker=DASK_N_THREADS)
    client = dask.distributed.Client(cluster)

    for feature in input_geojson['features']:
        if (feature['geometry']['type'] == "Polygon" or
                feature['geometry']['type'] == "MultiPolygon"):
            poly = True
            features.append(summ_stats_poly(raster_list, input_geojson))
            break
        elif (feature['geometry']['type'] == "LineString" or
                feature['geometry']['type'] == "MultiLineString"):
            line = True
            features.append(get_line(raster_list, input_geojson))
            break
        elif (feature['geometry']['type'] == "Point" or
                feature['geometry']['type'] == "MultiPoint"):
            point = True
            features.append(get_point(raster_list, input_geojson))

    client.close()
    cluster.close()

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

        def execute(self, data):
            model = data["model"]
            forecast_hours_ = data["forecast_hours_"]
            model_run = data["model_run"]
            input_geojson = data["input_geojson"]

            output_geojson = extract_raster_main(
                model, forecast_hours_, model_run, input_geojson)

            return output_geojson

        def __repr__(self):
            return '<ExtractRasterProcessor> {}'.format(self.name)

except (ImportError, RuntimeError):
    pass
