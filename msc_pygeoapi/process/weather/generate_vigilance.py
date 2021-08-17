# =================================================================
#
# Author: Julien Roy-Sabourin
#         <julien.roy-sabourin.eccc@gccollaboration.ca>
#         Louis-Philippe Rousseau-Lambert
#         <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2020 Julien Roy-Sabourin
#               2021 Louis-Philippe Rousseau-Lambert
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
from datetime import datetime
from io import BytesIO
import json
import logging
import os

import click
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from elasticsearch import exceptions, logger as elastic_logger
from matplotlib.colors import ListedColormap
import matplotlib.image as image
from matplotlib.offsetbox import AnchoredText, OffsetImage, AnnotationBbox
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from osgeo import gdal, osr
from PIL import Image

from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_LOGGING_LOGLEVEL,
    MSC_PYGEOAPI_ES_URL,
    MSC_PYGEOAPI_ES_USERNAME,
    MSC_PYGEOAPI_ES_PASSWORD,
    MSC_PYGEOAPI_BASEPATH
)
from msc_pygeoapi.util import (
    configure_es_connection
)

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

ES_INDEX = 'geomet-data-registry-nightly'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
CANADA_BBOX = '-140, 35, -44, 83'
LAMBERT_BBOX = [-170, 15, -40, 90]
COLOR_MAP = [[1, 1, 1, 1],
             [1, 1, 0, 1],
             [1, 0.5, 0, 1],
             [1, 0, 0, 1]]

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'generate-vigilance',
    'title': {
        'en': 'Generate vigilance product',
        'fr': 'Génération de produit de vgilance'
    },
    'description': {
        'en': 'Generate vigilance process for weather data',
        'fr': 'Génération de produit de vigilance '
              'pour les données météorologiques'
    },
    'keywords': ['generate vigilance weather'],
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
    'inputs': {
        'layers': {
            'title': '3 layers to produce vigilence',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'forecast-hour': {
            'title': 'forecast hour to use',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'model-run': {
            'title': 'model run to use',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'bbox': {
            'title': 'bounding box',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'format': {
            'title': 'output format: PNG, GeoTiff or GeoPNG',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'generate-vigilance-response': {
            'title': 'output WMS vigilance product',
            'schema': {
                'oneOf': [{
                    'contentMediaType': 'image/png'
                }, {
                    'contentMediaType': 'application/json'
                }, {
                    'contentMediaType': 'image/tiff'
                }]
                }
            }
    },
    'example': {
        'inputs': {
            "layers": "GEPS.DIAG.24_T8.ERGE15," +
            "GEPS.DIAG.24_T8.ERGE20,GEPS.DIAG.24_T8.ERGE25",
            "forecast-hour": "2021-08-20T00:00:00Z",
            "model-run": "2021-08-18T00:00:00Z",
            "bbox": CANADA_BBOX,
            "format": "PNG"
        }
    }
}


def convert_bbox(bbox):
    """
    validate and convert (string to float) the bounding box

    param bbox : bounding box to validate/convert

    return : bbox : valid and float converted bounding box
    """
    for index, item in enumerate(bbox):
        bbox[index] = float(item)

    if all((bbox[0] >= -180,
            bbox[0] < bbox[2],
            bbox[2] <= 180,
            bbox[1] >= -90,
            bbox[1] < bbox[3],
            bbox[3] <= 90)):
        return bbox

    return None


def valid_layer(layers):
    """
    validate the layers and return the layer data

    param layers : layers to validate

    return : sufix : (ERGE or ERLE)
             model : GEPS or REPS
             thresholds : list of thresholds
    """

    prefix = []
    sufix = []
    models = []
    tresholds = []
    for layer in layers:

        layer_ = layer.split('.')
        model = layer_[0]
        prefix_ = layer_[2]
        sufix_ = layer_[3]
        sufix_ = sufix_[0:4]

        pos = layer_[3].rfind('E')
        if len(layer_) == 5:
            treshold = float(layer_[3][pos + 1:] + '.' + layer_[4])
        else:
            treshold = int(layer_[3][pos + 1:])

        prefix.append(prefix_)
        sufix.append(sufix_)
        models.append(model)
        tresholds.append(treshold)

        if sufix_ == 'ERGE' or sufix_ == 'ERLE':
            pass
        else:
            LOGGER.error('invalid layer type, need to be ERGE or ERLE')
            return None, None, None

    if prefix[1:] == prefix[:-1] and sufix[1:] == sufix[:-1]:
        return sufix[0], models[0], tresholds

    LOGGER.error('invalid layers, weather variables need to match')
    return None, None, None


def get_files(layers, fh, mr):

    """
    ES search to find files names

    param layers : arrays of three layers
    param fh : forcast hour datetime
    param mr : model run

    return : files : arrays of threee file paths
    """

    conn_config = configure_es_connection(MSC_PYGEOAPI_ES_URL,
                                          MSC_PYGEOAPI_ES_USERNAME,
                                          MSC_PYGEOAPI_ES_PASSWORD)
    es = ElasticsearchConnector(conn_config)

    files = []
    weather_variables = []

    for layer in layers:

        s_object = {
            'query': {
                'bool': {
                    'must': {
                        'match': {'properties.layer.raw': layer}
                    },
                    'filter': [
                        {'term': {'properties.forecast_hour_datetime':
                                  fh.strftime(DATE_FORMAT)}},
                        {'term': {'properties.reference_datetime':
                                  mr.strftime(DATE_FORMAT)}}
                    ]
                }
            }
        }

        try:
            res = es.search(s_object, ES_INDEX)

            try:
                files.append(res['hits']['hits'][0]
                             ['_source']['properties']['filepath'])
                weather_variables.append(res['hits']['hits'][0]
                                         ['_source']['properties']
                                         ['weather_variable'][0])

            except IndexError as error:
                msg = 'invalid input value: {}' .format(error)
                LOGGER.error(msg)
                return None, None

        except exceptions.ElasticsearchException as error:
            msg = 'ES search failed: {}' .format(error)
            LOGGER.error(msg)
            return None, None
    return files, weather_variables


def get_bands(files):

    """
    extract the band number from the file path
    only works for vrt files

    param files : arrays of three file paths

    return : paths : file paths
             bands : grib bands numbers
    """

    p_tempo = files[0].split('?')
    p = p_tempo[0]
    paths = p[6:]
    bands = []

    for file_ in files:
        b = file_.split('=')
        bands.append(int(b[-1]))
    return paths, bands


def read_croped_array(band, geotransform, bbox):
    """
    create a array within the bbox from the grib band

    param band : grib band
    param geatransform : geographic info of the band
    param bbox : bounding box

    return : array : cropped array
    """

    xinit = geotransform[0]
    yinit = geotransform[3]

    xsize = geotransform[1]
    ysize = geotransform[5]

    p1 = (bbox[0], bbox[3])
    p2 = (bbox[2], bbox[1])

    row1 = int((p1[1] - yinit)/ysize)
    col1 = int((p1[0] - xinit)/xsize)

    row2 = int((p2[1] - yinit)/ysize)
    col2 = int((p2[0] - xinit)/xsize)

    array = band.ReadAsArray(col1, row1, col2 - col1 + 1, row2 - row1 + 1)
    return array


def get_new_array(path, bands, bbox):

    """
    combines 3 file into one array for vigilance

    param paths : arrays of three file paths
    param band : array of three grib band number

    return : max_array : the combined array for vigilance
    """

    try:
        ds = gdal.Open(path)
    except RuntimeError as err:
        msg = 'Cannot open file: {}, assigning NA'.format(err)
        LOGGER.error(msg)

    geotransform = ds.GetGeoTransform()
    band = bands[0]
    srcband = ds.GetRasterBand(band)
    array1 = read_croped_array(srcband, geotransform, bbox)
    array1[array1 < 40] = 0
    array1[(array1 >= 40) & (array1 < 60)] = 1
    array1[array1 >= 60] = 1

    band = bands[1]
    srcband = ds.GetRasterBand(band)
    array2 = read_croped_array(srcband, geotransform, bbox)
    array2[(array2 >= 1) & (array2 < 20)] = 1
    array2[(array2 >= 20) & (array2 < 40)] = 1
    array2[(array2 >= 40) & (array2 < 60)] = 2
    array2[array2 >= 60] = 2

    band = bands[2]
    srcband = ds.GetRasterBand(band)
    array3 = read_croped_array(srcband, geotransform, bbox)
    array3[(array3 >= 1) & (array3 < 20)] = 1
    array3[(array3 >= 20) & (array3 < 40)] = 2
    array3[(array3 >= 40) & (array3 < 60)] = 2
    array3[array3 >= 60] = 3

    max_array = np.maximum(array1, array2)
    max_array = np.maximum(max_array, array3)
    return max_array


def find_best_projection(bbox):
    """
    find whether the LCC or the plateCarree projection is better
    according to the bbox

    param bbox : bounding box

    return : project : best projection for the given bbox
    """

    project = ccrs.PlateCarree()

    if bbox[0] >= LAMBERT_BBOX[0] and bbox[0] <= LAMBERT_BBOX[2]:
        if bbox[2] >= LAMBERT_BBOX[0] and bbox[2] <= LAMBERT_BBOX[2]:
            if bbox[1] >= LAMBERT_BBOX[1] and bbox[1] <= LAMBERT_BBOX[3]:
                if bbox[3] >= LAMBERT_BBOX[1] and bbox[3] <= LAMBERT_BBOX[3]:
                    project = ccrs.LambertConformal()

    return project


def get_data_text(variable, tresholds, mr, model, fh):

    """
    Provide the text string of the metedata for the png output

    param variable : weather variable
    param tresholds : list of 3 user specified thresholds
    param mr : model run
    param model : GEPS or REPS
    param fh : forcast hour

    return : textstr : formated string for the png
    """
    mr = mr.strftime(DATE_FORMAT)
    fh = fh.strftime(DATE_FORMAT)
    trh = [tresholds[0], tresholds[1], tresholds[2]]
    textstr = '\n'.join(('{} {} - {}'. format(variable, trh, model),
                         'Émis/Issued: {} '.format(mr),
                         'Prévision/Forecast: {} '.format(fh)))

    return textstr


def add_basemap(data, bbox, textstr):
    """
    add the basemap spacified by the bbox to the vigilance data

    param data : vigilance data
    param bbox : geo exetent of the data

    return : map : png in bytes of the produced vigilance map
    with the bsaemap
    """

    # adding vigilance data
    project = find_best_projection(bbox)
    ny, nx = data.shape
    lons = np.linspace(bbox[0], bbox[2], nx)
    lats = np.linspace(bbox[3], bbox[1], ny)
    lons, lats = np.meshgrid(lons, lats)
    ax = plt.axes(projection=project)

    max_ = int(np.amax(data)) + 1
    colors = ListedColormap(COLOR_MAP[0:max_])
    plt.contourf(lons, lats, data, max_, transform=ccrs.PlateCarree(),
                 cmap=colors)

    # adding the basemap
    ax.coastlines(linewidth=0.35)
    ax.add_feature(cfeature.BORDERS, linestyle='-',
                   edgecolor='black',
                   linewidth=0.35)
    state = cfeature.NaturalEarthFeature(category='cultural',
                                         name='admin_1_states_provinces_lines',
                                         scale='50m', facecolor='none')
    ax.add_feature(state, edgecolor='black', linewidth=0.35)

    # adding vigilance metadata
    text_box = AnchoredText(textstr, frameon=True, loc=4, pad=0.5,
                            borderpad=0.05, prop={'size': 5})
    plt.setp(text_box.patch, facecolor='white', alpha=1, linewidth=0.35)
    ax.add_artist(text_box)

    # adding the logo
    im_path = os.path.join(MSC_PYGEOAPI_BASEPATH,
                           'resources/images/eccc-logo.png')
    im = image.imread(im_path)
    imagebox = OffsetImage(im, zoom=0.5, filternorm=True, filterrad=4.0,
                           resample=False, dpi_cor=False)
    ab = AnnotationBbox(imagebox, (0.003, 0.996), xycoords=ax.transAxes,
                        frameon=True, box_alignment=(0, 1), pad=0.1)
    plt.setp(ab.patch, linewidth=0.35)
    ab.set_zorder(10)
    ax.add_artist(ab)

    # adding the legend
    str1 = 'Be aware / Soyez Attentif'
    str2 = 'Be prepared / Soyez très vigilant'
    str3 = 'Be extra cautious / Vigilance absolue'
    y_patch = mpatches.Patch(color='yellow', label=str1)
    o_patch = mpatches.Patch(color='orange', label=str2)
    r_patch = mpatches.Patch(color='red', label=str3)
    leg = plt.legend(handles=[y_patch, o_patch, r_patch], loc='lower left',
                     bbox_to_anchor=(0, 0), fancybox=False, fontsize=4,
                     framealpha=1, borderaxespad=0.05, edgecolor='black',
                     borderpad=0.5)
    leg_frame = leg.get_frame()
    plt.setp(leg_frame, linewidth=0.35)

    buffer = BytesIO()
    plt.savefig(buffer, bbox_inches='tight', dpi=200, format='png')
    buffer.seek(0)

    return buffer


def get_geotiff(data, bbox, path):
    """
    transform the vigilance numpy array into a Geotiff file

    param data : vigilance array
    param bbox : bounding box

    return : buffer : buffer of the geoTiff bytes
    """

    driver = gdal.GetDriverByName('GTiff')
    ds = gdal.Open(path)
    ysize, xsize = data.shape

    driver.Create('/vsimem/vigi', xsize, ysize, 1, gdal.GDT_Byte)
    ds_ = gdal.Open('/vsimem/vigi', gdal.GA_Update)
    srs = osr.SpatialReference()
    wkt = ds.GetProjection()
    srs.ImportFromWkt(wkt)
    ds_.SetProjection(srs.ExportToWkt())
    gt = ds.GetGeoTransform()
    gt = (bbox[0], gt[1], gt[2], bbox[3], gt[4], gt[5])
    ds_.SetGeoTransform(gt)

    outband = ds_.GetRasterBand(1)
    outband.SetStatistics(np.min(data), np.max(data),
                          np.average(data), np.std(data))
    outband.WriteArray(data)

    driver.CreateCopy('/vsimem/vigi', ds_)

    file_buffer = gdal.VSIGetMemFileBuffer_unsafe('/vsimem/vigi')
    buffer = BytesIO()
    buffer.write(file_buffer)
    buffer.seek(0)
    gdal.Unlink('/vsimem/vigi')
    ds_ = None
    return buffer


def get_geopng(data, bbox):
    """
    transform the vigilance numpy array into Ge oPNG

    param data : vigilance array
    param bbox : bounding box

    return : buffer : buffer of the geoPng bytes

    """
    x_pixel_dist = (bbox[2] - bbox[0])/data.shape[1]
    y_pixel_dist = -1 * (bbox[3] - bbox[1])/data.shape[0]
    x_top_left = bbox[0]
    y_top_left = bbox[3]

    pgw = []
    pgw.append(x_pixel_dist)
    pgw.append(0)
    pgw.append(0)
    pgw.append(y_pixel_dist)
    pgw.append(x_top_left)
    pgw.append(y_top_left)

    # for generating the .pgw
    '''
    f = open("vigi.pgw", "w")
    for line in pgw:
        f.write(str(line) + '\n')
    f.close()
    '''

    color = np.zeros((data.shape[0], data.shape[1], 3))
    color[data == 0] = [255, 255, 255]
    color[data == 1] = [246, 255, 0]
    color[data == 2] = [255, 160, 0]
    color[data == 3] = [255, 0, 0]

    im = Image.fromarray(np.uint8(color), 'RGB')
    b = BytesIO()
    im.save(b, format='PNG')

    output = {
        'pgw': pgw,
        'png': b.getvalue()
    }
    return output


def generate_vigilance_main(layers, fh, mr, bbox, format_):
    """
    generate a vigilance file (with specified format)
    according to the thresholds

    param layers : 3 layer of the 3 different thresholds
    param fh : forcast hour
    param mr : model run
    param bbox : bounding box
    param format_ : output format

    return : image_buffer : buffer of the file in bytes
    """

    gdal.UseExceptions()
    bbox = convert_bbox(bbox)
    if bbox is not None:

        if len(layers) == 3:
            sufix, model, tresholds = valid_layer(layers)
            if sufix is None:
                return None

            files, variables = get_files(layers, fh, mr)
            if files is None:
                return None

            if len(files) == 3:
                path, bands = get_bands(files)
                if sufix == 'ERGE':
                    bands.sort()

                if sufix == 'ERLE':
                    bands.sort(reverse=True)

                vigi_data = get_new_array(path, bands, bbox)
                if format_ == 'png':
                    textstr = get_data_text(variables[0], tresholds, mr,
                                            model, fh)
                    png_buffer = add_basemap(vigi_data, bbox, textstr)
                    return png_buffer
                elif format_ == 'geotiff':
                    tiff_buffer = get_geotiff(vigi_data, bbox, path)
                    return tiff_buffer
                elif format_ == 'geopng':
                    geopng_buffer = get_geopng(vigi_data, bbox)
                    return geopng_buffer
                else:
                    LOGGER.error('invalid format')
            else:
                LOGGER.error('invalid layer')
        else:
            LOGGER.error('Invalid number of layers')
    else:
        LOGGER.error('Invalid bbox')
    return None


@click.command('generate-vigilance')
@click.pass_context
@click.option('--layers', 'layers', help='3 layers for vigilance')
@click.option('--forecast-hour', 'fh',
              type=click.DateTime(formats=[DATE_FORMAT]),
              help='Forecast hour to create the vigilance')
@click.option('--model-run', 'mr',
              type=click.DateTime(formats=[DATE_FORMAT]),
              help='model run to use for the time serie')
@click.option('--bbox', 'bbox', default=CANADA_BBOX, help='bounding box')
@click.option('--format', 'format_', help='output format')
def generate_vigilance(ctx, layers, fh, mr, bbox, format_):

    output = generate_vigilance_main(layers.split(','), fh, mr,
                                     bbox.split(','),
                                     format_.lower())
    if output is not None:
        click.echo(json.dumps('vigilance produced, curl via pygeoapi'))
    else:
        return output


try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class GenerateVigilanceProcessor(BaseProcessor):
        """Vigilance product Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns:
            pygeoapi.process.weather.generate_vigilance.GenerateVigilanceProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            layers = data.get('layers')
            fh = datetime.strptime(data.get('forecast-hour'),
                                   DATE_FORMAT)
            mr = datetime.strptime(data.get('model-run'),
                                   DATE_FORMAT)
            bbox = data.get('bbox')
            format_ = data.get('format').lower()

            try:
                output = generate_vigilance_main(layers.split(','),
                                                 fh, mr, bbox.split(','),
                                                 format_)
                if format_ == 'geopng':
                    return 'application/json', output
                elif format_ == 'png':

                    return 'image/png', output.getvalue()
                elif format_ == 'geotiff':
                    return 'image/tiff', output.getvalue()

            except ValueError as err:
                msg = 'Process execution error: {}'.format(err)
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

        def __repr__(self):
            return '<GenerateVigilanceProcessor> {}'.format(self.name)

except (ImportError, RuntimeError):
    pass
