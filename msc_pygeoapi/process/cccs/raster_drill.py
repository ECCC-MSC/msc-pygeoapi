# =================================================================
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>
#
# Copyright (c) 2019 Louis-Philippe Rousseau-Lambert
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
import logging
import os
import re

from osgeo import gdal
from pyproj import Proj, transform
import yaml
from yaml import CLoader

from msc_pygeoapi.process.cccs import (GEOMET_CLIMATE_CONFIG,
                                       GEOMET_CLIMATE_BASEPATH,
                                       GEOMET_CLIMATE_BASEPATH_VRT)

LOGGER = logging.getLogger(__name__)

UNITS = {'PR': '%',
         'TM': 'Celsius',
         'TN': 'Celsius',
         'TX': 'Celsius',
         'TT': 'Celsius',
         'SIC': '%',
         'SIT': 'm',
         'SFCWIND': 'm s-1',
         'SND': 'm'
         }

JSON_TEMPLATE = """{{
  "type": "Feature",
  "geometry": {{
    "type": "Point",
    "coordinates": [{}, {}]
  }},
  "properties": {{
    "time_begin": "{}",
    "time_end": "{}",
    "time_step": "{}",
    "variable_en" "{}",
    "variable_fr" "{}",
    "UOM": "{}",
    "value_type_en": "{}",
    "value_type_fr": "{}",
    "scenario_en": "{}",
    "scenario_fr": "{}",
    "period_en": "{}",
    "period_fr": "{}",
    "percentile_en": "{}",
    "percentile_fr": "{}",
    "label_en": "{}",
    "label_fr": "{}"
    "values":[
{}
    ]
  }}
}}"""


def get_time_info(cfg):
    """
    function to build an array of date based on the yaml info

    :param cfg: CCCS Yaml section for the layer
    :return: dict with the time steps
    """

    LOGGER.debug('creating a Date array')
    dates = []

    time_begin = cfg['climate_model']['temporal_extent']['begin']
    time_end = cfg['climate_model']['temporal_extent']['end']

    if cfg['timestep'] == 'P1Y':
        for i in range(int(time_begin), int(time_end) + 1):
            dates.append(i)

    elif cfg['timestep'] == 'P1M':
        begin_year, begin_month = time_begin.split('-')
        end_year, end_month = time_end.split('-')
        begin = (int(begin_year) * 12) + int(begin_month)
        end = (int(end_year) * 12) + int(end_month)
        for i in range(begin, end + 1):
            year = (i / 12)
            month = (i - (year * 12))
            if month == 0:
                year = year - 1
                month = 12
            time_stamp = '{}-{}'.format(year, str(month).zfill(2))
            dates.append(time_stamp)

    return dates


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

    x = int((x - origin_x) / width) - 1
    y = int((y - origin_y) / height) - 1

    return x, y


def get_location_info(file_, x, y, cfg, layer_keys):
    """
    extract x/y value across all bands of a raster file

    :param file_: filepath of raster data
    :param x: x coordinate
    :param y: y coordinate
    :param cfg: yaml information
    :param layer_keys: layer label splitted

    :return: dict of metadata and array values
    """

    dict_ = {
        'uom': None,
        'metadata': None,
        'time_step': None,
        'values': [],
        'dates': []
    }

    LOGGER.debug('Opening {}'.format(file_))
    try:
        ds = gdal.Open(file_)
        LOGGER.debug('Fetching units')
        dict_['time_step'] = cfg['timestep']

        dict_['metadata'] = layer_keys
        dict_['uom'] = UNITS[layer_keys['Variable']]

        LOGGER.debug('Transforming map coordinates into image coordinates')

        x_, y_ = geo2xy(ds, x, y)

    except RuntimeError as err:
        msg = 'Cannot open file: {}'.format(err)
        LOGGER.exception(msg)

    LOGGER.debug('Running through bands')

    for band in range(1, ds.RasterCount + 1):
        LOGGER.debug('Fetching band {}'.format(band))

        srcband = ds.GetRasterBand(band)
        array = srcband.ReadAsArray()

        try:
            dict_['values'].append(array[y_][x_])
        except IndexError as err:
            msg = 'invalid lat/long value: {}'.format(err)
            LOGGER.exception(msg)

    dict_['dates'] = get_time_info(cfg)

    LOGGER.debug('Freeing dataset object')
    ds = None

    return dict_


def write2format(values_dict, cfg, output_format, lon, lat):
    """
    Writes the information in the format provided by the user

    :param values_dict: result of the get_location_info function
    :param cfg: yaml information
    :param output_format: output format (GeoJSON or CSV)
    :param lon: longitude
    :param lat: latitude

    :return: GeoJSON or CSV output
    """

    time_begin = values_dict['dates'][0]
    time_end = values_dict['dates'][-1]
    time_step = values_dict['time_step']

    file_out = None

    LOGGER.debug('Creating the outpuf file')
    if len(values_dict['dates']) == len(values_dict['values']):
        if output_format == 'CSV':
            column1 = 'time_{}/{}/{}'.format(time_begin,
                                             time_end,
                                             time_step)
            column2 = 'values_{}'.format(values_dict['uom'])

            file_out = '{},{}'.format(column1, column2)

            for i in range(0, len(values_dict['dates'])):
                line = '\n{},{}'.format(values_dict['dates'][i],
                                        values_dict['values'][i])

                file_out += line

        elif output_format == 'GeoJSON':
            if 'CANGRD' not in cfg['label_en']:

                split_en = cfg['label_en'].split('/')
                split_fr = cfg['label_fr'].split('/')
                var_en, sce_en, seas_en, type_en, label_en = split_en

                var_fr, sce_fr, seas_fr, type_fr, label_fr = split_fr

                pctl_en = re.findall(r' \((.*?)\)', label_en)[-1]
                pctl_fr = re.findall(r' \((.*?)\)', label_fr)[-1]
            else:
                type_en, var_en, label_en = cfg['label_en'].split('/')
                type_fr, var_fr, label_fr = cfg['label_fr'].split('/')
                seas_en = re.findall(r' \((.*?)\)', label_en)[0]
                seas_fr = re.findall(r' \((.*?)\)', label_fr)[0]
                sce_en = 'Historical'
                sce_fr = 'Historique'
                pctl_en = pctl_fr = ''

            values = ''
            for k in values_dict['values']:
                if values != '':
                    values += ',\n'
                values += '      {}'.format(k)
            file_out = JSON_TEMPLATE.format(lon,
                                            lat,
                                            time_begin,
                                            time_end,
                                            time_step,
                                            var_en,
                                            var_fr.encode('utf-8'),
                                            values_dict['uom'],
                                            type_en,
                                            type_fr.encode('utf-8'),
                                            sce_en,
                                            sce_fr.encode('utf-8'),
                                            seas_en,
                                            seas_fr.encode('utf-8'),
                                            pctl_en,
                                            pctl_fr.encode('utf-8'),
                                            label_en,
                                            label_fr.encode('utf-8'),
                                            values)

    return file_out


@click.command('raster-drill')
@click.pass_context
@click.option('--layer', help='Layer name to process')
@click.option('--lon', help='Longitude')
@click.option('--lat', help='Latitude')
@click.option('--format', 'format_', type=click.Choice(['JSON', 'CSV']),
              default='JSON', help='output format')
def raster_drill(ctx, layer, lon, lat, format_='JSON'):
    """
    Writes the information in the format provided by the user
    and reads some information from the geomet-climate yaml

    :param layer: layer name
    :param lon: longitude
    :param lat: latitude
    :param format: output format (GeoJSON or CSV)

    :return: return the final file fo a given location
    """
    LOGGER.info('start raster drilling')

    if None in [layer, lon, lat]:
        raise click.ClickException('Missing required parameters')

    with open(GEOMET_CLIMATE_CONFIG) as fh:
        cfg = yaml.load(fh, Loader=CLoader)

    layer = layer
    lon = int(lon)
    lat = int(lat)

    if ('ABS' in layer or 'ANO' in layer and
       layer.startswith('CANGRD') is False):

        keys = ['Model',
                'Variable',
                'Scenario',
                'Period',
                'Type',
                'Percentile']
        values = layer.replace('_', '.').split('.')
        layer_keys = dict(zip(keys, values))

        data_basepath = GEOMET_CLIMATE_BASEPATH

        climate_model_path = cfg['layers'][layer]['climate_model']['basepath']
        file_path = cfg['layers'][layer]['filepath']
        inter_path = os.path.join(climate_model_path, file_path)

        file_name = cfg['layers'][layer]['filename']

    elif 'TREND' not in layer and layer.startswith('CANGRD') is True:
        keys = ['Model', 'Type', 'Variable', 'Period']
        values = layer.replace('_', '.').split('.')
        layer_keys = dict(zip(keys, values))

        data_basepath = GEOMET_CLIMATE_BASEPATH_VRT

        climate_model_path = cfg['layers'][layer]['climate_model']['basepath']
        file_path = cfg['layers'][layer]['filepath']
        inter_path = os.path.join(climate_model_path, file_path)

        file_name = '{}.vrt'.format(cfg['layers'][layer]['filename'])

        src_epsg = '+proj=stere +lat_0=90 +lat_ts=71 +lon_0=0 \
                   +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'
        inProj = Proj(init='epsg:4326')
        outProj = Proj(src_epsg)
        lon, lat = transform(inProj, outProj, lon, lat)

    else:
        msg = '{}: not a valid or time enabled layer'.format(layer)
        LOGGER.error(msg)
        raise ValueError(msg)

    ds = os.path.join(data_basepath,
                      inter_path,
                      file_name)

    output = get_location_info(ds, lon, lat, cfg['layers'][layer], layer_keys)
    output_file = write2format(output, cfg['layers'][layer], format_, lon, lat)
    click.echo(output_file)
