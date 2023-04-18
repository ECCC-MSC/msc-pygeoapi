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
import csv
import io
import json
import logging
import os
import re

from osgeo import gdal, osr
from pyproj import Proj, transform
import yaml
from yaml import CLoader

LOGGER = logging.getLogger(__name__)

UNITS = {
    'CDD': {'ABS': 'Cooling degree days / ' +
            'Degrés-jours de climatisation'},
    'GSC': {'ABS': 'Days / Jours'},
    'GSO': {'ABS': 'Days / Jours'},
    'GSW': {'ABS': 'Days / Jours'},
    'HDD': {'ABS': 'Heating degree days / Degrés-jours de chauffe'},
    'PR': {'ANO': '%', 'ABS': 'mm'},
    'PREP1': {'ABS': 'Days / Jours'},
    'SFCWIND': {'ANO': '%', 'ABS': 'm s-1'},
    'SIC': {'ANO': '%', 'ABS': '%'},
    'SIT': {'ANO': '%', 'ABS': 'm'},
    'SND': {'ANO': '%', 'ABS': 'm'},
    'SPEI': {'ABS': 'Standardized Precipitation Evapotranspiration Index / ' +
             'Indice de précipitations et d’évapotranspiration normalisé'},
    'TM': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TN': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TN20': {'ABS': 'Nights / Nuits'},
    'TT': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TX': {'ANO': 'Celsius', 'ABS': 'Celsius'},
    'TX30': {'ABS': 'Days / Jours'}
    }

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'raster-drill',
    'title': 'Raster Drill process',
    'description': 'Raster Drill process',
    'keywords': ['raster drill'],
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
    'inputs': {
        'layer': {
            'title': 'layer name',
            'schema': {
                'type': 'number',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'y': {
            'title': 'y coordinate',
            'schema': {
                'type': 'number',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'x': {
            'title': 'x coordinate',
            'schema': {
                'type': 'number',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'format': {
            'title': 'format: GeoJSON or CSV',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'raster-drill-response': {
            'title': 'output raster drill',
            'schema': {
                'oneOf': [{
                    'contentMediaType': 'application/json'
                }, {
                    'contentMediaType': 'application/json'
                }]
            }
        }
    },
    'example': {
        "inputs": {
            "layer": "CMIP5.TT.RCP26.YEAR.ANO_PCTL50",
            "y": 51.132831196692806,
            "x": -114.74968888274337,
            "format": "CSV"
        }
    }
}


def get_time_info(cfg):
    """
    function to build an array of date based on the yaml info

    :param cfg: CCCS Yaml section for the layer

    :returns: `dict` with the time steps
    """

    LOGGER.debug('creating a date array')
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
            year = int(i / 12)
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

    x = int((x - origin_x) / width)
    y = int((y - origin_y) / height)

    return (x, y)


def get_location_info(file_, x, y, cfg, layer_keys):
    """
    extract x/y value across all bands of a raster file

    :param file_: filepath of raster data
    :param x: x coordinate
    :param y: y coordinate
    :param cfg: yaml information
    :param layer_keys: layer label splitted

    :returns: `dict` of metadata and array values
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
        LOGGER.debug('Fetching units')
        dict_['time_step'] = cfg['timestep']

        dict_['metadata'] = layer_keys
        dict_['uom'] = UNITS[layer_keys['Variable']][layer_keys['Type']]

        ds = gdal.Open(file_)
        LOGGER.debug('Transforming map coordinates into image coordinates')
        x_, y_ = geo2xy(ds, x, y)

    except RuntimeError as err:
        ds = None
        msg = 'Cannot open file: {}'.format(err)
        LOGGER.exception(msg)

    LOGGER.debug('Running through bands')

    for band in range(1, ds.RasterCount + 1):
        LOGGER.debug('Fetching band {}'.format(band))

        srcband = ds.GetRasterBand(band)
        array = srcband.ReadAsArray().tolist()

        try:
            dict_['values'].append(array[y_][x_])
        except IndexError as err:
            msg = 'Invalid x/y value: {}'.format(err)
            LOGGER.exception(msg)

    dict_['dates'] = get_time_info(cfg)

    LOGGER.debug('Freeing dataset object')
    ds = None

    return dict_


def serialize(values_dict, cfg, output_format, x, y):
    """
    Writes the information in the format provided by the user

    :param values_dict: result of the get_location_info function
    :param cfg: yaml information
    :param output_format: output format (GeoJSON or CSV)
    :param x: x coordinate
    :param y: y coordinate

    :returns: GeoJSON or CSV output
    """

    time_begin = values_dict['dates'][0]
    time_end = values_dict['dates'][-1]
    time_step = values_dict['time_step']

    data = None

    LOGGER.debug('Creating the output file')
    if len(values_dict['dates']) == len(values_dict['values']):

        if 'CANGRD' not in cfg['label_en']:

            split_en = cfg['label_en'].split('/')
            split_fr = cfg['label_fr'].split('/')

            if 'SPEI' in cfg['label_en']:
                var_en, sce_en, seas_en, label_en = split_en
                var_fr, sce_fr, seas_fr, label_fr = split_fr
                type_en = type_fr = ''
            elif 'Index' in cfg['label_en']:
                var_en, sce_en, label_en = split_en
                var_fr, sce_fr, label_fr = split_fr
                type_en = type_fr = seas_en = seas_fr = ''
            else:
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

        if output_format == 'CSV':
            time = 'time_{}/{}/{}'.format(time_begin,
                                          time_end,
                                          time_step)
            row = [time,
                   'values',
                   'longitude',
                   'latitude',
                   'scenario_en',
                   'scenario_fr',
                   'time_res_en',
                   'time_res_fr',
                   'value_type_en',
                   'value_type_fr',
                   'percentile_en',
                   'percentile_fr',
                   'variable_en',
                   'variable_fr',
                   'uom']

            try:
                data = io.BytesIO()
                writer = csv.writer(data)
                writer.writerow(row)
            except TypeError:
                data = io.StringIO()
                writer = csv.writer(data)
                writer.writerow(row)

            for i in range(0, len(values_dict['dates'])):
                writer.writerow([values_dict['dates'][i],
                                 values_dict['values'][i],
                                 x,
                                 y,
                                 sce_en,
                                 sce_fr,
                                 seas_en,
                                 seas_fr,
                                 type_en,
                                 type_fr,
                                 pctl_en,
                                 pctl_fr,
                                 var_en,
                                 var_fr,
                                 values_dict['uom']])

        elif output_format == 'GeoJSON':

            values = []
            for k in values_dict['values']:
                values.append(k)

            data = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [x, y]
                },
                'properties': {
                    'time_begin': time_begin,
                    'time_end': time_end,
                    'time_step': time_step,
                    'variable_en': var_en,
                    'variable_fr': var_fr,
                    'uom': values_dict['uom'],
                    'value_type_en': type_en,
                    'value_type_fr': type_fr,
                    'scenario_en': sce_en,
                    'scenario_fr': sce_fr,
                    'period_en': seas_en,
                    'period_fr': seas_fr,
                    'percentile_en': pctl_en,
                    'percertile_fr': pctl_fr,
                    'label_en': label_en,
                    'label_fr': label_fr,
                    'values': values
                }
            }

    return data


def raster_drill(layer, x, y, format_):
    """
    Writes the information in the format provided by the user
    and reads some information from the geomet-climate yaml

    :param layer: layer name
    :param x: x coordinate
    :param y: y coordinate
    :param format_: output format (GeoJSON or CSV)

    :return: return the final file fo a given location
    """

    from msc_pygeoapi.process.cccs import (GEOMET_CLIMATE_CONFIG,
                                           GEOMET_CLIMATE_BASEPATH,
                                           GEOMET_CLIMATE_BASEPATH_VRT)

    LOGGER.info('start raster drilling')

    if format_ not in ['CSV', 'GeoJSON']:
        msg = 'Invalid format'
        LOGGER.error(msg)
        raise ValueError(msg)

    with open(GEOMET_CLIMATE_CONFIG, encoding='utf-8') as fh:
        cfg = yaml.load(fh, Loader=CLoader)

    data_basepath = GEOMET_CLIMATE_BASEPATH
    climate_model_path = cfg['layers'][layer]['climate_model']['basepath']
    file_path = cfg['layers'][layer]['filepath']
    inter_path = os.path.join(climate_model_path, file_path)

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

        file_name = cfg['layers'][layer]['filename']

    elif 'TREND' not in layer and layer.startswith('CANGRD'):
        keys = ['Model', 'Type', 'Variable', 'Period']
        values = layer.replace('_', '.').split('.')
        layer_keys = dict(zip(keys, values))

        data_basepath = GEOMET_CLIMATE_BASEPATH_VRT
        climate_model_path = cfg['layers'][layer]['climate_model']['basepath']
        file_path = cfg['layers'][layer]['filepath']
        inter_path = os.path.join(climate_model_path, file_path)

        file_name = '{}.vrt'.format(cfg['layers'][layer]['filename'])

    elif layer.startswith('SPEI'):
        keys = ['Variable', 'Variation', 'Scenario', 'Period', 'Percentile']
        values = layer.replace('-', '.').replace('_', '.').split('.')
        layer_keys = dict(zip(keys, values))
        layer_keys['Type'] = 'ABS'

        file_name = cfg['layers'][layer]['filename']

    elif layer.startswith('INDICES'):
        keys = ['Model', 'Variable', 'Scenario', 'Percentile']
        values = layer.replace('_', '.').split('.')
        layer_keys = dict(zip(keys, values))
        layer_keys['Type'] = 'ABS'

        file_name = cfg['layers'][layer]['filename']

    else:
        msg = 'Not a valid or time enabled layer: {}'.format(layer)
        LOGGER.error(msg)
        raise ValueError(msg)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(cfg['layers'][layer]['climate_model']['projection'])
    inProj = Proj(init='epsg:4326')
    outProj = Proj(srs.ExportToProj4())
    _x, _y = transform(inProj, outProj, x, y)

    ds = os.path.join(data_basepath, inter_path, file_name)

    data = get_location_info(ds, _x, _y, cfg['layers'][layer], layer_keys)
    output = serialize(data, cfg['layers'][layer], format_, x, y)

    return output


@click.group('execute')
def raster_drill_execute():
    pass


@click.command('raster-drill')
@click.pass_context
@click.option('--layer', help='Layer name to process', required=True)
@click.option('--x', help='x coordinate', required=True)
@click.option('--y', help='y coordinate', required=True)
@click.option('--format', 'format_', type=click.Choice(['GeoJSON', 'CSV']),
              default='GeoJSON', help='output format')
def raster_drill_cli(ctx, layer, x, y, format_='GeoJSON'):

    output = raster_drill(layer, float(x), float(y), format_)
    if format_ == 'GeoJSON':
        click.echo(json.dumps(output, ensure_ascii=False))
    elif format_ == 'CSV':
        click.echo(output.getvalue())


raster_drill_execute.add_command(raster_drill_cli)

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class RasterDrillProcessor(BaseProcessor):
        """Raster Drill Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.cccs.raster_drill.RasterDrillProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            mimetype = 'application/json'

            layer = data.get('layer')
            x = float(data.get('x'))
            y = float(data.get('y'))
            format_ = data.get('format')

            try:
                output = raster_drill(layer, x, y, format_)
            except ValueError as err:
                msg = 'Process execution error: {}'.format(err)
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            if format_ == 'GeoJSON':
                dict_ = output
            elif format_ == 'CSV':
                mimetype = 'text/csv'
                dict_ = output.getvalue()
            else:
                msg = 'Invalid format'
                LOGGER.error(msg)
                raise ValueError(msg)

            return mimetype, dict_

        def __repr__(self):
            return '<RasterDrillProcessor> {}'.format(self.name)
except (ImportError, RuntimeError):
    pass
