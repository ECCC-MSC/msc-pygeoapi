# =================================================================
#
# Author: Julien Roy-Sabourin <julien.roy-sabourin.eccc@gccollaboration.ca>
#
# Copyright (c) 2020 Julien Roy-Sabourin
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
from datetime import datetime, timedelta
import json
import logging
import matplotlib.pyplot as plt
from io import BytesIO


from elasticsearch import Elasticsearch, exceptions
from osgeo import gdal, osr
from pyproj import Proj, transform

LOGGER = logging.getLogger(__name__)
# ne pas oublier logger level est a debug:

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
ES_INDEX = 'geomet-data-registry-tileindex'

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'rdpa-graph',
    'title': 'Rdpa Graph process',
    'description': 'produce rdpa graph',
    'keywords': ['rdpa'],
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
        'id': 'layer',
        'title': 'layer name',
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
        'id': 'date_end',
        'title': 'end date (yyyy-mm-dd)',
        'description': 'final date of the graph',
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
        'id': 'date_begin',
        'title': 'begin date (yyyy-mm-dd)',
        'description': 'first date of the graph',
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
        'id': 'x',
        'title': 'x coordinate',
        'description': 'longitude',
        'input': {
            'literalDataDomain': {
                'dataType': 'float',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'y',
        'title': 'y coordinate',
        'description': 'latitude',
        'input': {
            'literalDataDomain': {
                'dataType': 'float',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'time_step',
        'title': 'time step',
        'description': 'time step for the graph in hours',
        'input': {
            'literalDataDomain': {
                'dataType': 'float',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'format',
        'title': 'output format',
        'description': 'GeoJSON or PNG',
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
    }],
    'outputs': [{
        'id': 'rdpa-graph-response',
        'title': 'output rdpa graph',
        'output': {
            'formats': [{
                'mimeType': 'image/png'
            }, {
                'mimeType': 'application/json'
            }]
        }
    }],
    'example': {
        'inputs': [{
                "id": "layer",
                "value": "RDPA.24P_PR"
            },
            {
                "id": "date_end",
                "value": "2020-05-15"
            },
            {
                "id": "date_begin",
                "value": "2020-05-01"
            },
            {
                "id": "x",
                "value": -73.63834417943312
            },
            {
                "id": "y",
                "value": 45.51508872002296
            },
            {
                "id": "time_step",
                "value": 24
            },
            {
                "id": "format",
                "value": "GeoJSON"
            }]
    }
}


def valid_dates(date):
    """
    validate that date is in the correct format (raise ValueError)
    and add deafaut hour(12Z) if not specified

    date : date to validate

    retunr validated date
    """

    if len(date) == 10:
        datetime.strptime(date, '%Y-%m-%d')
        date = date + 'T12:00:00Z'

    else:
        datetime.strptime(date, DATE_FORMAT)

    return date


def query_es(es_object, index_name, date_end, date_begin, layer):

    """
    find documents that fit with search param

    es_object : ES server
    index_name : index name in ES server to look into
    date_end : max forecast hour datetime value to match docs
    date_begin : min forecast hour datetime value to match docs
    layer : layers to match docs

    return : json of the search result

    """

    s_object = {
        'size': 124,              # result limit 1 month
        'query':
        {
            'bool':
            {
                'must':
                {
                    'range':
                    {
                        'properties.forecast_hour_datetime':
                        {
                            'lte': date_end,
                            'gte': date_begin
                        }
                    }
                },
                'filter':
                {
                    'term': {"properties.layer.raw": layer}
                }
            }
        }
    }

    try:
        res = es_object.search(index=index_name, body=s_object)

    except exceptions.ElasticsearchException as error:
        msg = 'ES search error: {}' .format(error)
        LOGGER.error(msg)
        return None, None

    res_sorted = sorted(res['hits']['hits'],
                        key=lambda i: i['_source']['properties']
                        ['forecast_hour_datetime'])

    return res_sorted, res['hits']['total']['value']


def xy_2_raster_data(path, x, y):

    """
    convert coordinate to x, y raster coordinate and
    return the x,y value of the raster

    path : where the grib raster file is located
    x : x coordinate
    y : y coordinate

    return : raster value in x, y position
    """

    try:
        grib = gdal.Open(path)

        transform = grib.GetGeoTransform()

        org_x = transform[0]
        org_y = transform[3]

        pix_w = transform[1]
        pix_h = transform[5]

        x = int((x - org_x) / pix_w)
        y = int((y - org_y) / pix_h)

        try:
            band1 = grib.GetRasterBand(1).ReadAsArray()
            return band1[y][x]

        except IndexError as error:
            msg = 'Invalid coordinates : {}' .format(error)
            LOGGER.error(msg)

    except RuntimeError as error:
        msg = 'can\'t open file : {}' .format(error)
        LOGGER.error(msg)

    return 0


def _24_or_6(file):
    """
    find if a rdpa file is for a 24h or 6h accumulation

    file : filepaht with file name

    return : true if 24h, false if 6h
    """

    path = file.split('/')

    if path[-2] == '24':
        return 24
    elif path[-2] == '06':
        return 6
    else:
        return 0


def get_values(res, x, y, cumul):
    """
    get the raw raster values at (x, y) for each document
    found by ES

    res : ES search result
    x : x coordinate
    y : y coordinate
    cummul : 24h or 6h accumulation files

    return : (x, y) raster values and dates

    """

    data = {
        'values': [],
        'dates': []
    }

    if cumul == 6:
        for doc in res:
            file_path = doc['_source']['properties']['filepath']
            date = doc['_source']['properties']['forecast_hour_datetime']
            val = xy_2_raster_data(file_path, x, y)
            data['values'].append(val)
            data['dates'].append(date)

    elif cumul == 24:      # use half of the documents

        date_ = res[-1]['_source']['properties']['forecast_hour_datetime']
        date_, time_ = date_.split('T')

        for doc in res:

            file_path = doc['_source']['properties']['filepath']
            date = doc['_source']['properties']['forecast_hour_datetime']
            tmp, time = date.split('T')

            if time == time_:
                val = xy_2_raster_data(file_path, x, y)
                data['values'].append(val)
                data['dates'].append(date)

    return data


def get_graph_arrays(values, time_step):
    """
    Produce the arrays for the graph accordingly to the time step

    values : raw (x, y) raster values and dates
    time_step : time step for the graph in hours

    return : data : json that contains 3 arrays :
                        values : rdpa value for 24h
                        total_value : total rdpa value since the begin date
                        date : date of rdpa values
    """

    data = {
        'values': [],
        'total_values': [],
        'dates': []
    }

    date_c = values['dates'][0]
    date_c = datetime.strptime(date_c, DATE_FORMAT)
    total = 0
    cmpt = -1

    for i in range(len(values['dates'])):
        val = values['values'][i]
        date = values['dates'][i]
        date = datetime.strptime(date, DATE_FORMAT)
        total += val

        if date != date_c:
            data['values'][cmpt] += val
            data['total_values'][cmpt] += val
        else:
            data['values'].append(val)
            data['total_values'].append(total)
            data['dates'].append(date)
            date_c = date + timedelta(hours=time_step)
            cmpt += 1

    if time_step >= 24 and (time_step % 24) == 0:
        for i in range(len(data['dates'])):
            date, time = datetime.strftime(data['dates'][i],
                                           DATE_FORMAT).split('T')
            data['dates'][i] = date
    else:
        for i in range(len(data['dates'])):
            data['dates'][i] = datetime.strftime(data['dates'][i],
                                                 '%Y-%m-%d %H:%M')
    return data


def transform_coord(file, x, y):
    """
    transform a lat long coordinate into the projection of the given file

    file : file to extract th projection from
    x : x coordinate (long)
    y : y coordinate (lat)

    return : _x _y : coordinata in transformed projection
    """

    ds = gdal.Open(file)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(ds.GetProjection())
    inProj = Proj(init='epsg:4326')
    outProj = Proj(srs.ExportToProj4())
    _x, _y = transform(inProj, outProj, x, y)
    return _x, _y


def geo_json(data, x, y):
    """
    return the process output in GeoJSON format

    data: JSON of graph data
    X : x corrdinate
    y : y coordinate

    return : output : GeoJSON of graph data
    """

    output = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [x, y]
        },
        'properties': {
        }
    }

    output['properties'] = data
    return output


def png(data, coord_x, coord_y, time_step):
    """
    produce a graph

    data : graph data
    coord_x : x coordinate
    coord_y : y coordinate
    time_step : time step for graph

    return : output : PNG graph in bytes
    """

    size = len(data['dates'])
    x_size = size/3.3
    if x_size < 8:
        x_size = 8
    elif x_size > 18:
        x_size = 18

    params = {'legend.fontsize': '14',
              'figure.figsize': (x_size, 8.2),
              'axes.labelsize': '14',
              'axes.titlesize': '16',
              'xtick.labelsize': '12',
              'ytick.labelsize': '12'}
    plt.rcParams.update(params)

    if(coord_y >= 0):
        coord = '(' + str(round(coord_y, 2)) + 'N '
    else:
        coord = '(' + str(round(-coord_y, 2)) + 'S '
    if(coord_x >= 0):
        coord = coord + str(round(coord_x, 2)) + 'E)'
    else:
        coord = coord + str(round(-coord_x, 2)) + 'W)'

    x = list(range(1, len(data['dates'])+1))
    y = data['values']
    y2 = data['total_values']

    fig, ax = plt.subplots()
    plt.bar(x, y, align='edge', width=-0.98)
    plt.title('Daily Total Precipitation (bars), Cummulative (line)\n' + coord)
    ax.set_ylabel('mm per day / par jour', color='b')
    plt.grid(True, which='both', alpha=0.5, linestyle='-')

    ax2 = plt.twinx()
    ax2.plot(x, y2, color='k')
    ax2.set_ylabel('mm cummulative / cumulatif')
    ax2.set_ylim(0, (max(y2) * 1.1))

    label = []
    spacing = int(round((size/124)*4))
    if spacing == 0:
        spacing = 1
    cmpt = spacing
    if time_step == 6 and spacing == 4:
        for i in range(len(data['dates'])):
            date, time = data['dates'][i].split(' ')
            data['dates'][i] = date

    for date in data['dates']:
        if cmpt % spacing == 0:
            label.append(date)
        else:
            label.append('')
        cmpt += 1

    ax.xaxis.set_ticks(list(range(1, len(data['dates'])+1)))
    ax.xaxis.set_ticklabels(label, rotation=90, ha='center')
    ax.margins(x=0)
    plt.subplots_adjust(bottom=0.2)

    b = BytesIO()
    plt.savefig(b, bbox_inches='tight', format='png')
    return b


def get_rpda_info(layer, date_end, date_begin, x, y, time_step, format_):
    """
    output information to produce graph about rain
    accumulation for given location and number of days

    layer : layer to search the info in
    date_end : end date
    date_begin : begin date
    x : x coordinate
    y : y coordinate
    time_step : time step for the graph in hours

    return : data
    """

    try:

        date_begin = valid_dates(date_begin)
        date_end = valid_dates(date_end)

    except ValueError as error:
        msg = 'invalid date : {}' .format(error)
        LOGGER.error(msg)
        return None

    # TODO: use env variable for ES connection
    es = Elasticsearch(['localhost:9200'])
    res, nb_res = query_es(es, ES_INDEX, date_end, date_begin, layer)

    if res is not None:
        if nb_res > 0:
            file1 = res[0]['_source']['properties']['filepath']
            cumul = _24_or_6(file1)
            try:
                if (time_step % cumul) == 0:
                    _x, _y = transform_coord(file1, x, y)
                    values = get_values(res, _x, _y, cumul)
                    data = get_graph_arrays(values, time_step)

                    if format_.lower() == 'geojson':
                        output = geo_json(data, x, y)
                    else:
                        output = png(data, x, y, time_step)

                    return output
                else:
                    LOGGER.error('invalid time step')
            except ZeroDivisionError as error:
                msg = 'layer error :  {}' .format(error)
                LOGGER.error(msg)
        else:
            LOGGER.error('no data found')
    else:
        LOGGER.error('failed to extract data')

    return None


@click.group('execute')
def rdpa_graph_execute():
    pass


@click.command('rdpa-graph')
@click.pass_context
@click.option('--layer', help='layer name', type=str)
@click.option('--date_end', help='end date of the graph', type=str)
@click.option('--date_begin', help='end date of the graph', type=str)
@click.option('--x', help='x coordinate', type=float)
@click.option('--y', help='y coordinate', type=float)
@click.option('--time_step', help='graph time step', type=int, default=0)
@click.option('--format', 'format_', type=click.Choice(['GeoJSON', 'PNG']),
              default='GeoJSON', help='output format')
def cli(ctx, layer, date_end, date_begin, x, y, time_step, format_):
    output = get_rpda_info(layer, date_end, date_begin, x, y, time_step,
                           format_)
    if format_.lower() == 'png':
        if output is not None:
            click.echo(output.getvalue())
        else:
            return None
    else:
        click.echo(json.dumps(output, ensure_ascii=False))


rdpa_graph_execute.add_command(cli)


try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class RdpaGraphProcessor(BaseProcessor):
        """rdpa graph Processor"""

        def __init__(self, provider_def):
            """
            Initialize object
            :param provider_def: provider definition
            :returns: pygeoapi.process.weather.rdpa_graph.RdpaGraphProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            layer = data['layer']
            date_end = data['date_end']
            date_begin = data['date_begin']
            x = data['x']
            y = data['y']
            time_step = data['time_step']
            format_ = data['format']

            if format_.lower() != 'geojson' and format_.lower() != 'png':
                msg = 'Invalid format'
                LOGGER.error(msg)
                raise ValueError(msg)

            try:
                output = get_rpda_info(layer, date_end, date_begin, x, y,
                                       time_step, format_)

            except ValueError as error:
                msg = 'Process execution error: {}'.format(error)
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            if format_.lower() == 'png':
                if output is not None:
                    return output.getvalue()
                else:
                    return BytesIO().getvalue()
            else:
                return output

        def __repr__(self):
            return '<RdpaGraphProcessor> {}'.format(self.name)

except (ImportError, RuntimeError):
    pass
