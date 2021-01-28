# =================================================================
#
# Author: Thinesh Sornalingam <thinesh.sornalingam@canada.ca>,
#         Robert Westhaver <robert.westhaver.eccc@gccollaboration.ca>
#         Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2020 Thinesh Sornalingam
# Copyright (c) 2020 Robert Westhaver
# Copyright (c) 2020 Tom Kralidis
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
from datetime import datetime
import logging
import os

from elasticsearch import logger as elastic_logger
from lxml import etree

from msc_pygeoapi import cli_options
from msc_pygeoapi.env import (
    MSC_PYGEOAPI_CACHEDIR,
    MSC_PYGEOAPI_LOGGING_LOGLEVEL,
)
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection,
    check_es_indexes_to_delete,
    json_pretty_print,
    DATETIME_RFC3339_MILLIS_FMT,
)


LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(getattr(logging, MSC_PYGEOAPI_LOGGING_LOGLEVEL))

STATIONS_LIST_NAME = 'swob-xml_station_list.csv'
STATIONS_LIST_URL = 'https://dd.weather.gc.ca/observations/doc/{}'.format(
    STATIONS_LIST_NAME
)

STATIONS_CACHE = os.path.join(MSC_PYGEOAPI_CACHEDIR, STATIONS_LIST_NAME)

# cleanup settings
DAYS_TO_KEEP = 30

# index settings
INDEX_BASENAME = 'swob_realtime.'

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': ['{}*'.format(INDEX_BASENAME)],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': {
                'properties': {
                    'rmk': {
                        'type': 'text',
                        'fields': {'raw': {'type': 'keyword'}},
                    }
                }
            },
        }
    },
}


def parse_swob(swob_file):
    """
    Read swob at swob_path and return object
    :param swob_path: file path to SWOB XML
    :returns: dictionary of SWOB
    """

    namespaces = {
        'gml': 'http://www.opengis.net/gml',
        'om': 'http://www.opengis.net/om/1.0',
        'xlink': 'http://www.w3.org/1999/xlink',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'dset': 'http://dms.ec.gc.ca/schema/point-observation/2.0',
    }

    swob_values = {}
    elevation = ''
    latitude = ''
    longitude = ''

    # extract the swob xml source name
    swob_name = os.path.basename(swob_file)

    # make sure the xml is parse-able
    with open(swob_file) as fh:
        try:
            xml_tree = etree.parse(fh)
        except (FileNotFoundError, etree.ParseError):
            msg = 'Error: file {} cannot be parsed as xml'.format(swob_file)
            LOGGER.debug(msg)
            raise RuntimeError(msg)

        gen_path = './/om:Observation/om:metadata/dset:set/dset:general'
        general_info_tree = xml_tree.findall(gen_path, namespaces)
        general_info_elements = list(general_info_tree[0].iter())
        properties = {}

        # extract swob dataset
        for element in general_info_elements:
            if 'name' in element.attrib:
                if element.tag.split('}')[1] == 'dataset':
                    properties[element.tag.split('}')[1]] = element.attrib[
                        'name'
                    ].replace('/', '-')

        # add swob source name to properties
        properties["id"] = swob_name

        # extract ID related properties
        id_path = (
            './/om:Observation/om:metadata/'
            + 'dset:set/dset:identification-elements'
        )
        identification_tree = xml_tree.findall(id_path, namespaces)
        identification_elements = list(identification_tree[0].iter())

        for element in identification_elements:
            element_name = ''
            if 'name' in element.attrib:
                for key in element.attrib:
                    if key == 'name':
                        if element.attrib[key] == 'stn_elev':
                            elevation = float(element.attrib['value'])
                            break
                        elif element.attrib[key] == 'lat':
                            latitude = float(element.attrib['value'])
                            break
                        elif element.attrib[key] == 'long':
                            longitude = float(element.attrib['value'])
                            break
                        else:
                            element_name = element.attrib[key]
                    else:
                        properties[
                            "{}-{}".format(element_name, key)
                        ] = element.attrib[key]

        # set up cords and time stamps
        swob_values['coordinates'] = [longitude, latitude, elevation]

        s_time = (
            './/om:Observation/om:samplingTime/'
            + 'gml:TimeInstant/gml:timePosition'
        )
        time_sample = list(xml_tree.findall(s_time, namespaces)[0].iter())[0]
        properties['obs_date_tm'] = time_sample.text

        r_time = (
            './/om:Observation/om:resultTime/'
            + 'gml:TimeInstant/gml:timePosition'
        )
        time_result = list(xml_tree.findall(r_time, namespaces)[0].iter())[0]
        properties['processed_date_tm'] = time_result.text

        # extract the result data from the swob
        res_path = './/om:Observation/om:result/dset:elements'
        result_tree = xml_tree.findall(res_path, namespaces)
        result_elements = list(result_tree[0].iter())

        last_element = ''
        for element in result_elements:
            nested = element.iter()
            for nest_elem in nested:
                value = ''
                uom = ''
                if 'name' in nest_elem.attrib:
                    name = nest_elem.attrib['name']
                    if 'value' in nest_elem.attrib:
                        value = nest_elem.attrib['value']

                        # Checks to see if value string can be cast
                        # to float/int
                        try:
                            if '.' in value:
                                value = float(value)
                            else:
                                value = int(value)
                        except ValueError:
                            msg = (
                                f'Warning the value: "{value}" could not be '
                                f'converted to a number, this can be because '
                                f'of an improperly formatted number value or '
                                f'because of an intentional string value'
                            )

                            LOGGER.debug(msg)
                            pass

                    if 'uom' in nest_elem.attrib:
                        if nest_elem.attrib['uom'] != 'unitless':
                            uom = nest_elem.attrib['uom'].replace('\u00c2', '')

                    # element can be 1 of 3 things:
                    #   1. a data piece
                    #   2. a qa summary
                    #   3. a data flag
                    if all([name != 'qa_summary', name != 'data_flag']):
                        properties[name] = value
                        if uom:
                            properties["{}-{}".format(name, 'uom')] = uom
                        last_element = name
                    elif name == 'qa_summary':
                        properties["{}-{}".format(last_element, 'qa')] = value
                    elif name == 'data_flag':
                        properties[
                            "{}-{}-{}".format(last_element, 'data_flag', 'uom')
                        ] = uom
                        properties[
                            "{}-{}-{}".format(
                                last_element, 'data_flag', 'code_src'
                            )
                        ] = nest_elem.attrib['code-src']
                        properties[
                            "{}-{}-{}".format(
                                last_element, 'data_flag', 'value'
                            )
                        ] = value

            swob_values['properties'] = properties

            for k, v in swob_values['properties'].items():
                if v == 'MSNG':
                    swob_values['properties'][k] = None

            return swob_values


def swob2geojson(swob_file):
    """
    Produce GeoJSON from dict
    :param swob_dict: swob in memory
    :returns: geojson
    """

    swob_dict = parse_swob(swob_file)
    json_output = {}

    try:
        if len(swob_dict) == 0:
            msg = 'Error: dictionary passed into swob2geojson is blank'
            LOGGER.debug(msg)
            raise RuntimeError(msg)
    except TypeError:
        msg = "Error: NoneType passed in as swob dict"
        LOGGER.debug(msg)
        raise RuntimeError(msg)

    # verify dictionary contains the data we need to avoid error
    if 'properties' in swob_dict and 'coordinates' in swob_dict:
        json_output['id'] = swob_dict['properties']['id']
        json_output['type'] = 'Feature'
        json_output["geometry"] = {
            "type": "Point",
            "coordinates": swob_dict['coordinates'],
        }
        json_output["properties"] = swob_dict["properties"]
        return json_output
    else:
        msg = (
            'Error: dictionary passed into swob2geojson lacks',
            ' required fields',
        )
        LOGGER.debug(msg)
        raise RuntimeError(msg)


class SWOBRealtimeLoader(BaseLoader):
    """SWOB Real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config, verify_certs=False)
        self.items = []
        self.conn.create_template(INDEX_BASENAME, SETTINGS)

    def generate_observations(self, filepath):
        """
        Generates and yields a series of observations, one for each row in
        <filepath>. Observations are returned as Elasticsearch bulk API
        upsert actions, with documents in GeoJSON to match the Elasticsearch
        index mappings.

        :param filename: Path to a data file of realtime SWOB
        :returns: Generator of Elasticsearch actions to upsert the observations
        """

        observation = swob2geojson(filepath)
        observation_id = observation['id']

        LOGGER.debug(
            'Observation {} created successfully'.format(observation_id)
        )

        obs_dt = datetime.strptime(
            observation['properties']['date_tm-value'],
            DATETIME_RFC3339_MILLIS_FMT,
        )
        obs_dt2 = obs_dt.strftime('%Y-%m-%d')
        es_index = '{}{}'.format(INDEX_BASENAME, obs_dt2)

        action = {
            '_id': observation_id,
            '_index': es_index,
            '_op_type': 'update',
            'doc': observation,
            'doc_as_upsert': True,
        }

        self.items.append(observation)

        yield action

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        LOGGER.debug('Received file {}'.format(filepath))
        chunk_size = 80000

        package = self.generate_observations(filepath)
        self.conn.submit_elastic_package(package, request_size=chunk_size)

        return True


@click.group()
def swob_realtime():
    """Manages SWOB realtime indices"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_DIRECTORY()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, directory, es, username, password, ignore_certs):
    """adds data to system"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.xml')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = SWOBRealtimeLoader(conn_config)
        result = loader.load_data(file_to_process)
        if result:
            click.echo(
                'GeoJSON features generated: {}'.format(
                    json_pretty_print(loader.items)
                )
            )


@click.command()
@click.pass_context
@cli_options.OPTION_DAYS(
    default=DAYS_TO_KEEP, help='Delete indexes older than n days (default={})'
)
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete old indexes?')
def clean_indexes(ctx, days, es, username, password, ignore_certs):
    """Clean SWOB realtime indexes older than n number of days"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    indexes = conn.get('{}*'.format(INDEX_BASENAME))
    click.echo(indexes)

    if indexes:
        indexes_to_delete = check_es_indexes_to_delete(indexes, days)
        if indexes_to_delete:
            click.echo('Deleting indexes {}'.format(indexes_to_delete))
            conn.delete(','.join(indexes))

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete these indexes?'
)
def delete_indexes(ctx, es, username, password, ignore_certs):
    """Delete all SWOB realtime indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    all_indexes = '{}*'.format(INDEX_BASENAME)

    click.echo('Deleting indexes {}'.format(all_indexes))
    conn.delete(all_indexes)

    click.echo('Done')


swob_realtime.add_command(add)
swob_realtime.add_command(clean_indexes)
swob_realtime.add_command(delete_indexes)
