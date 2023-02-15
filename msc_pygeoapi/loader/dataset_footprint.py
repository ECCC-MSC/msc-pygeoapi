# =================================================================
#
# Author: Nicolas Dion-Degodez
#         <nicolas.dion-degodez@ec.gc.ca>
#
# Copyright (c) 2023 Nicolas Dion-Degodez
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

import json
import yaml
import logging
import os
from pathlib import Path
from osgeo import ogr, osr

import click

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    configure_es_connection
)

LOGGER = logging.getLogger(__name__)

# index settings
INDEX_NAME = 'dataset_footprint'

MAPPINGS = {
    'properties': {
        'geometry': {'type': 'geo_shape'},
        'properties': {
            'properties': {
                'title_en': {
                    'type': 'text',
                },
                'title_fr': {
                    'type': 'text',
                },
                'abstract_en': {
                    'type': 'text',
                },
                'abstract_fr': {
                    'type': 'text',
                },
            }
        }
    }
}

SETTINGS = {
    'order': 0,
    'version': 1,
    'index_patterns': ['{}*'.format(INDEX_NAME)],
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': None
}

class DatasetFootprintLoader(BaseLoader):
    """Dataset Footprint loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.filepath = None
        self.parent_filepath = None
        self.bbox = None
        self.proj4 = None
        self.conn = ElasticsearchConnector(conn_config)

        SETTINGS['mappings'] = MAPPINGS
        self.conn.create_template(INDEX_NAME, SETTINGS)


    def open_mcf(self, filepath):
        """
        Opens an MCF file

        :param filepath: filepath to data on disk

        :returns: opened MCF file        
        """

        with open(filepath, "r") as stream:
            return yaml.safe_load(stream)


    def contains_proj4(self, file):
        """
        Check if the file contains a proj4. 
        Also gets the values of the proj4 and bbox

        :param file: The file to check

        :returns: `boolean` indicating whether the file contains a proj4 or not
        """

        # Exclude MCFs that do not have a spatial key
        try: 
            # If the MCF has a proj4, get the bbox and proj4 values
            for value in file["identification"]["extents"]["spatial"]:
                if "proj4" in value:
                    self.bbox = value["bbox"]
                    self.proj4 = value["proj4"]
                    return True
        except KeyError:
            return False

        return False


    def get_reprojected_polygon(self):
        """
        Create the footprint polygon

        :returns: footprint polygon in json format
        """

        west, south, east, north = self.bbox

        # Check if units are in meters or degrees. Assign a max_length value accordingly
        if "+units=m" in self.proj4:
            max_length = 500
        else:
            max_length = 0.05

        # Create and segmentize the polygon
        geojson = {"type": "POLYGON", "coordinates": [[[west, south], [west, north], [east, north], [east, south], [west, south]]]}
        geom = ogr.CreateGeometryFromJson(json.dumps(geojson))
        geom.Segmentize(max_length)

        # Reproject the polygon
        source = osr.SpatialReference()
        source.ImportFromProj4(self.proj4)
        target = osr.SpatialReference()
        target.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        target.ImportFromEPSG(4326)
        transform = osr.CoordinateTransformation(source, target)
        transformer = ogr.GeomTransformer(transform)
        geom_dst = transformer.Transform(geom)

        return geom_dst.ExportToJson()


    def mcf_to_dict(self, file):
        """
        Convert the MCF into a GeoJSON object

        :param file: The file to convert

        :returns: `dict` of GeoJSON
        """

        # Initiate a dictionary to stock the data to transfer in ES
        dict_ = {
            'type': 'Feature',
            'properties': {},
            'geometry': None
        }

        # Copy the MCF's data in the dictionary
        dict_['id'] = dict_['properties']['id'] = file["metadata"]["identifier"]
        dict_['properties']['title_en'] = file["identification"]["title"]["en"]
        dict_['properties']['title_fr'] = file["identification"]["title"]["fr"]
        # If the MCF doesn't have an abstract key, then look at his parent
        try: 
            dict_['properties']['abstract_en'] = file["identification"]["abstract"]["en"]
        except KeyError:
            self.parent_filepath = os.path.join(os.path.dirname(self.filepath), file['base_mcf'])
            opened_parent_file = self.open_mcf(self.parent_filepath)
            dict_['properties']['abstract_en'] = opened_parent_file["identification"]["abstract"]["en"]

        try: 
            dict_['properties']['abstract_fr'] = file["identification"]["abstract"]["fr"]
        except KeyError:
            self.parent_filepath = os.path.join(os.path.dirname(self.filepath), file['base_mcf'])
            opened_parent_file = self.open_mcf(self.parent_filepath)
            dict_['properties']['abstract_fr'] = opened_parent_file["identification"]["abstract"]["fr"]

        return dict_
        

    def load_data(self, filepath):
        """
        loads data from event to target

        :param filepath: filepath to data on disk

        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)

        LOGGER.debug('Received file {}'.format(self.filepath))

        # Open the MCF
        opened_file = self.open_mcf(self.filepath)

        # If the MCF contains a proj4, proceed to the rest of the code
        if (self.contains_proj4(opened_file)):
            # Transform the yaml file to a dict
            data = self.mcf_to_dict(opened_file)

            # Get the reprojected polygon and stock it in the dict
            polygon = self.get_reprojected_polygon()
            data['geometry'] = json.loads(polygon)
            
            # Send the data to ElasticSearch
            try:
                r = self.conn.Elasticsearch.index(
                    index=INDEX_NAME, id=data['id'], body=data
                )
                LOGGER.debug('Result: {}'.format(r))
                return True
            except Exception as err:
                LOGGER.warning('Error indexing: {}'.format(err))
                return False
        else:
            LOGGER.warning('The file does not contain a proj4. Proceeding to the next file.')
            return False


@click.group()
def dataset_footprint():
    """Manages dataset footprint index"""
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
    """add data to system"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.yml')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = DatasetFootprintLoader(conn_config)
        result = loader.load_data(file_to_process)
        if not result:
            click.echo('features not generated')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(
    prompt='Are you sure you want to delete this index?'
)
def delete_index(ctx, es, username, password, ignore_certs):
    """Delete dataset footprint indexes"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    conn.delete(INDEX_NAME)


dataset_footprint.add_command(add)
dataset_footprint.add_command(delete_index)