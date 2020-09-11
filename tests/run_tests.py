# =================================================================
#
# Author: Thinesh Sornalingam <thinesh.sornalingam@canada.ca>,
#         Robert Westhaver <robert.westhaver.eccc@gccollaboration.ca>,
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

import json
import unittest

from msc_pygeoapi.loader.swob_realtime import swob2geojson


def write_output(name, list_dict):
    """
    Helper used in generating potential outputs
    param: name - String, name of xml file
    param: list_dict - list, containing geojson dicts for multiple
            geojson per file functionality
    """
    with open(name + '.json', 'wb') as fp:
        for feature in list_dict:
            fp.write(
                json.dumps(feature, indent=4, ensure_ascii=False).encode(
                    'utf8'
                )
            )


def read_json(file_name):
    """
    Helper used in reading master json templates and converting back to dicts
    param: file_name - String, name of json file to load
    """
    with open(file_name, 'rb') as fp:
        return json.load(fp)


class Sob2GeoJsonTest(unittest.TestCase):
    """Test suite for converting swobs to geojson"""

    def test_CGCH_minute(self):
        """Test converting CGCH minute swob into geojson"""

        test_file = './tests/swob/2020-07-01-0007-CGCH-AUTO-minute-swob.xml'
        master_file = './tests/geojson/CGCH_minute_master.json'
        master_geojson = read_json(master_file)
        self.assertEqual(swob2geojson(test_file), master_geojson)

    def test_CAFC_minute(self):
        """Test converting CAFC minute swob into geojson"""

        test_file = './tests/swob/2020-07-01-0007-CAFC-AUTO-minute-swob.xml'
        master_file = './tests/geojson/CAFC_minute_master.json'
        master_geojson = read_json(master_file)
        self.assertEqual(swob2geojson(test_file), master_geojson)

    def test_CPOX_minute(self):
        """Test converting CPOX minute swob into geojson"""

        test_file = './tests/swob/2020-06-08-0000-CPOX-AUTO-minute-swob.xml'
        master_file = './tests/geojson/CPOX_minute_master.json'
        master_geojson = read_json(master_file)
        self.assertEqual(swob2geojson(test_file), master_geojson)

    def test_CAAW_minute(self):
        """Test converting CAAW minute swob into geojson"""

        test_file = './tests/swob/2020-06-08-0000-CAAW-AUTO-minute-swob.xml'
        master_file = './tests/geojson/CAAW_minute_master.json'
        master_geojson = read_json(master_file)
        self.assertEqual(swob2geojson(test_file), master_geojson)

    def test_CYBQ_swob(self):
        """Test converting CYBQ swob into geojson"""

        test_file = './tests/swob/2020-05-31-0200-CYBQ-AUTO-swob.xml'
        master_file = './tests/geojson/CYBQ_swob_master.json'
        master_geojson = read_json(master_file)
        self.assertEqual(swob2geojson(test_file), master_geojson)


# main
if __name__ == '__main__':
    unittest.main()
