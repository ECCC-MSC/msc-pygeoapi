# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
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

from datetime import datetime
from random import randrange

import pytest
import requests


@pytest.fixture()
def url(pytestconfig):
    return pytestconfig.getoption('url')


def test_api(url):
    """Test suite for hydrometric real-time data API queries"""

    hydrometric_realtime_collections = [
        'hydrometric-realtime'
    ]

    # display all hydrometric realtime data feature collections
    req = f'{url}/collections'
    response = requests.get(req).json()

    collections = [c['id'] for c in response['collections']]

    # 5 hydrometric data feature collections are displayed
    assert all(elem in collections for elem in hydrometric_realtime_collections)  # noqa

    # describe hydrometric realtime data feature collection
    req = f'{url}/collections/hydrometric-realtime'
    response = requests.get(req).json()

    # feature collections metadata is displayed including appropriate title,
    # description, spatial extent and links
    assert response['title'] == 'Real-time hydrometric data'
    assert 'description' in response

    bbox = response['extent']['spatial']['bbox'][0]
    assert bbox == [-142, 42, -52, 84]
    assert 'links' in response

    # access a single hydrometric realtime feature
    req = f'{url}/collections/hydrometric-realtime/items'
    response = requests.get(req).json()

    # single realtime measurement is returned from the past 30 days
    feature = randrange(response['numberReturned'])
    datetime_ = response['features'][feature]['properties']['DATETIME']

    delta = datetime.now() - datetime.strptime(datetime_, '%Y-%m-%dT%H:%M:%S')
    assert delta.days < 31

    # query hydrometric realtime feature collection based on
    # STATION_NUMBER property
    req = f'{url}/collections/hydrometric-realtime/items'
    params = {
        'STATION_NUMBER': '02KD004'
    }

    response = requests.get(req, params=params).json()

    # list of up to 500 measurements are returned from the past 30 days
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['STATION_NUMBER'] == '02KD004'  # noqa

    # query hydrometric realtime feature collection based on
    # STATION_NUMBER property sorted by IDENTIFIER
    req = f'{url}/collections/hydrometric-realtime/items'
    params = {
        'STATION_NUMBER': '02KD004',
        'sortby': 'IDENTIFIER'
    }

    response = requests.get(req, params=params).json()

    # list of up to 500 measurements from the past 30 days are returned
    # sorted by IDENTIFIER
    datetime_ = response['features'][0]['properties']['DATETIME']

    delta = datetime.now() - datetime.strptime(datetime_, '%Y-%m-%dT%H:%M:%S')
    assert delta.days < 31

    # query hydrometric realtime feature collection sorted by
    # earliest date and time
    req = f'{url}/collections/hydrometric-realtime/items'
    params = {
        'sortby': 'DATETIME'
    }

    response = requests.get(req, params=params).json()

    # list of 500 measurements are returned and minimum datetime is near to
    # but after the beginning of the 30th previous day
    datetime_ = response['features'][0]['properties']['DATETIME']

    delta = datetime.now() - datetime.strptime(datetime_, '%Y-%m-%dT%H:%M:%S')
    assert delta.days < 31

    # query hydrometric realtime feature collection sorted by
    # latest date and time
    req = f'{url}/collections/hydrometric-realtime/items'
    params = {
        'sortby': '-DATETIME'
    }

    response = requests.get(req, params=params).json()

    # list of 500 measurements are returned and maximum datetime is before
    # but within an hour of the current time
    datetime_ = response['features'][0]['properties']['DATETIME']

    delta = datetime.now() - datetime.strptime(datetime_, '%Y-%m-%dT%H:%M:%S')
    assert delta.days < 1
    assert delta.seconds < 3600

    # query hydrometric realtime feature collection based on spatial subsetting
    req = f'{url}/collections/hydrometric-realtime/items'
    params = {
        'bbox': '-80,50,-50,60'
    }

    response = requests.get(req, params=params).json()

    # list of 500 measurements are returned from the southeast Canada
    # over the past 30 days
    feature = randrange(response['numberReturned'])
    coords = response['features'][feature]['geometry']['coordinates']
    bbox = [float(b) for b in params['bbox'].split(',')]
    assert coords[0] > bbox[0]
    assert coords[0] < bbox[2]
    assert coords[1] > bbox[1]
    assert coords[1] < bbox[3]
