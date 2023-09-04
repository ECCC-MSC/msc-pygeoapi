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

import csv
from io import StringIO
from random import randrange

import pytest
import requests


@pytest.fixture()
def url(pytestconfig):
    return pytestconfig.getoption('url')


def test_api(url):
    """Test suite for hydat data API queries"""

    hydat_collections = [
        'hydrometric-stations',
        'hydrometric-daily-mean',
        'hydrometric-monthly-mean',
        'hydrometric-annual-statistics',
        'hydrometric-annual-peaks'
    ]

    # display all hydrometric data feature collections
    req = f'{url}/collections'
    response = requests.get(req).json()

    collections = [c['id'] for c in response['collections']]

    # 5 hydrometric data feature collections are displayed
    assert all(elem in collections for elem in hydat_collections)

    # describe hydrometric stations feature collection
    req = f'{url}/collections/hydrometric-stations'
    response = requests.get(req).json()

    # feature collections metadata is displayed including appropriate title,
    # description, spatial extent and links
    assert response['title'] == 'Hydrometric Monitoring Stations'
    assert 'description' in response

    bbox = response['extent']['spatial']['bbox'][0]
    temporal = response['extent']['temporal']['interval'][0]

    assert bbox == [-142, 42, -52, 84]
    assert temporal[0] == '1850-01-01T00:00:00'
    assert temporal[1] is None

    assert 'links' in response

    # query hydrometric stations feature collection
    req = f'{url}/collections/hydrometric-stations/items'
    response = requests.get(req).json()

    # default list of 500 stations is returned with correct data
    assert response['numberReturned'] == 500

    # query hydrometric stations feature collection and return
    # sorted by IDENTIFIER
    req = f'{url}/collections/hydrometric-stations/items'
    params = {
        'sortby': 'IDENTIFIER'
    }
    response = requests.get(req, params=params).json()

    assert response['features'][0]['id'] == '01AA002'

    # query hydrometric stations feature collection to return in CSV format
    req = f'{url}/collections/hydrometric-stations/items'
    params = {
        'f': 'csv'
    }
    response = requests.get(req, params=params).text

    # default list of 10 stations is returned with correct data
    reader = csv.DictReader(StringIO(response))
    assert len(reader.fieldnames) == 12

    # access a single hydrometric stations feature
    req = f'{url}/collections/hydrometric-stations/items/01BH001'
    response = requests.get(req, params=params).json()

    # single station (01BH001) is returned with correct data
    assert response['id'] == '01BH001'
    assert response['properties']['STATION_NAME'] == 'DARTMOUTH (RIVIERE) PRES DE CORTEREAL'  # noqa
    assert response['properties']['PROV_TERR_STATE_LOC'] == 'QC'

    # query hydrometric stations feature collection based on
    # PROV_TERR_STATE_LOC property
    req = f'{url}/collections/hydrometric-stations/items'
    params = {
        'PROV_TERR_STATE_LOC': 'SK'
    }
    response = requests.get(req, params=params).json()

    # list of stations in Saskatchewan (first 500) is returned with
    # correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['PROV_TERR_STATE_LOC'] == 'SK'  # noqa

    # query hydrometric stations feature collection based on STATUS_EN property
    req = f'{url}/collections/hydrometric-stations/items'
    params = {
        'STATUS_EN': 'Active'
    }
    response = requests.get(req, params=params).json()

    # list of active stations (first 500) is returned with correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['STATUS_EN'] == 'Active'

    # access a single hydrometric daily mean feature
    req = f'{url}/collections/hydrometric-daily-mean/items/10NC001.1979-07-19'
    response = requests.get(req, params=params).json()

    # single daily mean is returned with correct data
    assert response['id'] == '10NC001.1979-07-19'
    assert response['properties']['DISCHARGE'] == 131

    # query hydrometric daily means feature collection based on
    # STATION_NUMBER property
    req = f'{url}/collections/hydrometric-daily-mean/items'
    params = {
        'STATION_NUMBER': '10NC001'
    }
    response = requests.get(req, params=params).json()

    # list of daily means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['STATION_NUMBER'] == '10NC001'  # noqa

    # query hydrometric daily means feature collection based on
    # STATION_NUMBER property sorted by IDENTIFIER
    req = f'{url}/collections/hydrometric-daily-mean/items'
    params = {
        'STATION_NUMBER': '10NC001',
        'sortby': 'IDENTIFIER'
    }
    response = requests.get(req, params=params).json()

    # list of daily means are (first 500) returned with correct data sorted
    # by IDENTIFIER
    assert response['features'][0]['id'] == '10NC001.1969-09-01'

    # query hydrometric daily means feature collection based on
    # STATION_NUMBER property and temporal subsetting
    req = f'{url}/collections/hydrometric-daily-mean/items'
    params = {
        'STATION_NUMBER': '10NC001',
        'datetime': '1979-01-01/1981-01-01'
    }
    response = requests.get(req, params=params).json()

    # list of daily means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    date_ = response['features'][feature]['properties']['DATE']
    assert date_ >= '1979-01-01'
    assert date_ <= '1981-01-01'

    # query hydrometric daily means feature collection based on spatial
    # subsetting
    req = f'{url}/collections/hydrometric-daily-mean/items'
    params = {
        'bbox': '-80,40,-50,50'
    }
    response = requests.get(req, params=params).json()

    # list of daily means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    coords = response['features'][feature]['geometry']['coordinates']
    bbox = [float(b) for b in params['bbox'].split(',')]
    assert coords[0] > bbox[0]
    assert coords[0] < bbox[2]
    assert coords[1] > bbox[1]
    assert coords[1] < bbox[3]

    # access a single hydrometric monthly mean feature
    req = f'{url}/collections/hydrometric-monthly-mean/items/09EA004.1979-09'

    response = requests.get(req).json()

    # single daily mean is returned with correct data
    assert response['id'] == '09EA004.1979-09'
    assert response['properties']['STATION_NUMBER'] == '09EA004'

    # query hydrometric monthly means feature collection based on
    # STATION_NUMBER property
    req = f'{url}/collections/hydrometric-monthly-mean/items'
    params = {
        'STATION_NUMBER': '10NC001'
    }
    response = requests.get(req, params=params).json()

    # list of monthly means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['STATION_NUMBER'] == '10NC001'  # noqa

    # query hydrometric monthly means feature collection based on
    # STATION_NUMBER property sorted by STATION_NAME
    req = f'{url}/collections/hydrometric-monthly-mean/items'
    params = {
        'STATION_NUMBER': '10NC001',
        'sortby': 'STATION_NAME'
    }
    response = requests.get(req, params=params).json()

    # list of monthly means are (first 500) returned with correct data sorted
    # by STATION_NAME
    assert response['features'][0]['properties']['STATION_NAME'] == 'ANDERSON RIVER BELOW CARNWATH RIVER'  # noqa

    # query hydrometric monthly means feature collection based on
    # STATION_NUMBER property and temporal subsetting
    req = f'{url}/collections/hydrometric-monthly-mean/items'
    params = {
        'STATION_NUMBER': '10NC001',
        'datetime': '1979-01/1981-01'
    }
    response = requests.get(req, params=params).json()

    # list of monthly means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    date_ = response['features'][feature]['properties']['DATE']
    assert date_ >= '1979-01'
    assert date_ <= '1981-01'

    # query hydrometric monthly means feature collection based on
    # spatial subsetting
    req = f'{url}/collections/hydrometric-monthly-mean/items'
    params = {
        'bbox': '-80,40,-50,50'
    }
    response = requests.get(req, params=params).json()

    # list of monthly means are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    coords = response['features'][feature]['geometry']['coordinates']
    bbox = [float(b) for b in params['bbox'].split(',')]
    assert coords[0] > bbox[0]
    assert coords[0] < bbox[2]
    assert coords[1] > bbox[1]
    assert coords[1] < bbox[3]

    # access a single hydrometric annual statistics feature
    req = f'{url}/collections/hydrometric-annual-statistics/items/01AA002.1976.discharge-debit'  # noqa

    response = requests.get(req, params=params).json()

    # single annual statistic flow value is returned with correct data
    assert response['id'] == '01AA002.1976.discharge-debit'
    assert response['properties']['MAX_VALUE'] == 281

    # query hydrometric annual statistics feature collection based on data type
    req = f'{url}/collections/hydrometric-annual-statistics/items'
    params = {
        'DATA_TYPE_EN': 'Discharge'
    }

    response = requests.get(req, params=params).json()

    # list of annual statistic values are (first 500) returned with
    # correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['DATA_TYPE_EN'] == 'Discharge'  # noqa

    # query hydrometric annual statistics feature collection based on
    # data type sorted by MAX_VALUE
    req = f'{url}/collections/hydrometric-annual-statistics/items'
    params = {
        'DATA_TYPE_EN': 'Discharge',
        'sortby': 'MAX_VALUE'
    }

    response = requests.get(req, params=params).json()

    # list of annual statistic values are (first 500) returned with correct
    # data sorted by MAX_VALUE
    assert response['features'][0]['properties']['MAX_VALUE'] == 0

    # access a single hydrometric annual peaks feature
    req = f'{url}/collections/hydrometric-annual-peaks/items/02FE012.1961.level-niveaux.maximum-maximale'  # noqa

    response = requests.get(req).json()

    # single annual peak value is returned with correct data
    assert response['id'] == '02FE012.1961.level-niveaux.maximum-maximale'

    # query hydrometric annual peaks feature collection based on STATION_NUMBER
    req = f'{url}/collections/hydrometric-annual-statistics/items'
    params = {
        'STATION_NUMBER': '01AG003'
    }

    response = requests.get(req, params=params).json()

    # list of annual peak values are (first 500) returned with correct data
    feature = randrange(response['numberReturned'])
    assert response['features'][feature]['properties']['STATION_NUMBER'] == '01AG003'  # noqa

    # query hydrometric annual peaks feature collection based on
    # STATION_NUMBER sorted by DATA_TYPE_EN
    req = f'{url}/collections/hydrometric-annual-statistics/items'
    params = {
        'STATION_NUMBER': '01AG003',
        'sortby': 'DATA_TYPE_EN'
    }

    response = requests.get(req, params=params).json()

    # list of annual peak values are (first 500) returned with correct data
    # sorted by DATA_TYPE_EN
    assert response['features'][0]['properties']['DATA_TYPE_EN'] == 'Discharge'
