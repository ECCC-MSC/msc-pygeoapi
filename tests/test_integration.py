# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2026 Tom Kralidis
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

import pytest
import requests


@pytest.fixture()
def url(pytestconfig):
    return pytestconfig.getoption('url')


def test_landing_page(url):
    """Test landing page"""

    request = url
    response = requests.get(request).json()

    assert response['title'] == 'MSC GeoMet - GeoMet-OGC-API'

    request = f'{url}/?lang=fr'
    params = {
        'lang': 'fr'
    }

    response = requests.get(request, params=params)
    assert response.headers['X-Powered-By'] == 'pygeoapi 0.20.0'
    response = response.json()

    assert response['title'] == 'GeoMet du SMC - GeoMet-OGC-API'


def test_collections(url):
    """Test collections"""

    request = f'{url}/collections'
    response = requests.get(request)

    assert response.ok

    response = response.json()

    collections = response['collections']

    assert len(collections) == 100

    collection_errors = []

    for collection in collections:
        # test all collections by "touching" the data
        # for itemType data, this requires a .../schema invocation
        # for coverage/EDR data, this is already done by pygeoapi
        # to generate dimensions/parameters in the collection metadata
        collection_id = collection['id']
        if collection.get('itemType') is not None:
            request = f'{url}/collections/{collection_id}/schema'
            response = requests.get(request)

            if not response.ok:
                collection_errors.append({
                   'collection': collection_id,
                   'status_code': response.status_code
                })

    assert len(collection_errors) == 0, collection_errors
