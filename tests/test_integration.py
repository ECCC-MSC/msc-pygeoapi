# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2026 Tom Kralidis
# Copyright (c) 2025 Mustafa Zafar
# Copyright (c) 2026 Justin Tran
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

    assert len(collections) == 103

    collection_errors = []

    # certain collections can contain 0 features
    # an exception is made for these cases
    skip_feature_collections = [
        'coastal_flood_risk_index',
        'hurricanes-cyclone-realtime',
        'hurricanes-error_cone-realtime',
        'hurricanes-track-realtime',
        'hurricanes-wind_radii-realtime',
        'metnotes',
        'thunderstorm_outlook',
        'weather-alerts'
    ]

    # certain collections do not contain the extent.temporal.grid property
    # an exception is made for these cases
    skip_coverage_collections = [
        'weather:cansips:100km:forecast:seasonal-products',
        'weather:cansips:100km:forecast:monthly-products'
    ]

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
                   'status_code': response.status_code,
                   'error_message': 'Error in retrieving .../schema endpoint'
                })

            # Single item check
            request = f'{url}/collections/{collection_id}/items'
            response = requests.get(request, params={'limit': 1})
            if not response.ok:
                collection_errors.append({
                   'collection': collection_id,
                   'status_code': response.status_code,
                   'error_message': 'Error in retrieving .../items endpoint'
                })
            else:
                instance = response.json()
                if len(instance['features']) == 0:
                    if collection_id not in skip_feature_collections:
                        msg = f'No features for collection {collection_id}'
                        collection_errors.append({
                            'collection': collection_id,
                            'status_code': None,
                            'error_message': msg
                        })
                else:
                    feature_id = instance['features'][0]['id']
                    request = (
                        f'{url}/collections/{collection_id}/items/{feature_id}'
                    )
                    response = requests.get(request)
                    if not response.ok:
                        msg = f'Failed to retrieve feature {feature_id}'
                        collection_errors.append({
                            'collection': collection_id,
                            'status_code': response.status_code,
                            'error_message': msg
                        })
        else:
            # Case of coverage data
            request = f'{url}/collections/{collection_id}'
            response = requests.get(request)
            if not response.ok:
                collection_errors.append({
                   'collection': collection_id,
                   'status_code': response.status_code,
                   'error_message': 'Failed to retrieve collection data'
                })
            else:
                instance = response.json()
                spatial_extent = instance['extent']['spatial']
                if 'grid' not in spatial_extent:
                    msg = 'Missing property extent.spatial.grid'
                    collection_errors.append({
                        'collection': collection_id,
                        'status_code': None,
                        'error_message': msg
                    })

                if len(spatial_extent['bbox'][0]) != 4:
                    msg = 'Invalid length for extent.spatial.bbox'
                    collection_errors.append({
                        'collection': collection_id,
                        'status_code': None,
                        'error_message': msg
                    })

                if 'temporal' in instance['extent']:
                    temporal_extent = instance['extent']['temporal']
                    if 'interval' not in temporal_extent:
                        msg = 'Missing property extent.temporal.interval'
                        collection_errors.append({
                            'collection': collection_id,
                            'status_code': None,
                            'error_message': msg
                        })

                    if 'grid' not in temporal_extent:
                        if collection_id not in skip_coverage_collections:
                            msg = 'Missing property extent.temporal.grid'
                            collection_errors.append({
                                'collection': collection_id,
                                'status_code': None,
                                'error_message': msg
                            })

            request = f'{url}/collections/{collection_id}/schema'
            response = requests.get(request)
            if not response.ok:
                collection_errors.append({
                   'collection': collection_id,
                   'status_code': response.status_code,
                   'error_message': 'Error in retrieving .../schema endpoint'
                })
            else:
                instance = response.json()
                if (len(instance['properties'].keys()) == 0):
                    msg = 'No keys found in properties of its schema'
                    collection_errors.append({
                        'collection': collection_id,
                        'status_code': None,
                        'error_message': msg
                    })

            request = f'{url}/collections/{collection_id}/coverage'
            response = requests.get(request)
            if not response.ok:
                collection_errors.append({
                    'collection': collection_id,
                    'status_code': response.status_code,
                    'error_message': 'Failed to retrieve coverage data'
                })
            else:
                instance = response.json()
                domain_axes = sorted(instance['domain']['axes'].keys())
                ranges_params = instance['ranges'].keys()

                for ranges_param in ranges_params:
                    param_axes = instance['ranges'][ranges_param]['axisNames']
                    # Sorting required, as not always in alphabetic order
                    param_axes.sort(key=str.lower)

                    if param_axes != domain_axes:
                        msg = (
                            f'ranges.{ranges_param}.axisNames do not '
                            f'matach values of domain.axis keys'
                        )
                        collection_errors.append({
                            'collection': collection_id,
                            'status_code': None,
                            'error_message': msg
                        })

                params = instance['parameters'].keys()
                for param in params:
                    if param not in ranges_params:
                        msg = f'Missing key {param} in ranges'
                        collection_errors.append({
                            'collection': collection_id,
                            'status_code': None,
                            'error_message': msg
                        })

    assert len(collection_errors) == 0, collection_errors


def test_processes(url):
    """Test processes"""

    request = f'{url}/processes'
    response = requests.get(request)
    assert response.status_code == 200

    instance = response.json()
    process_errors = []
    assert len(instance['processes']) == 1

    for process in instance['processes']:
        indiv_process = f"{url}/processes/{process['id']}"
        response = requests.get(indiv_process)
        instance = response.json()

        if not response.ok:
            process_errors.append({
                'process': process,
                'status_code': response.status_code
            })
        else:
            if instance['jobControlOptions'] != ['sync-execute']:
                process_errors.append({
                    'process': process,
                    'status_code': None,
                    'error_message': 'Unexpected value for sync-execute'
                })
            if instance['outputTransmission'] != ['value']:
                process_errors.append({
                    'process': process,
                    'status_code': None,
                    'error_message': 'Unexpected value for outputTransmission'
                })

    assert len(process_errors) == 0, process_errors
