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


def test_process_executions(url):
    """Test a GeoMet Process Collection execute response"""

    request = f'{url}/processes/raster-drill/execution'

    # Data to send with the request (typically a dictionary)
    data = {
          "inputs": {
            "format": "CSV",
            "layer": "CMIP5.TT.RCP26.YEAR.ANO_PCTL50",
            "x": -114.74968888274337,
            "y": 51.132831196692806
            }
    }

    response = requests.post(request, json=data)

    assert response.status_code == 200
