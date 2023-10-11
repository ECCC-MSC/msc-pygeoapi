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

import pytest

from util import get_test_file_path


@pytest.fixture()
def openapi_config():
    openapi_document = '../deploy/default/msc-pygeoapi-openapi.yml'
    with open(get_test_file_path(openapi_document)) as fh:
        return fh.read()


def test_openapi_config(openapi_config):
    """Test OpenAPI document"""

    assert 'https://api.w' not in openapi_config
    assert 'url: ${MSC_PYGEOAPI_OGC_API_URL}' in openapi_config
    assert '$ref: ${MSC_PYGEOAPI_OGC_API_URL}' in openapi_config
