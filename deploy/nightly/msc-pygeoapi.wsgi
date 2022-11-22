# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2020 Government of Canada
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

import os
import sys

os.environ['PYGEOAPI_CONFIG'] = '/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml'
os.environ['PYGEOAPI_OPENAPI'] = '/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/deploy/nightly/msc-pygeoapi-openapi.yml'
os.environ['MSC_PYGEOAPI_ES_URL'] = 'http://localhost:9200'
os.environ['MSC_PYGEOAPI_OGC_API_URL'] = 'https://geomet-dev-03-nightly.cmc.ec.gc.ca/msc-pygeoapi/nightly/latest/'
os.environ['MSC_PYGEOAPI_OGC_API_URL_BASEPATH'] = '/'
os.environ['MSC_PYGEOAPI_TEMPLATES'] = '/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/theme/templates'
os.environ['MSC_PYGEOAPI_STATIC'] = '/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/theme/static'
os.environ['MSC_PYGEOAPI_OGC_SCHEMAS_LOCATION'] = '/data/web/msc-pygeoapi-nightly/latest/schemas.opengis.net'
os.environ['GEOMET_DDI_BASEPATH'] = '/data/geomet/feeds/ddi'
os.environ['GEOMET_SCIENCE_BASEPATH'] = '/data/geomet/feeds/local/SCIENCE'

sys.path.insert(0, '/data/web/msc-pygeoapi-nightly/latest/lib/python3.6/site-packages')

from pygeoapi.flask_app import APP as application
