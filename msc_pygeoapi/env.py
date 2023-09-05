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

import logging
import os


LOGGER = logging.getLogger(__name__)

LOGGER.info('Fetching environment variables')

MSC_PYGEOAPI_LOGGING_LOGLEVEL = os.getenv('MSC_PYGEOAPI_LOGGING_LOGLEVEL',
                                          'ERROR')
MSC_PYGEOAPI_LOGGING_LOGFILE = os.getenv('MSC_PYGEOAPI_LOGGING_LOGFILE',
                                         None)

MSC_PYGEOAPI_OGC_API_URL = os.getenv('MSC_PYGEOAPI_OGC_API_URL'.rstrip('/'),
                                     None)

MSC_PYGEOAPI_ES_URL = os.getenv('MSC_PYGEOAPI_ES_URL', None)
MSC_PYGEOAPI_ES_TIMEOUT = int(os.getenv('MSC_PYGEOAPI_ES_TIMEOUT', 90))
MSC_PYGEOAPI_CACHEDIR = os.getenv('MSC_PYGEOAPI_CACHEDIR', '/tmp')

MSC_PYGEOAPI_ES_USERNAME = os.getenv('MSC_PYGEOAPI_ES_USERNAME', None)
MSC_PYGEOAPI_ES_PASSWORD = os.getenv('MSC_PYGEOAPI_ES_PASSWORD', None)

if None in (MSC_PYGEOAPI_ES_USERNAME, MSC_PYGEOAPI_ES_PASSWORD):
    LOGGER.debug(
        'Missing Elasticsearch authentication information:'
        ' Continuing without authentication'
    )
    MSC_PYGEOAPI_ES_AUTH = None
else:
    MSC_PYGEOAPI_ES_AUTH = (MSC_PYGEOAPI_ES_USERNAME, MSC_PYGEOAPI_ES_PASSWORD)

MSC_PYGEOAPI_BASEPATH = os.path.dirname(os.path.realpath(__file__))

GEOMET_HPFX_BASEPATH = os.getenv('GEOMET_HPFX_BASEPATH', None)
GEOMET_SCIENCE_BASEPATH = os.getenv('GEOMET_SCIENCE_BASEPATH', None)
