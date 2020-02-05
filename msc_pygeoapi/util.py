# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
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

import logging
from urllib.parse import urlparse

from elasticsearch import Elasticsearch


LOGGER = logging.getLogger(__name__)


def get_es(url):
    """
    helper function to instantiate an Elasticsearch connection

    :param url: URL of ES endpoint
    :returns: `elasticsearch.Elasticsearch` object
    """

    url_parsed = urlparse(url)

    LOGGER.debug('Connecting to Elasticsearch')

    if url_parsed.port is None:  # proxy to default HTTP(S) port
        if url_parsed.scheme == 'https':
            port = 443
        else:
            port = 80
    else:  # was set explictly
        port = url_parsed.port

    url_settings = {
        'host': url_parsed.hostname,
        'port': port
    }

    if url_parsed.path:
        url_settings['url_prefix'] = url_parsed.path

    LOGGER.debug('URL settings: {}'.format(url_settings))

    es = Elasticsearch([url_settings])

    if not es.ping():
        msg = 'Cannot connect to Elasticsearch'
        LOGGER.error(msg)
        raise RuntimeError(msg)

    return es


def click_abort_if_false(ctx, param, value):
    """
    Helper function to abort (or not) `click` command on prompt/confirm

    :param ctx: context manager
    :param param: name of click option
    :param value: `bool` of value

    :returns: abort of click command if `False`
    """

    if not value:
        ctx.abort()
