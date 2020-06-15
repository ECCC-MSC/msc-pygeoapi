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

from datetime import datetime, date, time
import json
import logging
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, BulkIndexError

LOGGER = logging.getLogger(__name__)

VERIFY = False

DATETIME_RFC3339_FMT = '%Y-%m-%dT%H:%M:%SZ'
DATETIME_RFC3339_MAPPING = {
    'type': 'date',
    'format': 'date_time_no_millis',
    'ignore_malformed': False
}


def get_es(url, auth=None):
    """
    helper function to instantiate an Elasticsearch connection

    :param url: URL of ES endpoint
    :param auth: HTTP username-password tuple for authentication (optional)
    :returns: `elasticsearch.Elasticsearch` object
    """

    url_parsed = urlparse(url)
    url_settings = {
        'host': url_parsed.hostname
    }

    LOGGER.debug('Connecting to Elasticsearch')

    if url_parsed.port is None:  # proxy to default HTTP(S) port
        if url_parsed.scheme == 'https':
            url_settings['port'] = 443
            url_settings['scheme'] = url_parsed.scheme
        else:
            url_settings['port'] = 80
    else:  # was set explictly
        url_settings['port'] = url_parsed.port

    if url_parsed.path:
        url_settings['url_prefix'] = url_parsed.path

    LOGGER.debug('URL settings: {}'.format(url_settings))

    if auth is None:
        es = Elasticsearch([url_settings], verify_certs=VERIFY)
    else:
        es = Elasticsearch([url_settings], http_auth=auth, verify_certs=VERIFY)

    if not es.ping():
        msg = 'Cannot connect to Elasticsearch'
        LOGGER.error(msg)
        raise RuntimeError(msg)

    return es


def submit_elastic_package(es, package, request_size=10000):
    """
    Helper function to send an update request to Elasticsearch and
    log the status of the request. Returns True iff the upload succeeded.

    :param es: Elasticsearch client object.
    :param package: Iterable of bulk API update actions.
    :param request_size: Number of documents to upload per request.
    :returns: `bool` of whether the operation was successful.
    """

    inserts = 0
    updates = 0
    noops = 0
    errors = []

    try:
        for ok, response in streaming_bulk(es, package,
                                           chunk_size=request_size,
                                           request_timeout=30,
                                           raise_on_error=False):
            if not ok:
                errors.append(response)
            else:
                status = response['update']['result']

                if status == 'created':
                    inserts += 1
                elif status == 'updated':
                    updates += 1
                elif status == 'noop':
                    noops += 1
                else:
                    LOGGER.error('Unhandled status code {}'.format(status))
                    errors.append(response)
    except BulkIndexError as err:
        LOGGER.error('Unable to perform bulk insert due to: {}'
                     .format(err.errors))
        return False

    total = inserts + updates + noops
    LOGGER.info('Inserted package of {} observations ({} inserts, {} updates,'
                ' {} no-ops'.format(total, inserts, updates, noops))

    if len(errors) > 0:
        LOGGER.warning('{} errors encountered in bulk insert: {}'.format(
            len(errors), errors))
        return False

    return True


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


def json_pretty_print(data):
    """
    Pretty print a JSON serialization
    :param data: `dict` of JSON
    :returns: `str` of pretty printed JSON representation
    """

    return json.dumps(data, indent=4, default=json_serial)


def json_serial(obj):
    """
    helper function to convert to JSON non-default
    types (source: https://stackoverflow.com/a/22238613)
    :param obj: `object` to be evaluate
    :returns: JSON non-default type to `str`
    """

    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')

    msg = '{} type {} not serializable'.format(obj, type(obj))
    LOGGER.error(msg)
    raise TypeError(msg)


def _get_date_format(date):
    """
    Convenience function to parse dates

    :param date: date form

    returns: date as datetime object
    """
    for char in ["T", "-", ":"]:
        if char in date:
            date = date.replace(char, '')
    date = date[:14]
    date = datetime.strptime(date, "%Y%m%d%H%M%S")

    return date


def _get_element(node, path, attrib=None):
    """
    Convenience function to resolve lxml.etree.Element handling

    :param node: xml node
    :param path: path in the xml node
    :param attrib: attribute to get in the node

    returns: attribute as text or None
    """

    val = node.find(path)
    if attrib is not None and val is not None:
        return val.attrib.get(attrib)
    if hasattr(val, 'text') and val.text not in [None, '']:
        return val.text
    return None


def strftime_rfc3339(datetimeobj: datetime) -> str:
    """
    helper function to convert datetime object to RFC3393 compliant string.

    :param datetimeobj: `datetime` object
    :returns: RFC3339 compliant datetime `str`
    """
    return datetimeobj.strftime(DATETIME_RFC3339_FMT)
