# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2021 Tom Kralidis
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

from datetime import datetime, date, time, timedelta
import json
import logging

from parse import parse


LOGGER = logging.getLogger(__name__)

DATETIME_RFC3339_FMT = '%Y-%m-%dT%H:%M:%SZ'
DATETIME_RFC3339_MILLIS_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'


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


def strftime_rfc3339(datetimeobj):
    """
    helper function to convert datetime object to RFC3393 compliant string.

    :param datetimeobj: `datetime` object
    :returns: RFC3339 compliant datetime `str`
    """
    return datetimeobj.strftime(DATETIME_RFC3339_FMT)


def generate_datetime_range(start, end, delta):
    """
    Generator that yields datetime objects between start and end,
    inclusively.
    :param start: datetime object
    :param end: datetime object
    :param delta: timedelta object
    :return: Generator of datetime objects
    """

    current = start
    while current <= end:
        yield current
        current += delta


def check_es_indexes_to_delete(
    indexes, days, pattern='{index_name}.{year:d}-{month:d}-{day:d}'
):
    """
    helper function to determine ES indexes that are older than a certain date

    :param indexes: list of ES index names
    :param days: number of days used to determine deletion criteria
    :param pattern: pattern used to parse index name
                    (default: {index_name}.{year:d}-{month:d}-{day:d})

    :returns: list of indexes to delete
    """

    today = datetime.utcnow()
    to_delete = []

    for index in indexes:
        parsed = parse(pattern, index)
        parsed.named.pop('index_name')
        index_date = datetime(**parsed.named)
        if index_date < (today - timedelta(days=days)):
            to_delete.append(index)

    return to_delete


def configure_es_connection(es, username, password, ignore_certs=False):
    """
    helper function to create an ES connection configuration dictionnary with
    the relevant params passed via CLI.

    :param es: `str` ES url
    :param username: `str` ES username for authentication
    :param password: `str` ES password for authentication
    :param ignore_certs: `bool` indicates whether to ignore certs when
                         connecting. Defaults to False.

    :returns: `dict` containing ES connection configuration
    """
    conn_config = {}

    if es:
        conn_config['url'] = es
    if all([username, password]):
        conn_config['auth'] = (username, password)

    # negate ignore_certs CLI flag value to get verify_certs value
    conn_config['verify_certs'] = not ignore_certs

    return conn_config
