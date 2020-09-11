# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
#
# Copyright (c) 2020 Etienne Pelletier
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

import click

from msc_pygeoapi.util import click_abort_if_false


def OPTION_DATASET(*args, **kwargs):

    default_args = ['--dataset']

    default_kwargs = {
        'required': True,
        'help': 'ES dataset to load, or all if loading everything',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_DAYS(**kwargs):

    default_kwargs = {
        'type': int,
    }

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option('--days', '-d', **kwargs)


def OPTION_DB(*args, **kwargs):

    default_args = ['--db']

    default_kwargs = {
        'required': True,
        'help': 'Database connection string',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_DIRECTORY(*args, **kwargs):

    default_args = ['--directory', '-d', 'directory_']

    default_kwargs = {
        'type': click.Path(exists=True, resolve_path=True),
        'required': False,
        'help': 'Path to directory',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_ELASTICSEARCH(*args, **kwargs):

    default_args = ['--es']

    default_kwargs = {
        'help': 'URL to Elasticsearch',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_ES_PASSWORD(*args, **kwargs):

    default_args = ['--password']

    default_kwargs = {
        'help': 'Password to connect to Elasticsearch',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_ES_USERNAME(*args, **kwargs):

    default_args = ['--username']

    default_kwargs = {
        'help': 'Username to connect to Elasticsearch',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_FILE(*args, **kwargs):

    default_args = ['--file', '-f', 'file_']

    default_kwargs = {
        'type': click.Path(exists=True, resolve_path=True),
        'required': False,
        'help': 'Path to file',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_INDEX_NAME(*args, **kwargs):

    default_args = ['--index-name', '-i']

    default_kwargs = {
        'help': 'Elasticsearch index name to delete',
    }

    if not args:
        args = default_args

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option(*args, **kwargs)


def OPTION_YES(**kwargs):

    default_kwargs = {
        'is_flag': True,
        'callback': click_abort_if_false,
        'expose_value': False,
    }

    kwargs = {**default_kwargs, **kwargs} if kwargs else default_kwargs

    return click.option('--yes', **kwargs)
