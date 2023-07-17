# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
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

import click

from msc_pygeoapi.env import (
    MSC_PYGEOAPI_LOGGING_LOGLEVEL, MSC_PYGEOAPI_LOGGING_LOGFILE)

# import and setup logger prior to other imports in order to enable logging
# during importing of data and process commands
from msc_pygeoapi.log import setup_logger
setup_logger(MSC_PYGEOAPI_LOGGING_LOGLEVEL, MSC_PYGEOAPI_LOGGING_LOGFILE)

from msc_pygeoapi.loader import data  # noqa
from msc_pygeoapi.loader import metadata  # noqa
from msc_pygeoapi.process import process  # noqa


__version__ = '0.11.4'


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(data)
cli.add_command(metadata)
cli.add_command(process)
