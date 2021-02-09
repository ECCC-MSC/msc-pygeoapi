# =================================================================
#
# Author: Etienne <etienne.pelletier@canada.ca>
#
# Copyright (c) 2021 Etienne Pelletier
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

import logging

LOGGER = logging.getLogger(__name__)


class BaseConnector:
    """generic Connector ABC"""

    def __init__(self, connector_def):
        """
        Initialize BaseConnector object

        :param connector_def: connector definition

        :returns: msc_pygeoapi.connector.base.BaseConnector
        """

    def connect(self):
        """
        Create connection to connector
        """

    def create(self):
        """
        Create a connector resource
        """

        raise NotImplementedError()

    def get(self):
        """
        Retrieve connector resources
        """

        raise NotImplementedError()

    def delete(self):
        """
        Delete connector resource(s)
        """

        raise NotImplementedError()

    def __repr__(self):
        return '<BaseConnector> {}'.format(self.name)
