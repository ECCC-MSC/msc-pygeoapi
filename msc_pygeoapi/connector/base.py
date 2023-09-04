# =================================================================
#
# Author: Etienne <etienne.pelletier@canada.ca>
#
# Copyright (c) 2021 Etienne Pelletier
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

        raise NotImplementedError()

    def exists(self, resource):
        """
        Determine whether a resource exists

        :param resource: resource name

        :returns: `bool`
        """

        raise NotImplementedError()

    def create(self, resource):
        """
        Create a connector resource

        :param resource: resource name

        :returns: `bool` of creation result
        """

        raise NotImplementedError()

    def get(self):
        """
        Retrieve connector resources
        """

        raise NotImplementedError()

    def delete(self, resources):
        """
        Delete connector resource(s)

        :param resources: `list` of resource names

        :returns: `bool` of deletion result
        """

        raise NotImplementedError()

    def __repr__(self):
        return f'<BaseConnector> {self.name}'
