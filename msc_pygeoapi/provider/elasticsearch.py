# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
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

import logging

from pygeoapi.provider.elasticsearch_ import ElasticsearchCatalogueProvider

LOGGER = logging.getLogger(__name__)


class ElasticsearchCatalogueWMOWIS2GDCProvider(ElasticsearchCatalogueProvider):
    """Elasticsearch Provider for WMO WIS2 Global Discovery Catalogue"""

    def __init__(self, provider_def):
        super().__init__(provider_def)

    def mask_prop(self, property_name):
        """
        generate property name based on ES backend setup

        :param property_name: property name

        :returns: masked property name
        """

        return property_name

    def __repr__(self):
        return '<ElasticsearchCatalogueWMOWIS2GDCProvider> {}'.format(self.data)  # noqa


class ElasticsearchWMOWIS2BrokerMessagesProvider(ElasticsearchCatalogueWMOWIS2GDCProvider):  # noqa
    """Elasticsearch Provider for WMO WIS2 Notification Messages"""

    def __init__(self, provider_def):
        super().__init__(provider_def)

    def __repr__(self):
        return '<ElasticsearchWMOWIS2BrokerMessagesProvider> {}'.format(self.data)  # noqa