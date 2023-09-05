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

from msc_pygeoapi.handler.base import BaseHandler
from msc_pygeoapi.plugin import PLUGINS, load_plugin

LOGGER = logging.getLogger(__name__)


class CoreHandler(BaseHandler):
    """base handler"""

    def __init__(self, filepath):
        """
        initializer

        :param filepath: path to file

        :returns: `msc_pygeoapi.handler.core.CoreHandler`
        """

        self.plugin = None

        BaseHandler.__init__(self, filepath)

    def handle(self):
        """
        handle incoming file

        :returns: `bool` of status result
        """

        LOGGER.debug('Detecting filename pattern')
        for key in PLUGINS['loader'].keys():
            if PLUGINS['loader'][key]['filename_pattern'] in self.filepath:
                plugin_def = PLUGINS['loader'][key]
                LOGGER.debug(f'Loading plugin {plugin_def}')
                self.plugin = load_plugin('loader', plugin_def)

        if self.plugin is None:
            msg = 'Plugin not found'
            LOGGER.error(msg)
            raise RuntimeError(msg)

        LOGGER.debug('Handling file')
        status = self.plugin.load_data(self.filepath)
        LOGGER.debug(f'Status: {status}')

        return True

    def __repr__(self):
        return f'<CoreHandler> {self.filepath}'
