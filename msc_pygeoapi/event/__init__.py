# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
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


class Event:
    """core event"""

    def __init__(self, parent):
        """initialize"""
        pass

    def dispatch(self, parent):
        """
        sarracenia dispatcher

        :param parent: `sarra.sr_subscribe.sr_subscribe`

        :returns: `bool` of dispatch result
        """

        try:
            from msc_pygeoapi.handler.core import CoreHandler

            filepath = parent.msg.local_file
            parent.logger.debug('Filepath: {}'.format(filepath))
            handler = CoreHandler(filepath)
            result = handler.handle()
            parent.logger.debug('Result: {}'.format(result))
            return True
        except Exception as err:
            parent.logger.warning(err)
            return False

    def __repr__(self):
        return '<Event>'

self.plugin = 'FileEvent'
