# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#         Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#         Etienne Pelletier <etienne.pelletier@ec.gc.ca>
#
# Copyright (c) 2021 Tom Kralidis
# Copyright (c) 2024 Louis-Philippe Rousseau-Lambert
# Copyright (c) 2024 Etienne Pelletier
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

from sarracenia.flowcb import FlowCB

LOGGER = logging.getLogger(__name__)


class EventBase(FlowCB):

    def process_messages(self, worklist) -> bool:
        """
        Process messages from the worklist

        :param worklist: `sarracenia.flow.worklist`

        :returns: `bool`
        """

        for msg in worklist.incoming:

            try:
                from msc_pygeoapi.handler.core import CoreHandler

                filepath = f"{msg['new_dir']}/{msg['new_file']}"
                LOGGER.debug(f'Filepath: {filepath}')
                handler = CoreHandler(filepath)
                result = handler.handle()
                LOGGER.debug(f'Result: {result}')
            except Exception as err:
                LOGGER.error(f'Error handling message: {err}')
                worklist.failed.append(msg)
                return False

        return True


class EventAfterWork(EventBase):

    def after_work(self, worklist) -> None:
        """
        sarracenia after_work dispatcher

        :param worklist: `sarracenia.flow.worklist`

        :returns: `bool`
        """
        return self.process_messages(worklist)


class EventAfterAccept(EventBase):

    def after_accept(self, worklist) -> None:
        """
        sarracenia after_accept dispatcher

        :param worklist: `sarracenia.flow.worklist`

        :returns: `bool`
        """
        return self.process_messages(worklist)
