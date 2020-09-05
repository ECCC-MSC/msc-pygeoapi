###############################################################################
#
# Copyright (C) 2020 Tom Kralidis
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################

import logging
import sys

LOGGER = logging.getLogger(__name__)


def setup_logger(loglevel, logfile=None):
    """
    Setup configuration
    :param loglevel: logging level
    :param logfile: logfile location
    :returns: void (creates logging instance)
    """

    log_format = '[%(asctime)s] %(levelname)s - %(message)s'
    date_format = '%Y-%m-%dT%H:%M:%SZ'

    loglevels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET,
    }

    loglevel = loglevels[loglevel]

    if logfile is not None:
        if logfile == 'stdout':
            logging.basicConfig(
                level=loglevel,
                datefmt=date_format,
                format=log_format,
                stream=sys.stdout,
            )
        else:
            logging.basicConfig(
                level=loglevel,
                datefmt=date_format,
                format=log_format,
                filename=logfile,
            )

        LOGGER.debug('Logging initialized')
