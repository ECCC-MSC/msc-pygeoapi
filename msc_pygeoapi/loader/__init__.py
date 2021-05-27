# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#         Felix Laframboise <felix.laframboise@canada.ca>
#
# Copyright (c) 2019 Tom Kralidis
# Copyright (c) 2021 Felix Laframboise
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

import click

LOGGER = logging.getLogger(__name__)

try:
    from msc_pygeoapi.loader.bulletins_realtime import bulletins_realtime
    from msc_pygeoapi.loader.citypageweather_realtime import citypageweather
    from msc_pygeoapi.loader.hydat import hydat
    from msc_pygeoapi.loader.ahccd import ahccd
    from msc_pygeoapi.loader.hydrometric_realtime import hydrometric_realtime
    from msc_pygeoapi.loader.hurricanes_realtime import hurricanes
    from msc_pygeoapi.loader.forecast_polygons import forecast_polygons
    from msc_pygeoapi.loader.marine_weather_realtime import marine_weather
    from msc_pygeoapi.loader.cap_alerts_realtime import cap_alerts
    from msc_pygeoapi.loader.swob_realtime import swob_realtime
    from msc_pygeoapi.loader.ltce import ltce
    from msc_pygeoapi.loader.climate_archive import climate_archive
    from msc_pygeoapi.loader.aqhi_realtime import aqhi_realtime
except ImportError as err:
    LOGGER.info('loaders not imported')
    LOGGER.debug(err)


@click.group()
def data():
    pass


# add load commands
try:
    data.add_command(bulletins_realtime)
    data.add_command(citypageweather)
    data.add_command(hydat)
    data.add_command(hurricanes)
    data.add_command(ahccd)
    data.add_command(hydrometric_realtime)
    data.add_command(forecast_polygons)
    data.add_command(marine_weather)
    data.add_command(cap_alerts)
    data.add_command(swob_realtime)
    data.add_command(ltce)
    data.add_command(climate_archive)
    data.add_command(aqhi_realtime)
except NameError as err:
    LOGGER.info('loaders not found')
    LOGGER.debug(err)
