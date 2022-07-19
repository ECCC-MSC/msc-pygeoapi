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

from importlib import import_module
import logging

import click

LOGGER = logging.getLogger(__name__)


@click.group()
def data():
    pass


commands = (
    ('msc_pygeoapi.loader.bulletins_realtime', 'bulletins_realtime'),
    ('msc_pygeoapi.loader.citypageweather_realtime', 'citypageweather'),
    ('msc_pygeoapi.loader.hydat', 'hydat'),
    ('msc_pygeoapi.loader.ahccd', 'ahccd'),
    ('msc_pygeoapi.loader.hydrometric_realtime', 'hydrometric_realtime'),
    ('msc_pygeoapi.loader.hurricanes_realtime', 'hurricanes'),
    ('msc_pygeoapi.loader.forecast_polygons', 'forecast_polygons'),
    ('msc_pygeoapi.loader.marine_weather_realtime', 'marine_weather'),
    ('msc_pygeoapi.loader.cap_alerts_realtime', 'cap_alerts'),
    ('msc_pygeoapi.loader.swob_realtime', 'swob_realtime'),
    ('msc_pygeoapi.loader.aqhi_realtime', 'aqhi_realtime'),
    ('msc_pygeoapi.loader.ltce', 'ltce'),
    ('msc_pygeoapi.loader.climate_archive', 'climate_archive'),
)

for module, name in commands:
    try:
        mod = import_module(module)
        data.add_command(getattr(mod, name))
    except ImportError as err:
        LOGGER.info(
            f'msc-pygeoapi data {name.replace("_", "-")} command unavailable.'
        )
        LOGGER.debug(f'Import error when loading {module}.{name}: {err}.')
