# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#         Felix Laframboise <felix.laframboise@canada.ca>
#
# Copyright (c) 2022 Tom Kralidis
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

from msc_pygeoapi.loader.discovery_metadata import discovery_metadata

LOGGER = logging.getLogger(__name__)


@click.group()
def data():
    """Data publishing"""
    pass


@click.group()
def metadata():
    """Metadata publishing"""
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
    ('msc_pygeoapi.loader.aqhi_stations', 'aqhi_stations'),
    ('msc_pygeoapi.loader.ltce', 'ltce'),
    ('msc_pygeoapi.loader.climate_archive', 'climate_archive'),
    ('msc_pygeoapi.loader.metnotes', 'metnotes'),
    ('msc_pygeoapi.loader.cumulative_effects_hs', 'cumulative_effects_hs'),
    ('msc_pygeoapi.loader.radar_coverage_realtime', 'radar_coverage_realtime'),
    ('msc_pygeoapi.loader.nwp_dataset_footprints', 'nwp_dataset_footprints')
)

for module, name in commands:
    try:
        mod = import_module(module)
        data.add_command(getattr(mod, name))
    except ImportError as err:
        command_name = name.replace('_', '-')
        LOGGER.info(
            'msc-pygeoapi data {} command unavailable.'.format(command_name)
        )
        module_name = '{}.{}'.format(module, name)
        msg = 'Import error when loading {}: {}'.format(module_name, err)
        LOGGER.debug(msg)


metadata.add_command(discovery_metadata)
