# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#
# Copyright (c) 2020 Tom Kralidis
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

import importlib
import logging

LOGGER = logging.getLogger(__name__)

PLUGINS = {
    'loader': {
        'hydrometric_realtime': {
            'filename_pattern': 'hydrometric',
            'handler': 'msc_pygeoapi.loader.hydrometric_realtime.HydrometricRealtimeLoader'  # noqa
        },
        'bulletins_realtime': {
            'filename_pattern': 'bulletins/alphanumeric',
            'handler': 'msc_pygeoapi.loader.bulletins.BulletinsRealtimeLoader'  # noqa
        },
        'citypageweather_realtime': {
            'filename_pattern': 'citypage_weather/xml',
            'handler': 'msc_pygeoapi.loader.citypageweather_realtime.CitypageweatherRealtimeLoader' # noqa
        },
        'hurricanes_realtime': {
            'filename_pattern': 'trajectoires/hurricane',
            'handler': 'msc_pygeoapi.loader.hurricanes_realtime.HurricanesRealtimeLoader'  # noqa
        },
        'forecast_polygons': {
            'filename_pattern': 'meteocode/geodata/',
            'handler': 'msc_pygeoapi.loader.forecast_polygons.ForecastPolygonsLoader'  # noqa
        },
        'marine_weather_realtime': {
            'filename_pattern': 'marine_weather/xml/',
            'handler': 'msc_pygeoapi.loader.marine_weather_realtime.MarineWeatherRealtimeLoader'  # noqa
        },
        'cap_alerts_realtime': {
            'filename_pattern': 'alerts/cap',
            'handler': 'msc_pygeoapi.loader.cap_alerts_realtime.CapAlertsRealtimeLoader'  # noqa
        }
    }
}


def load_plugin(plugin_type, plugin_def):
    """
    loads plugin by type

    :param plugin_type: type of plugin (loader, etc.)
    :param plugin_def: plugin definition

    :returns: plugin object
    """

    if plugin_type not in PLUGINS.keys():
        msg = 'Plugin {} not found'.format(plugin_type)
        LOGGER.exception(msg)
        raise InvalidPluginError(msg)

    handler = plugin_def['handler']

    packagename, classname = handler.rsplit('.', 1)

    LOGGER.debug('package name: {}'.format(packagename))
    LOGGER.debug('class name: {}'.format(classname))

    module = importlib.import_module(packagename)
    class_ = getattr(module, classname)
    plugin = class_(plugin_def)
    return plugin


class InvalidPluginError(Exception):
    """Invalid plugin"""
    pass
