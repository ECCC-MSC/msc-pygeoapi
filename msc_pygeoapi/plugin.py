# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#         Felix Laframboise <felix.laframboise@canada.ca>
#         Louis-Philippe Rousseau-Lambert
#             <louis-philippe.rousseaulambert@ec.gc.ca>
#
# Copyright (c) 2021 Felix Laframboise
# Copyright (c) 2023 Tom Kralidis
# Copyright (c) 2025 Louis-Philippe Rousseau-Lambert
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
            'handler': 'msc_pygeoapi.loader.bulletins_realtime.BulletinsRealtimeLoader'  # noqa
        },
        'citypageweather_realtime': {
            'filename_pattern': 'citypage_weather',
            'handler': 'msc_pygeoapi.loader.citypageweather_realtime.CitypageweatherRealtimeLoader'  # noqa
        },
        'hurricanes_realtime': {
            'filename_pattern': 'hurricanes/',
            'handler': 'msc_pygeoapi.loader.hurricanes_realtime.HurricanesRealtimeLoader'  # noqa
        },
        'forecast_polygons': {
            'filename_pattern': 'meteocode/geodata/',
            'handler': 'msc_pygeoapi.loader.forecast_polygons.ForecastPolygonsLoader'  # noqa
        },
        'marine_weather_realtime': {
            'filename_pattern': 'marine_weather',
            'handler': 'msc_pygeoapi.loader.marine_weather_realtime.MarineWeatherRealtimeLoader'  # noqa
        },
        'cap_alerts_realtime': {
            'filename_pattern': 'alerts/cap',
            'handler': 'msc_pygeoapi.loader.cap_alerts_realtime.CapAlertsRealtimeLoader'  # noqa
        },
        'alerts_realtime': {
            'filename_pattern': 'dms-geomet/alerts',
            'handler': 'msc_pygeoapi.loader.alerts_realtime.AlertsRealtimeLoader'  # noqa
        },
        'swob_realtime': {
            'filename_pattern': 'observations/swob-ml',
            'handler': 'msc_pygeoapi.loader.swob_realtime.SWOBRealtimeLoader'
        },
        'aqhi_realtime': {
            'filename_pattern': 'air_quality/aqhi',
            'handler': 'msc_pygeoapi.loader.aqhi_realtime.AQHIRealtimeLoader'
        },
        'metnotes_realtime': {
            'filename_pattern': 'metnotes',
            'handler': 'msc_pygeoapi.loader.metnotes.MetNotesRealtimeLoader'
        },
        'cumulative_effects_hs': {
            'filename_pattern': 'model_raqdps-fw/cumulative_effects/json',
            'handler': 'msc_pygeoapi.loader.cumulative_effects_hs.CumulativeEffectsHSLoader'  # noqa
        },
        'prognos_realtime': {
            'filename_pattern': 'stat-post-processing',
            'handler': 'msc_pygeoapi.loader.prognos_realtime.PROGNOSRealtimeLoader'  # noqa
        },
        'thunderstorm_outlook': {
            'filename_pattern': 'ThunderstormOutlook',
            'handler': 'msc_pygeoapi.loader.thunderstorm_outlook.ThunderstormOutlookLoader'  # noqa
        },
        'coastal_flood_risk_index': {
            'filename_pattern': 'CoastalFloodRiskIndex',
            'handler': 'msc_pygeoapi.loader.coastal_flood_risk_index.CoastalFloodRiskIndexLoader'  # noqa
        },
        'weatherstories': {
            'filename_pattern': 'wxstory_',
            'handler': 'msc_pygeoapi.loader.weatherstories_realtime.WeatherStoriesRealtimeLoader'  # noqa
        }
    }
}


def load_plugin(plugin_type, plugin_def, **kwargs):
    """
    loads plugin by type

    :param plugin_type: type of plugin (loader, etc.)
    :param plugin_def: plugin definition

    :returns: plugin object
    """

    if plugin_type not in PLUGINS.keys():
        msg = f'Plugin {plugin_type} not found'
        LOGGER.exception(msg)
        raise InvalidPluginError(msg)

    handler = plugin_def['handler']

    packagename, classname = handler.rsplit('.', 1)

    LOGGER.debug(f'package name: {packagename}')
    LOGGER.debug(f'class name: {classname}')

    module = importlib.import_module(packagename)
    class_ = getattr(module, classname)
    plugin = class_(plugin_def)
    return plugin


class InvalidPluginError(Exception):
    """Invalid plugin"""
    pass
