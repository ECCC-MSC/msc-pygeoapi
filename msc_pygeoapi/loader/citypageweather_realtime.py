# =================================================================
#
# Author: Etienne Pelletier
#         <etienne.pelletier@ec.gc.ca>
#
# Author: Louis-Philippe Rousseau-Lambert
#         <Louis-Philippe.RousseauLambert2@canada.ca>

# Copyright (c) 2020 Louis-Philippe Rousseau-Lambert
# Copyright (c) 2023 Tom Kralidis
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

import click
from datetime import datetime
import json
import logging
from lxml import etree
import os
from parse import parse
from pathlib import Path
import re

from msc_pygeoapi import cli_options
from msc_pygeoapi.connector.elasticsearch_ import ElasticsearchConnector
from msc_pygeoapi.env import MSC_PYGEOAPI_BASEPATH, MSC_PYGEOAPI_CACHEDIR
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import (
    DATETIME_RFC3339_FMT,
    configure_es_connection,
    _get_element,
    safe_cast_to_number
)

LOGGER = logging.getLogger(__name__)

# cleanup settings
DAYS_TO_KEEP = 30

# Index settings
INDEX_NAME = 'citypageweather_realtime'

CPW_PROPERTIES = {
    'properties': {
        'identifier': {
            'type': 'text',
            'fields': {'raw': {'type': 'keyword'}}
        },
        'name': {
            'type': 'object',
            'properties': {
                'en': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'fr': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}}
            }
        },
        'region': {
            'type': 'object',
            'properties': {
                'en': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'fr': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}}
            }
        },
        'url': {
            'type': 'object',
            'properties': {
                'en': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'fr': {'type': 'text', 'fields': {'raw': {'type': 'keyword'}}}
            }
        },
        'currentConditions': {
            'type': 'object',
            'properties': {
                'icon': {
                    'type': 'text',
                    'fields': {'raw': {'type': 'keyword'}}
                },
                'timestamp': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        },
                        'fr': {
                            'type': 'date',
                            'format': 'date_time_no_millis'
                        }
                    }
                },
                'relativeHumidity': {
                    'type': 'object',
                    'properties': {
                        'unitType': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'float'
                                },
                                'fr': {
                                    'type': 'float'
                                }
                            }
                        }
                    }
                },
                'wind': {
                    'type': 'object',
                    'properties': {
                        'speed': {
                            'type': 'object',
                            'properties': {
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'gust': {
                            'type': 'object',
                            'properties': {
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'integer'
                                        },
                                        'fr': {
                                            'type': 'integer'
                                        }
                                    }
                                }
                            }
                        },
                        'direction': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'bearing': {
                            'type': 'object',
                            'properties': {
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'float'
                                        },
                                        'fr': {
                                            'type': 'float'
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'temperature': {
                    'type': 'object',
                    'properties': {
                        'unitType': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'units': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'float'
                                },
                                'fr': {
                                    'type': 'float'
                                }
                            }
                        }
                    }
                },
                'dewpoint': {
                    'type': 'object',
                    'properties': {
                        'unitType': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'units': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'float'
                                },
                                'fr': {
                                    'type': 'float'
                                }
                            }
                        }
                    }
                },
                'windChill': {
                    'type': 'object',
                    'properties': {
                        'unitType': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'float'
                                },
                                'fr': {
                                    'type': 'float'
                                }
                            }
                        }
                    }
                },
                'station': {
                    'type': 'object',
                    'properties': {
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'lat': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'lon': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'code': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        }
                    }
                },
                'condition': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'pressure': {
                    'type': 'object',
                    'properties': {
                        'unitType': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'units': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'value': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'float'
                                },
                                'fr': {
                                    'type': 'float'
                                }
                            }
                        }
                    }
                }
            }
        },
        'forecastGroup': {
            'type': 'object',
            'properties': {
                'timestamp': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'regionalNormals': {
                    'type': 'object',
                    'properties': {
                        'textSummary': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'temperature': {
                            'type': 'nested',
                            'properties': {
                                'class': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'integer'
                                        },
                                        'fr': {
                                            'type': 'integer'
                                        }
                                    }
                                },
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'forecasts': {
                    'type': 'nested',
                    'properties': {
                        'period': {
                            'type': 'object',
                            'properties': {
                                'textForecastName': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'textSummary': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'textForecast_name': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'cloud_precip': {
                            'type': 'text'
                        },
                        'abbreviated_forecast': {
                            'type': 'object',
                            'properties': {
                                'icon': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'pop': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'text_summary': {
                                    'type': 'text'
                                }
                            }
                        },
                        'temperatures': {
                            'type': 'object',
                            'properties': {
                                'text_summary': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'temp_high': {
                                    'type': 'integer'
                                },
                                'temp_low': {
                                    'type': 'integer'
                                }
                            }
                        },
                        'winds': {
                            'type': 'object',
                            'properties': {
                                'text_summary': {
                                    'type': 'text'
                                },
                                'periods': {
                                    'type': 'object',
                                    'properties': {
                                        'index': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'rank': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'speed': {
                                            'type': 'object',
                                            'properties': {
                                                'unitType': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                'units': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'gust': {
                                            'type': 'object',
                                            'properties': {
                                                'unitType': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                'units': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'direction': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'bearing': {
                                            'type': 'object',
                                            'properties': {
                                                'units': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'en': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        },
                                                        'fr': {
                                                            'type': 'text',
                                                            'fields': {
                                                                'raw': {
                                                                    'type': 'keyword'  # noqa
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'precipitation': {
                            'type': 'object',
                            'properties': {
                                'textSummary': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'precip_periods': {
                                    'type': 'object',
                                    'properties': {
                                        'start': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'end': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'precip_type': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'windChill': {
                            'type': 'object'
                        },
                        'uv': {
                            'type': 'object',
                            'properties': {
                                'category': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'text_summary': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'index': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'rel_hum': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'humidex': {
                            'type': 'object'
                        }
                    }
                }
            }
        },
        'hourlyForecastGroup': {
            'type': 'object',
            'properties': {
                'timestamp': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'hourlyForecasts': {
                    'type': 'nested',
                    'properties': {
                        'condition': {
                            'type': 'object',
                            'properties': {
                                'en': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'fr': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                }
                            }
                        },
                        'humidex': {
                            'type': 'object',
                            'properties': {
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'byte'
                                        },
                                        'fr': {
                                            'type': 'byte'
                                        }
                                    }
                                }
                            }
                        },
                        'iconCode': {
                            'type': 'object',
                            'properties': {
                                'format': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'url': {
                                    'type': 'text',
                                    'fields': {'raw': {'type': 'keyword'}}
                                },
                                'value': {
                                    'type': 'byte'
                                }
                            }
                        },
                        'lop': {
                            'type': 'object',
                            'properties': {
                                'category': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'byte'
                                        },
                                        'fr': {
                                            'type': 'byte'
                                        }
                                    }
                                }
                            }
                        },
                        'temperature': {
                            'type': 'object',
                            'properties': {
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'units': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {'type': 'keyword'}
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'byte'
                                        },
                                        'fr': {
                                            'type': 'byte'
                                        }
                                    }
                                }
                            }
                        },
                        'timestamp': {
                            'type': 'date'
                        },
                        'uv': {
                            'type': 'object',
                            'properties': {
                                'index': {
                                    'type': 'object',
                                    'properties': {
                                        'value': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'byte'
                                                },
                                                'fr': {
                                                    'type': 'byte'
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'wind': {
                            'type': 'object',
                            'properties': {
                                'direction': {
                                    'type': 'object',
                                    'properties': {
                                        'value': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'windDirFull': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                'gust': {
                                    'type': 'object',
                                    'properties': {
                                        'unitType': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'units': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'value': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'byte'
                                                },
                                                'fr': {
                                                    'type': 'byte'
                                                }
                                            }
                                        }
                                    }
                                },
                                'speed': {
                                    'type': 'object',
                                    'properties': {
                                        'unitType': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'units': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'value': {
                                            'type': 'object',
                                            'properties': {
                                                'en': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                },
                                                'fr': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'raw': {
                                                            'type': 'keyword'
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        'windChill': {
                            'type': 'object',
                            'properties': {
                                'unitType': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {
                                                    'type': 'keyword'
                                                }
                                            }
                                        },
                                        'fr': {
                                            'type': 'text',
                                            'fields': {
                                                'raw': {
                                                    'type': 'keyword'
                                                }
                                            }
                                        }
                                    }
                                },
                                'value': {
                                    'type': 'object',
                                    'properties': {
                                        'en': {
                                            'type': 'byte'
                                        },
                                        'fr': {
                                            'type': 'byte'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        'riseSet': {
            'type': 'object',
            'properties': {
                'disclaimer': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'sunrise': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'date'
                        },
                        'fr': {
                            'type': 'date'
                        }
                    }
                },
                'sunset': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'date'
                        },
                        'fr': {
                            'type': 'date'
                        }
                    }
                }
            }
        },
        'warnings': {
            'type': 'nested',
            'properties': {
                'description': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'eventIssue': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'date'
                        },
                        'fr': {
                            'type': 'date'
                        }
                    }
                },
                'expiryTime': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'date'
                        },
                        'fr': {
                            'type': 'date'
                        }
                    }
                },
                'alertColourLevel': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'priority': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'type': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                },
                'url': {
                    'type': 'object',
                    'properties': {
                        'en': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        },
                        'fr': {
                            'type': 'text',
                            'fields': {'raw': {'type': 'keyword'}}
                        }
                    }
                }
            }
        }
    }
}


SETTINGS = {
    'settings': {'number_of_shards': 1, 'number_of_replicas': 0},
    'mappings': {
        'properties': {
            'geometry': {'type': 'geo_shape'},
            'properties': CPW_PROPERTIES
        }
    }
}

MAX_XML_DATETIME_DIFF_SECONDS = 10


class CitypageweatherRealtimeLoader(BaseLoader):
    """Current conditions real-time loader"""

    def __init__(self, conn_config={}):
        """initializer"""

        BaseLoader.__init__(self)

        self.conn = ElasticsearchConnector(conn_config)
        self.filename_pattern = (
            '{datetime}_MSC_CitypageWeather_{sitecode}_{lang}.xml'  # noqa
        )
        self.conn.create(INDEX_NAME, mapping=SETTINGS)
        self.xml_root = None
        self.lang = None
        self.filepath_en = None
        self.filepath_fr = None
        self.parsed_filename = None
        self.wxo_lookup = None
        self.sitecode = None
        self.citycode = None
        self.cpw_feature = {
            'type': "Feature",
            'properties': {
                'lastUpdated': datetime.now().strftime(DATETIME_RFC3339_FMT)
            }
        }

    def load_data(self, filepath: str) -> bool:
        """
        fonction from base to load the data in ES

        :param filepath: filepath for parsing the CPW file

        :returns: True/False
        """

        LOGGER.debug(f'Received {filepath} for loading...')
        # parse filename and extract current lang and alt lang
        current_filepath = Path(filepath)
        self.parsed_filename = parse(
            self.filename_pattern, current_filepath.name
        )
        current_lang = self.parsed_filename.named['lang']
        alt_lang = 'fr' if current_lang == 'en' else 'en'

        # set current file language filepath
        setattr(self, f'filepath_{current_lang}', current_filepath)

        # set alternate file language filepath
        alt_xml_wildcard = self.filename_pattern.format(
            datetime='*',
            sitecode=self.parsed_filename.named['sitecode'],
            lang=alt_lang
        )

        # glob all alternate language files and sort by
        # absolute datetime difference to current file
        associated_alt_files = sorted(
            current_filepath.parent.glob(alt_xml_wildcard),
            key=self._sort_by_datetime_diff
        )

        if associated_alt_files:
            # get file with datetime closest to current filepath
            setattr(
                self,
                f'filepath_{alt_lang}',
                associated_alt_files[0]
            )
        else:
            LOGGER.warning(
                f'No associated {alt_lang} Citypage XML files found for '
                f'{current_filepath}. Skipping file...'
            )
            return False

        LOGGER.debug(
            f'Processing XML: {self.filepath_en} and {self.filepath_fr}'
        )

        # load wxo_lookup
        with open(
            os.path.join(MSC_PYGEOAPI_BASEPATH, 'resources/wxo_lookup.json')
        ) as json_file:
            self.wxo_lookup = json.load(json_file)

        try:
            self.xml_roots = {
                'en': etree.parse(self.filepath_en).getroot(),
                'fr': etree.parse(self.filepath_fr).getroot()
            }
        except Exception as err:
            LOGGER.error(f'ERROR: cannot process data: {err}')
            return False

        xml_creation_dates = [
            datetime.strptime(
                self.xml_roots[key].find('dateTime/timeStamp').text,
                '%Y%m%d%H%M%S'
            )
            for key in self.xml_roots
        ]

        # calculate diff between the two nearest en/fr XML creation dates
        xml_creation_diff_seconds = abs(
            (xml_creation_dates[0] - xml_creation_dates[1]).total_seconds()
        )
        if xml_creation_diff_seconds > MAX_XML_DATETIME_DIFF_SECONDS:
            LOGGER.warning(
                'File creation times differ by more than '
                f'{MAX_XML_DATETIME_DIFF_SECONDS} seconds. '
                'Skipping loading...'
            )
            return False
        else:
            LOGGER.debug(
                f'File creation times differ by {xml_creation_diff_seconds} '
                'seconds. Proceeding...'
            )

        self.sitecode = self.parsed_filename.named['sitecode']
        try:
            self.citycode = self.wxo_lookup[self.sitecode]['citycode']
        except KeyError:
            LOGGER.error(
                f'ERROR: cannot find sitecode {self.sitecode} key in WxO '
                'lookup table.'
            )
            return False

        data = self.xml2json_cpw()

        if data:
            try:
                r = self.conn.Elasticsearch.update(
                    index=INDEX_NAME,
                    id=data['properties']['identifier'],
                    doc_as_upsert=True,
                    doc=data
                )
                LOGGER.debug(f'Result: {r}')
                return True
            except Exception as err:
                LOGGER.warning(f'Error indexing: {err}')
                return False
        else:
            LOGGER.warning(
                'No data found in XML files. Skipping indexing...'
            )
            return False

    def _sort_by_datetime_diff(self, file):
        """
        Sort files by absolute datetime difference between filename and
        parsed datetime in active file

        :param file: `Path` object
        :returns: `timedelta` object
        """

        return abs(
            datetime.strptime(
                self.parsed_filename.named['datetime'], '%Y%m%dT%H%M%S.%fZ'
            )
            - datetime.strptime(
                parse(self.filename_pattern, file.name).named['datetime'],
                '%Y%m%dT%H%M%S.%fZ'
            )
        )

    def _node_to_dict(self, node, lang=None):
        """
        Convert an lxml.etree.Element to a dict

        :param node: `lxml.etree.Element` node

        :returns: `dict` representation of xml node
        """

        if node is not None:
            # if node has no attributes, just return the text
            if not node.attrib and node.text:
                if lang:
                    return {lang: safe_cast_to_number(node.text)}
                else:
                    return safe_cast_to_number(node.text)
            else:
                node_dict = {}
                for attrib in node.attrib:
                    if node.attrib[attrib]:
                        # in some case node attributes contain datetime strings
                        # formatted as YYYYMMDDHHMMSS, in this case we
                        # want to convert them to RFC3339
                        regex = r"^(?:[2][0-9]{3})(?:(?:0[1-9]|1[0-2]))(?:(?:0[1-9]|[12]\d|3[01]))(?:(?:[01]\d|2[0-3]))(?:[0-5]\d){2}$"  # noqa
                        if re.match(regex, node.attrib[attrib]):
                            dt = datetime.strptime(
                                node.attrib[attrib], '%Y%m%d%H%M%S'
                            )
                            if lang:
                                node_dict[attrib] = {
                                    lang: dt.strftime(DATETIME_RFC3339_FMT)
                                }
                            else:
                                node_dict[attrib] = dt.strftime(
                                    DATETIME_RFC3339_FMT
                                )
                        elif lang:
                            node_dict[attrib] = {
                                lang: safe_cast_to_number(node.attrib[attrib])
                            }
                        else:
                            node_dict[attrib] = safe_cast_to_number(
                                node.attrib[attrib]
                            )

            if node.text and node.text.strip():
                if lang:
                    node_dict['value'] = {lang: safe_cast_to_number(node.text)}
                else:
                    node_dict['value'] = safe_cast_to_number(node.text)

            return node_dict

        return None

    def _deep_merge(self, d1, d2):
        """
        Deep merge two dictionaries
        :param d1: `dict` to merge into
        :param d2: `dict` to merge from

        :returns: `dict` of merged dictionaries
        """
        for key in d2:
            if key in d1:
                if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                    self._deep_merge(d1[key], d2[key])
                else:
                    d1[key] = d2[key]
            else:
                d1[key] = d2[key]
        return d1

    def _set_nested_value(self, d, keys, value):
        """
        Set nested value in dictionary, and merges dictionaries if they
        already exist at path
        :param d: `dict` to set value in
        :param keys: `list` of keys
        :param value: value to set

        :returns: `dict` of modified dictionary
        """
        for key in keys[:-1]:
            d = d.setdefault(key, {})

        if keys[-1] in d:
            # try to merge dictionaries
            if isinstance(value, dict):
                for k, v in value.items():
                    if k in d[keys[-1]]:
                        if isinstance(v, dict):
                            d[keys[-1]][k] = self._deep_merge(
                                d[keys[-1]][k], v
                            )
                        else:
                            d[keys[-1]][k] = v
                    else:
                        d[keys[-1]][k] = v
            else:
                d[keys[-1]] = value
        else:
            d[keys[-1]] = value

        return d

    def _get_utc_timestamp(self, node):
        """
        Get timestamp from node
        :param node: `lxml.etree.Element` node

        :returns: `dict` of timestamp
        """
        timestamp = node.find('timeStamp')
        if timestamp is not None:
            dt = datetime.strptime(timestamp.text, '%Y%m%d%H%M%S')
            return {self.lang: dt.strftime('%Y-%m-%dT%H:%M:%SZ')}
        return None

    def _set_cpw_location(self):
        """
        Set location and identifier information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        self.cpw_feature['properties']['identifier'] = self.citycode

        location = self.xml_root.find('location')
        if location is not None:
            self._set_nested_value(
                self.cpw_feature['properties'],
                ['name'],
                {self.lang: location.find('name').text}
            )

            self._set_nested_value(
                self.cpw_feature['properties'],
                ['region'],
                {self.lang: location.find('region').text}
            )

            lon = location.find('name').attrib.get('lon')
            lat = location.find('name').attrib.get('lat')

            lon, lon_dir = float(lon[:-1]), lon[-1]
            lat, lat_dir = float(lat[:-1]), lat[-1]

            if lon_dir in ['W', 'O']:
                lon *= -1  # west means negative longitude
            if lat_dir == 'S':
                lat *= -1  # south means negative latitude

            self.cpw_feature['geometry'] = {
                'type': 'Point',
                'coordinates': [lon, lat, 0.0]
            }

            if self.lang == 'en':
                self._set_nested_value(
                    self.cpw_feature['properties'],
                    ['url'],
                    {
                        self.lang: f'https://weather.gc.ca/en/location/index.html?coords={lat},{lon}'  # noqa
                    },
                )
            else:
                self._set_nested_value(
                    self.cpw_feature['properties'],
                    ['url'],
                    {
                        self.lang: f'https://meteo.gc.ca/fr/location/index.html?coords={lat},{lon}'  # noqa
                    },
                )

        return self.cpw_feature

    def _set_cpw_current_conditions(self):
        """
        Set current conditions information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        current_conditions = self.xml_root.find("currentConditions")

        current_conditions_dict = {}

        if current_conditions is not None and len(current_conditions):

            iconCode = current_conditions_dict['iconCode'] = (
                self._node_to_dict(current_conditions.find('iconCode'))
            )

            if iconCode and 'value' in iconCode:
                current_conditions_dict['iconCode'][
                    'url'
                ] = f'https://weather.gc.ca/weathericons/{current_conditions_dict["iconCode"]["value"]:02d}.gif'  # noqa

            for date in self.xml_root.findall(
                "currentConditions/dateTime"
                "[@zone='UTC'][@name='observation']"
            ):
                timestamp = self._get_utc_timestamp(date)
                if timestamp:
                    current_conditions_dict['timestamp'] = timestamp

            kv_mapping = {
                'relativeHumidity': 'relativeHumidity',
                'wind': [
                    'wind/speed',
                    'wind/gust',
                    'wind/direction',
                    'wind/bearing'
                ],
                'pressure': 'pressure',
                'temperature': 'temperature',
                'dewpoint': 'dewpoint',
                'windChill': 'windChill',
                'station': 'station',
                'condition': 'condition'
            }

            for key, value in kv_mapping.items():
                if isinstance(value, list):
                    _dict = {}
                    for val in value:
                        node = current_conditions.find(val)
                        if node is not None and node.text:
                            _dict[val.split('/')[-1]] = self._node_to_dict(
                                current_conditions.find(val), self.lang
                            )
                    if _dict:
                        current_conditions_dict[key] = _dict
                else:
                    node = current_conditions.find(value)
                    if node is not None and (node.attrib or node.text):
                        current_conditions_dict[key] = self._node_to_dict(
                            current_conditions.find(value), self.lang
                        )

        if self.cpw_feature.get('properties', {}).get('currentConditions', {}):
            existing_dict = self.cpw_feature['properties']['currentConditions']
            current_conditions_dict = self._deep_merge(
                existing_dict, current_conditions_dict
            )
        else:
            self.cpw_feature['properties'][
                'currentConditions'
            ] = current_conditions_dict

        return self.cpw_feature

    def _set_cpw_forecast_group_regional_normals(self):
        """
        Set regional normals information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        regional_normals = self.xml_root.find('forecastGroup/regionalNormals')
        if regional_normals is not None and len(regional_normals):
            regional_normals_dict = {}
            textSummary = regional_normals.find('textSummary')
            temperatures = regional_normals.findall('temperature')

            if textSummary.text:
                self._set_nested_value(
                    regional_normals_dict,
                    ['textSummary'],
                    {self.lang: textSummary.text}
                )

            regional_high_lows = [
                self._node_to_dict(temp, self.lang) for temp in temperatures
            ]

            # if properties.forecast_group.regionalNormals.temperature
            # exists, retrieve them and update them with the new values
            if (
                self.cpw_feature.get('properties', {})
                .get('forecastGroup', {})
                .get('regionalNormals', {})
                .get('temperature', {})
            ):
                for i, temp in enumerate(regional_high_lows):
                    existing_dict = self.cpw_feature['properties'][
                        'forecastGroup'
                    ]['regionalNormals']['temperature'][i]
                    regional_high_lows[i] = self._deep_merge(
                        existing_dict, temp
                    )

            if regional_high_lows:
                regional_normals_dict['temperature'] = regional_high_lows

            if (
                self.cpw_feature.get('properties', {})
                .get('forecastGroup', {})
                .get('regionalNormals', {})
            ):
                existing_dict = self.cpw_feature['properties'][
                    'forecastGroup'
                ]['regionalNormals']
                regional_normals_dict = self._deep_merge(
                    existing_dict, regional_normals_dict
                )
            else:
                self.cpw_feature['properties']['forecastGroup'][
                    'regionalNormals'
                ] = regional_normals_dict

        return self.cpw_feature

    def _set_forecast_general_info(self, forecast, forecast_dict):
        """
        Set general forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        period_dict = self._node_to_dict(forecast.find('period'))

        self._set_nested_value(
            forecast_dict,
            ['period', 'textForecastName'],
            {self.lang: period_dict.get('textForecastName')}
        )
        self._set_nested_value(
            forecast_dict,
            ['period', 'value'],
            {self.lang: period_dict.get('value')}
        )
        self._set_nested_value(
            forecast_dict,
            ['textSummary'],
            {self.lang: _get_element(forecast, 'textSummary')}
        )

        return forecast_dict

    def _set_forecast_cloud_precip(self, forecast_elem, forecast_dict):
        """
        Set cloud precipitation forecast information for
        the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        cloud_precip = forecast_elem.find('cloudPrecip')
        if cloud_precip is not None and len(cloud_precip):
            self._set_nested_value(
                forecast_dict,
                ['cloudPrecip'],
                {self.lang: cloud_precip.find('textSummary').text}
            )

        return forecast_dict

    def _set_forecast_abbreviated_forecast(self, forecast_elem, forecast_dict):
        """
        Set abbreviated forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        abbreviated_forecast = forecast_elem.find('abbreviatedForecast')
        if abbreviated_forecast is not None and len(abbreviated_forecast):
            self._set_nested_value(
                forecast_dict,
                ['abbreviatedForecast', 'textSummary'],
                {self.lang: abbreviated_forecast.find('textSummary').text}
            )

            self._set_nested_value(
                forecast_dict,
                ['abbreviatedForecast', 'icon'],
                self._node_to_dict(abbreviated_forecast.find('iconCode'))
            )

            self._set_nested_value(
                forecast_dict,
                ['abbreviatedForecast', 'icon', 'url'],
                f'https://weather.gc.ca/weathericons/{forecast_dict["abbreviatedForecast"]["icon"]["value"]:02d}.gif'  # noqa
            )

        return forecast_dict

    def _set_forecast_temperatures(self, forecast_elem, forecast_dict):
        """
        Set temperatures forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        temperatures = forecast_elem.find('temperatures')
        if temperatures is not None and len(temperatures):
            self._set_nested_value(
                forecast_dict,
                ['temperatures', 'textSummary'],
                {self.lang: temperatures.find('textSummary').text}
            )

            temps = []
            for i, temp in enumerate(temperatures.findall('temperature')):
                temp_dict = self._node_to_dict(temp, self.lang)
                # get existing forecast_dict['temperatures']['temperature'][i]
                # if it exists
                if i < len(
                    forecast_dict['temperatures'].get('temperature', [])
                ):
                    existing_dict = forecast_dict['temperatures'][
                        'temperature'
                    ][i]
                    for key in existing_dict.keys():
                        if key in temp_dict:
                            existing_dict[key] = {
                                **existing_dict[key],
                                **temp_dict[key]
                            }
                else:
                    temps.append(temp_dict)
            if temps:
                self._set_nested_value(
                    forecast_dict, ['temperatures', 'temperature'], temps
                )

        return forecast_dict

    def _set_forecast_winds(self, forecast_elem, forecast_dict):
        """
        Set winds forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        winds = forecast_elem.find('winds')
        if winds is not None and len(winds):
            if winds.find('textSummary') is not None:
                self._set_nested_value(
                    forecast_dict,
                    ['winds', 'textSummary'],
                    {self.lang: _get_element(winds, 'textSummary')}
                )

            periods = []
            for i, period in enumerate(winds.findall('wind')):
                wind_period_dict = {}
                attrs = ['index', 'rank']
                for attr in attrs:
                    wind_period_dict[attr] = {
                        self.lang: safe_cast_to_number(
                            period.attrib.get(attr)
                        )
                    }

                nodes = ['speed', 'gust', 'direction', 'bearing']
                for node in nodes:
                    wind_period_dict[node] = self._node_to_dict(
                        period.find(node), self.lang
                    )

                if i < len(forecast_dict.get('winds', {}).get('periods', [])):
                    existing_dict = forecast_dict['winds']['periods'][i]
                    forecast_dict['winds']['periods'][i] = self._deep_merge(
                        existing_dict, wind_period_dict
                    )
                else:
                    periods.append(wind_period_dict)

            if periods:
                self._set_nested_value(
                    forecast_dict, ['winds', 'periods'], periods
                )

        return forecast_dict

    def _set_forecast_precipitation(self, forecast_elem, forecast_dict):
        """
        Set precipitation forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        precipitation = forecast_elem.find('precipitation')
        if precipitation is not None and len(precipitation):

            # set textSummary if it exists
            if precipitation.find('textSummary').text:
                self._set_nested_value(
                    forecast_dict,
                    ['precipitation', 'textSummary'],
                    {self.lang: _get_element(precipitation, 'textSummary')}
                )

            # get precipitation periods
            precip_periods = []
            for i, precip_type in enumerate(
                precipitation.findall('precipType')
            ):
                if precip_type.attrib.get('start') and precip_type.attrib.get(
                    'end'
                ):
                    precip_type_dict = self._node_to_dict(
                        precip_type, self.lang
                    )
                    if i < len(
                        forecast_dict.get('precipitation', {}).get(
                            'precipPeriods', []
                        )
                    ):
                        existing_dict = forecast_dict['precipitation'][
                            'precipPeriods'
                        ][i]
                        forecast_dict['precipitation']['precipPeriods'][i] = (
                            self._deep_merge(existing_dict, precip_type_dict)
                        )
                    else:
                        precip_periods.append(precip_type_dict)

            if precip_periods:
                self._set_nested_value(
                    forecast_dict,
                    ['precipitation', 'precipPeriods'],
                    precip_periods
                )

            # get accumulation
            accumulation = precipitation.find('accumulation')
            if accumulation is not None and len(accumulation):
                name = accumulation.find('name').text
                amount = accumulation.find('amount')

                if name or amount:
                    accumulation_dict = {
                        'name': {self.lang: name},
                        'amount': self._node_to_dict(amount, self.lang)
                    }
                    if 'accumulation' in forecast_dict['precipitation']:
                        existing_accumulation_dict = forecast_dict[
                            'precipitation'
                        ]['accumulation']
                        forecast_dict['precipitation']['accumulation'] = (
                            self._deep_merge(
                                existing_accumulation_dict, accumulation_dict
                            )
                        )

                    if 'accumulation' not in forecast_dict['precipitation']:
                        forecast_dict['precipitation']['accumulation'] = {
                            'name': {self.lang: name},
                            'amount': self._node_to_dict(amount, self.lang)
                        }

        return forecast_dict

    def _set_forecast_windchill(self, forecast_elem, forecast_dict):
        """
        Set windchill forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        windchill = forecast_elem.find('windChill')
        if windchill is not None and len(windchill):
            if windchill.find('textSummary').text:
                self._set_nested_value(
                    forecast_dict,
                    ['windChill', 'textSummary'],
                    {self.lang: windchill.find('textSummary').text}
                )

            if windchill.find('calculated').text:
                self._set_nested_value(
                    forecast_dict,
                    ['windChill', 'calculated'],
                    {self.lang: windchill.find('calculated').text}
                )

            if windchill.find('frostbite').text:
                self._set_nested_value(
                    forecast_dict,
                    ['windChill', 'frostbite'],
                    {self.lang: windchill.find('frostbite').text}
                )

        return forecast_dict

    def _set_forecast_uv(self, forecast, forecast_dict):
        """
        Set uv forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        uv = forecast.find('uv')
        if uv is not None and len(uv):
            uv_dict = {}
            if uv.attrib.get('category'):
                uv_dict['category'] = {self.lang: uv.attrib.get('category')}

            if (
                uv.find('textSummary') is not None
                and uv.find('textSummary').text
            ):
                uv_dict['textSummary'] = {
                    self.lang: uv.find('textSummary').text
                }

            if uv.find('index').text:
                uv_dict['index'] = {self.lang: uv.find('index').text}

            if 'uv' in forecast_dict:
                existing_dict = forecast_dict['uv']
                forecast_dict['uv'] = self._deep_merge(existing_dict, uv_dict)
            else:
                forecast_dict['uv'] = uv_dict

        return forecast_dict

    def _set_forecast_rel_hum(self, forecast_elem, forecast_dict):
        """
        Set relative humidity forecast information for
        the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        rel_hum = forecast_elem.find('relativeHumidity')
        if rel_hum is not None:
            self._set_nested_value(
                forecast_dict,
                ['relativeHumidity'],
                self._node_to_dict(rel_hum, self.lang)
            )

        return forecast_dict

    def _set_forecast_humidex(self, forecast_elem, forecast_dict):
        """
        Set humidex forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        humidex = forecast_elem.find('humidex')
        if humidex is not None and len(humidex):
            if humidex.find('textSummary').text:
                self._set_nested_value(
                    forecast_dict,
                    ['humidex', 'textSummary'],
                    {self.lang: humidex.find('textSummary').text}
                )

            if humidex.find('calculated').text:
                self._set_nested_value(
                    forecast_dict,
                    ['humidex', 'calculated'],
                    {self.lang: humidex.find('calculated').text}
                )

        return forecast_dict

    def _set_forecast_visibility(self, forecast_elem, forecast_dict):
        """
        Set visibility forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        visibility = forecast_elem.find('visibility')
        if visibility is not None and len(visibility):
            visibility_dict = {}
            for child in visibility:
                if 'Visib' in child.tag and child.text:
                    visibility_dict['textSummary'] = {
                        self.lang: child.find('textSummary').text
                    }
                    visibility_dict['cause'] = child.attrib.get('cause')

            if visibility_dict:
                self._set_nested_value(
                    forecast_dict, ['visibility'], visibility_dict
                )

        return forecast_dict

    def _set_forecast_snowlevel(self, forecast_elem, forecast_dict):
        """
        Set snowlevel forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        snow_level = forecast_elem.find('snowLevel')
        if snow_level is not None and len(snow_level):
            snow_level_dict = {}
            if snow_level.find('textSummary').text:
                snow_level_dict['textSummary'] = {
                    self.lang: snow_level.find('textSummary').text
                }

            if snow_level_dict:
                self._set_nested_value(
                    forecast_dict, ['snowLevel'], snow_level_dict
                )

        return forecast_dict

    def _set_forecast_frost(self, forecast_elem, forecast_dict):
        """
        Set frost forecast information for the citypageweather object

        :param forecast: `xml.etree.Element` of forecast
        :param forecast_dict: `dict` of forecast information

        :returns: `dict` of modified citypageweather object
        """

        frost = forecast_elem.find('frost')
        if frost is not None and len(frost):
            frost_dict = {}
            if frost.find('textSummary').text:
                frost_dict['textSummary'] = {
                    self.lang: frost.find('textSummary').text
                }

            if frost_dict:
                self._set_nested_value(forecast_dict, ['frost'], frost_dict)

        return forecast_dict

    def _set_cpw_forecast_group(self):
        """
        Set forecast group information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        forecast_group = self.xml_root.find("forecastGroup")

        if not 'forecastGroup' in self.cpw_feature['properties']:  # noqa
            self.cpw_feature['properties']['forecastGroup'] = {}

        if forecast_group is not None and len(forecast_group):

            for date in forecast_group.findall(
                "dateTime" "[@zone='UTC'][@name='forecastIssue']"
            ):
                timestamp = self._get_utc_timestamp(date)
                if timestamp is not None:
                    self._set_nested_value(
                        self.cpw_feature['properties']['forecastGroup'],
                        ['timestamp'],
                        timestamp
                    )

            self._set_cpw_forecast_group_regional_normals()

            # iterate over forecasts and populate
            forecasts = forecast_group.findall("forecast")

            if (
                forecasts is not None
                and len(forecasts)
                and 'forecasts'
                not in self.cpw_feature['properties']['forecastGroup']
            ):
                self.cpw_feature['properties']['forecastGroup'][
                    'forecasts'
                ] = []

            for i, forecast_elem in enumerate(forecasts):
                # if index exists in
                # self.cpw_feature['properties']['forecastGroup']['forecasts']
                # use it, otherwise create a new dict
                if i < len(
                    self.cpw_feature['properties']['forecastGroup'][
                        'forecasts'
                    ]
                ):
                    forecast_dict = self.cpw_feature['properties'][
                        'forecastGroup'
                    ]['forecasts'][i]
                else:
                    forecast_dict = {}

                # get list of all self._get_forecast_* functions and call each
                # function to populate each forecast object
                set_forecast_funcs = [
                    getattr(self, f)
                    for f in dir(self)
                    if f.startswith('_set_forecast_')
                ]

                for forecast_func in set_forecast_funcs:
                    forecast_dict = forecast_func(forecast_elem, forecast_dict)

                try:
                    self.cpw_feature['properties']['forecastGroup'][
                        'forecasts'
                    ][i] = forecast_dict
                except IndexError:
                    self.cpw_feature['properties']['forecastGroup'][
                        'forecasts'
                    ].insert(i, forecast_dict)

        if not self.cpw_feature['properties'].get('forecast_group'):
            self.cpw_feature['properties'].pop('forecast_group', None)

        return self.cpw_feature

    def _set_hourly_forecast_datetime(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast datetime information for the hourly forecast object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        dt = datetime.strptime(
            hourly_forecast_elem.attrib.get('dateTimeUTC'), '%Y%m%d%H%M%S'
        )
        if dt is not None:
            self._set_nested_value(
                hourly_forecast_dict,
                ['timestamp'],
                dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_condition(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast condition information for the hourly forecast
        object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        condition = hourly_forecast_elem.find('condition')
        if condition is not None:
            self._set_nested_value(
                hourly_forecast_dict,
                ['condition'],
                {self.lang: condition.text}
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_icon_code(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast icon code information for the citypageweather
        object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of hourly forecast object
        """

        icon_code = hourly_forecast_elem.find('iconCode')
        if icon_code is not None:
            hourly_forecast_dict['iconCode'] = self._node_to_dict(icon_code)

            if 'iconCode' in hourly_forecast_dict:
                hourly_forecast_dict['iconCode'][
                    'url'
                ] = f'https://weather.gc.ca/weathericons/{hourly_forecast_dict["iconCode"]["value"]:02d}.gif'  # noqa

        return hourly_forecast_dict

    def _set_hourly_forecast_temperature(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast temperature information for the hourly forecast
        object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        temperature = hourly_forecast_elem.find('temperature')
        if temperature is not None:
            self._set_nested_value(
                hourly_forecast_dict,
                ['temperature'],
                self._node_to_dict(temperature, self.lang)
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_lop(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast lop information for the hourly forecast object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        lop = hourly_forecast_elem.find('lop')
        if lop is not None and lop.text:
            self._set_nested_value(
                hourly_forecast_dict,
                ['lop'],
                self._node_to_dict(lop, self.lang)
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_humidex(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast humidex information for the hourly forecast object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        humidex = hourly_forecast_elem.find('humidex')
        if humidex is not None and humidex.text:
            self._set_nested_value(
                hourly_forecast_dict,
                ['humidex'],
                self._node_to_dict(humidex, self.lang)
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_windchill(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast windchill information for the hourly forecast
        object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        windchill = hourly_forecast_elem.find('windChill')
        if windchill is not None and windchill.text:
            self._set_nested_value(
                hourly_forecast_dict,
                ['windChill'],
                self._node_to_dict(windchill, self.lang)
            )

        return hourly_forecast_dict

    def _set_hourly_forecast_uv(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast uv information for the hourly forecast object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        uv = hourly_forecast_elem.find('uv')
        if uv is not None:
            index = uv.find('index')
            if index is not None and index.text:
                self._set_nested_value(
                    hourly_forecast_dict,
                    ['uv', 'index', 'value'],
                    self._node_to_dict(index, self.lang)
                )

        return hourly_forecast_dict

    def _set_hourly_forecast_wind(
        self, hourly_forecast_elem, hourly_forecast_dict
    ):
        """
        Set hourly forecast wind information for the hourly forecast object

        :param hourly_forecast_elem: `xml.etree.Element` of hourly forecast
        :param hourly_forecast_dict: `dict` of hourly forecast information

        :returns: `dict` of modified hourly forecast object
        """

        wind = hourly_forecast_elem.find('wind')
        if wind is not None:
            wind_dict = {}
            nodes = ['speed', 'direction', 'gust']
            for node in nodes:
                node_elem = wind.find(node)
                if node_elem is not None and node_elem.text:
                    wind_dict[node] = self._node_to_dict(node_elem, self.lang)

        self._set_nested_value(hourly_forecast_dict, ['wind'], wind_dict)

        return hourly_forecast_dict

    def _set_cpw_hourly_forecast_group(self):
        """
        Set hourly forecast group information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        hourly_forecast_group = self.xml_root.find("hourlyForecastGroup")

        if 'hourlyForecastGroup' not in self.cpw_feature['properties']:
            self.cpw_feature['properties']['hourlyForecastGroup'] = {}

        if hourly_forecast_group is not None and len(hourly_forecast_group):

            for date in hourly_forecast_group.findall(
                "dateTime" "[@zone='UTC'][@name='forecastIssue']"
            ):
                timestamp = self._get_utc_timestamp(date)
                if timestamp is not None:
                    self._set_nested_value(
                        self.cpw_feature['properties']['hourlyForecastGroup'],
                        ['timestamp'],
                        timestamp
                    )

            # iterate over hourly forecasts and populate
            hourly_forecasts = hourly_forecast_group.findall("hourlyForecast")
            if (
                hourly_forecasts is not None
                and len(hourly_forecasts)
                and 'hourlyForecasts'
                not in self.cpw_feature['properties']['hourlyForecastGroup']
            ):
                self.cpw_feature['properties']['hourlyForecastGroup'][
                    'hourlyForecasts'
                ] = []

            for i, hourly_forecast_elem in enumerate(hourly_forecasts):
                # if index exists in
                # self.cpw_feature['properties']['hourlyForecastGroup']['hourlyForecasts']
                # use it, otherwise create a new dict
                if i < len(
                    self.cpw_feature['properties']['hourlyForecastGroup'][
                        'hourlyForecasts'
                    ]
                ):
                    hourly_forecast_dict = self.cpw_feature['properties'][
                        'hourlyForecastGroup'
                    ]['hourlyForecasts'][i]
                else:
                    hourly_forecast_dict = {}

                # get list of all self._set_forecast_* functions and call each
                # function to populate each forecast object
                set_forecast_funcs = [
                    getattr(self, f)
                    for f in dir(self)
                    if f.startswith('_set_hourly_forecast_')
                ]

                for forecast_func in set_forecast_funcs:
                    hourly_forecast_dict = forecast_func(
                        hourly_forecast_elem, hourly_forecast_dict
                    )

                try:
                    self.cpw_feature['properties']['hourlyForecastGroup'][
                        'hourlyForecasts'
                    ][i] = hourly_forecast_dict
                except IndexError:
                    self.cpw_feature['properties']['hourlyForecastGroup'][
                        'hourlyForecasts'
                    ].insert(i, hourly_forecast_dict)

        return self.cpw_feature

    def _set_cpw_warnings(self):
        """
        Set warnings information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        warnings = self.xml_root.find("warnings")

        if warnings is not None and len(warnings):
            events = []
        warnings = self.xml_root.find("warnings")

        if (
            warnings is not None
            and len(warnings)
            and 'warnings' not in self.cpw_feature['properties']
        ):
            self.cpw_feature['properties']['warnings'] = []

        events = []
        if warnings is not None and len(warnings):
            for i, event in enumerate(warnings):
                if i < len(self.cpw_feature['properties']['warnings']):
                    event_dict = self.cpw_feature['properties']['warnings'][i]
                else:
                    event_dict = {}

                event_dict = self._deep_merge(
                    event_dict, self._node_to_dict(event, self.lang)
                )

                event_issue = event.find("dateTime[@name='eventIssue']")
                if event_issue is not None:
                    self._set_nested_value(
                        event_dict,
                        ['eventIssue'],
                        self._get_utc_timestamp(event_issue)
                    )

                events.append(event_dict)

        self.cpw_feature['properties']['warnings'] = events

        return self.cpw_feature

    def _set_cpw_riseSet(self):
        """
        Set rise and set information for the citypageweather object

        :returns: `dict` of modified citypageweather object
        """

        rise_set = self.xml_root.find("riseSet")

        if rise_set is not None and len(rise_set):
            if 'riseSet' not in self.cpw_feature['properties']:
                rise_set_dict = {}
            else:
                rise_set_dict = self.cpw_feature['properties']['riseSet']

            # get disclaimer
            disclaimer = rise_set.find('disclaimer')
            if disclaimer is not None:
                self._set_nested_value(
                    rise_set_dict, ['disclaimer'], {self.lang: disclaimer.text}
                )

            # get sunrise and sunset UTC
            for node in rise_set.findall('dateTime[@zone="UTC"]'):
                if node.attrib.get('name') == 'sunrise':
                    self._set_nested_value(
                        rise_set_dict,
                        ['sunrise'],
                        self._get_utc_timestamp(node)
                    )
                if node.attrib.get('name') == 'sunset':
                    self._set_nested_value(
                        rise_set_dict,
                        ['sunset'],
                        self._get_utc_timestamp(node)
                    )

            if rise_set_dict:
                self.cpw_feature['properties']['riseSet'] = rise_set_dict

        return self.cpw_feature

    def xml2json_cpw(self):
        """
        main for generating weather data

        :param wxo_lookup: json file to have the city id
        :param xml: xml file to parse and convert to json

        :returns: xml as json object
        """

        for lang, xml_root in self.xml_roots.items():
            self.xml_root = xml_root
            self.lang = lang
            self._set_cpw_location()
            self._set_cpw_current_conditions()
            self._set_cpw_forecast_group()
            self._set_cpw_hourly_forecast_group()
            self._set_cpw_warnings()
            self._set_cpw_riseSet()

        return self.cpw_feature


@click.group()
def citypageweather():
    """Manages current conditions index"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_FILE()
@cli_options.OPTION_DIRECTORY()
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
def add(ctx, file_, directory, es, username, password, ignore_certs):
    """adds data to system"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    conn_config = configure_es_connection(es, username, password, ignore_certs)

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for file in Path(directory).rglob('*_MSC_CitypageWeather_*.xml'):
            files_to_process.append(file)
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        loader = CitypageweatherRealtimeLoader(conn_config)
        loader.load_data(file_to_process)

    click.echo('Done')


@click.command()
@click.pass_context
@cli_options.OPTION_ELASTICSEARCH()
@cli_options.OPTION_ES_USERNAME()
@cli_options.OPTION_ES_PASSWORD()
@cli_options.OPTION_ES_IGNORE_CERTS()
@cli_options.OPTION_YES(prompt='Are you sure you want to delete this index?')
def delete_index(ctx, es, username, password, ignore_certs):
    """Delete current conditions index"""

    conn_config = configure_es_connection(es, username, password, ignore_certs)
    conn = ElasticsearchConnector(conn_config)

    conn.delete(INDEX_NAME)


@click.command()
@click.pass_context
@cli_options.OPTION_HOURS(
    default=24,
    help='Delete cache older than n hours (default=24)',
)
@cli_options.OPTION_YES(
    prompt=f'Are you sure you want to clean the cache ({Path(MSC_PYGEOAPI_CACHEDIR) / "citypage_weather"})?'  # noqa
)
def clean_cache(ctx, hours):
    """Clean the cache"""

    cache_location = Path(MSC_PYGEOAPI_CACHEDIR) / "citypage_weather"
    if not cache_location.exists():
        click.echo('Cache is empty or does not exist.')
    else:
        cache_files = list(cache_location.rglob('*_MSC_CitypageWeather_*.xml'))
        click.echo(
            f"Removing {len(cache_files)} CityPage Weather XML files from cache..."  # noqa
        )
        for file in cache_files:
            if (
                datetime.now() - datetime.fromtimestamp(file.stat().st_mtime)
            ).seconds / 3600 > hours:
                file.unlink()
        click.echo('Done')


citypageweather.add_command(add)
citypageweather.add_command(clean_cache)
citypageweather.add_command(delete_index)
