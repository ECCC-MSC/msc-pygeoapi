# msc_pygeoapi

## Overview

MSC GeoMet pygeoapi server configuration and utilities

## Installation

### Requirements
- Python 3.  Works with Python 2.7
- [virtualenv](https://virtualenv.pypa.io/)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during msc_pygeoapi installation.

Dependencies of note:
- [Elasticsearch](https://elastic.co) (5 or above)
 - i.e. `sudo echo `deb https://artifacts.elastic.co/packages/5.x/apt stable main` > /etc/apt/sources.list.d/elastic.list`
 - [pygeoapi](https://github.com/geopython/pygeoapi)

### Installing msc-pygeoapi
```bash
# setup virtualenv
python3 -m venv --system-site-packages msc-pygeoapi
cd msc-pygeoapi
source bin/activate

# clone codebase and install
git clone https://github.com/ECCC-MSC/msc-pygeoapi.git
cd msc-pygeoapi
python setup.py build
python setup.py install
```

## Running

```bash
msc-pygeoapi --version
```

Server will be located at http://localhost/features

# Sample Queries

## Hydrometric features (Water Level and Flow)

```bash

# all collections
http://localhost/features/collections

# hydrometric daily mean
http://localhost/features/collections/hydrometric-daily-mean

# filter by time
http://localhost/features/collections/hydrometric-daily-mean/items?time=2011-11-11/2012-11-11

# filter by bbox
http://localhost/features/collections/hydrometric-daily-mean/items?bbox=-80,45,-50,55

# filter by station number
http://localhost/features/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066

# filter by bbox
http://localhost/features/collections/hydrometric-daily-mean/items?bbox=-80,40,-50,54

# filter by station number and time
http://localhost/features/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31

# filter by station number and time, limit results
http://localhost/features/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31&limit=100

# filter by station number and time, limit and page results
http://localhost/features/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31&limit=100&startindex=100

# HTML respsonses
# filter by active stations in Nunavut
http://localhost/features/collections/hydrometric-stations/items?STATUS_EN=Active&limit=5000&f=html&PROV_TERR_STATE_LOC=NU
```

## Running the loaders

```bash
pip install -r requirements-oracle.txt

msc-pygeoapi load hydat <rest of flags/parameters>
msc-pygeoapi load climate-archive <rest of flags/parameters>
msc-pygeoapi load ahccd_cmip5 <rest of flags/parameters>

# bulletins - delete index
msc-pygeoapi load bulletins delete-index  # use --yes flag to bypass prompt

# realtime - standard workflow
msc-pygeoapi load hydrometric-realtime cache-stations  # download stations list to $MSC_PYGEOAPI_CACHEDIR location

sr_subscribe start deploy/default/sarracenia/hydrometric_realtime.conf  # begin realtime update process

msc-pygeoapi load hydrometric-realtime clean-records --days 30  # use --yes flag to bypass prompt (usually in crontab)
```

## Running processes
```bash

# run the CCCS Raster drill process (returns GeoJSON by default)
msc-pygeoapi process cccs execute raster-drill --y=45 --x=-75 --layer=CMIP5.SFCWIND.HISTO.WINTER.ABS_PCTL95

# run the CCCS Raster drill process returning CSV
msc-pygeoapi process cccs execute raster-drill --y=45 --x=-75 --layer=CMIP5.SFCWIND.HISTO.WINTER.ABS_PCTL95 --format=CSV
```

## Development

### Running Tests

```bash
# install dev requirements
pip install -r requirements-dev.txt

# run tests like this:
python msc_pygeoapi/tests/run_tests.py

# or this:
python setup.py test

# measure code coverage
coverage run --source=msc_pygeoapi -m unittest msc_pygeoapi.tests.run_tests
coverage report -m
```

## Releasing

```bash
python setup.py sdist bdist_wheel --universal
twine upload dist/*
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/ECCC-MSC/msc-pygeoapi/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
