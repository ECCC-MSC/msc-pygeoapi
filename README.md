[![Build Status](https://github.com/ECCC-MSC/msc-pygeoapi/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/ECCC-MSC/msc-pygeoapi/actions)
[![Build Status](https://github.com/ECCC-MSC/msc-pygeoapi/workflows/flake8%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/ECCC-MSC/msc-pygeoapi/actions)

# msc_pygeoapi

## Overview

MSC GeoMet pygeoapi server configuration and utilities

## Installation

### Requirements
- Python 3
- [virtualenv](https://virtualenv.pypa.io/)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during msc-pygeoapi installation.

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

# clone codebase
git clone https://github.com/ECCC-MSC/msc-pygeoapi.git
cd msc-pygeoapi

# add GCWeb theme files
curl -L https://github.com/wet-boew/GCWeb/releases/download/v14.6.0/themes-dist-14.6.0-gcweb.1.zip -o ./themes-gcweb.zip 
unzip -o ./themes-gcweb.zip "*/GCWeb/*" -d theme/static
unzip -o ./themes-gcweb.zip "*/wet-boew/*" -d theme/static
mv ./theme/static/themes-dist-14.6.0-gcweb ./theme/static/themes-gcweb
rm -f ./themes-gcweb.zip

# compile translation files (generate locale/*/LC_MESSAGES/message.mo)
pybabel compile -d locale -l fr

# install codebase
python setup.py build
python setup.py install

# configure environment
cp msc-pygeoapi.env dev.env
vi dev.env # edit paths accordingly
. dev.env

# serve API
pygeoapi serve
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
http://localhost/features/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31&limit=100&offset=100

# HTML respsonses
# filter by active stations in Nunavut
http://localhost/features/collections/hydrometric-stations/items?STATUS_EN=Active&limit=5000&f=html&PROV_TERR_STATE_LOC=NU
```

## Running the loaders

```bash
pip install -r requirements-oracle.txt

# view all data loaders available
msc-pygeoapi data

# examples for some loaders
msc-pygeoapi data hydat <rest of flags/parameters>
msc-pygeoapi data climate-archive <rest of flags/parameters>
msc-pygeoapi data ahccd_cmip5 <rest of flags/parameters>
msc-pygeoapi data marine-weather add -d <path_to_directory of XML files>

# bulletins - delete index
msc-pygeoapi data bulletins_realtime delete-index  # use --yes flag to bypass prompt

# realtime - standard workflow
msc-pygeoapi data hydrometric-realtime cache-stations  # download stations list to $MSC_PYGEOAPI_CACHEDIR location

sr_subscribe start deploy/default/sarracenia/hydrometric_realtime.conf  # begin realtime update process

msc-pygeoapi data hydrometric-realtime clean-indexes --days 30  # use --yes flag to bypass prompt (usually in crontab)
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

# API tests run against http://localhost:5000
# use --url to override

# run all tests
pytest

# run one test file
pytest test/test_hydat.py

# override endpoint
pytest test/test_hydat.py --url https://example.org/dev

# skip API tests (run only unit tests)
pytest -k 'not api'
```

### Multilingual Updates

```bash
# Extract from latest code the keys to be translated
pybabel extract -F babel-mapping.ini -o locale/messages.pot ./

# Update the existing .po language file with new/updated keys:
pybabel update -d locale -l fr -i locale/messages.pot

# Open the relevant .po file and contribute your translations
vi locale/fr/LC_MESSAGES/messages.po

# Then compile a .mo file to be used by the application
pybabel compile -d locale -l fr
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
