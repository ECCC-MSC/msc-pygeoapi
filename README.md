# MSC pygeoapi

MSC GeoMet WFS 3 server implementation using pygeoapi

# Installation

```bash
sudo apt-get install msc-pygeoapi
```

Server should be located at http://localhost/msc-data-api

# Sample Queries

## Hydrometric features (Water Level Flow)

```bash

# all collections
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections

# hydrometric daily mean
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean

# filter by time
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?time=2011-11-11/2012-11-11

# filter by bbox
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?bbox=-80,45,-50,55

# filter by station number
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066

# filter by station number and time
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31

# filter by station number and time, limit results
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31&limit=100

# filter by station number and time, limit and page results
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-daily-mean/items?STATION_NUMBER=02RH066&time=2011-01-01/2012-12-31&limit=100&startindex=100

# HTML respsonses
# filter by active stations in Nunavut
http://geomet-dev-02.cmc.ec.gc.ca:9080/pygeoapi/collections/hydrometric-stations/items?STATUS_EN=Active&limit=5000&f=html&PROV_TERR_STATE_LOC=NU
```
