# MSC pygeoapi

MSC GeoMet WFS 3 server implementation using pygeoapi

# Installation

## Dependencies

- [Elasticsearch](https://elastic.co) (5 or above)
 - i.e. `sudo echo `deb https://artifacts.elastic.co/packages/5.x/apt stable main` > /etc/apt/sources.list.d/elastic.list`
- [pygeoapi](https://github.com/geopython/pygeoapi)


```bash
sudo apt-get install msc-pygeoapi
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
