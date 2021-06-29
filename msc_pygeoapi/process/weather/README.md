# Process Overview

The extract raster process clips a raster file by a geojson type. The process will extract the temperature, wind speed, and wind direction for all queried forecast hours within/along/at the queried geometry. It then returns the weather data as a time series.

The process returns observations of the weather data in differing formats based on geometry type.

## Inputs

The extract raster process requires 4 inputs
1. model - the model to query
2. forecast hours - the forecast hours to query
3. model run - the model run to query
4. input geojson - the geometry to clip the raster by

The first three inputs are used to search an Elasticsearch index to collect the appropriate raster files from the query. A temperature, wind speed, and wind direction file are collected for each for each forecast hour.

The fourth input is a geojson that contains the geometry type, the geomtery to clip by, and the coordinate reference system of the geometry.

Example inputs can be found here
https://gist.github.com/tom-cooney/8fd9cf47703e94f3db749bdeb97962fe

## Outputs

The extract raster process outputs 1 geojson file with the metadata of the clipped raster, the geometry of the input query, and either obersvations of summary statistics depending on the geometry type.

*For point queries the weather observations for each of temperature, wind speed, and wind direction at the point are returned for each forecast hour.

*For line queries the weather observations for each of temperature, wind speed, and wind direction along the line are returned for each forecast hour.

*For polygon queries the min observation, max observation, and mean for each of temperature, wind speed, and wind direction are returned for each forecast hour.

# Running Process on pygeoapi

First navigate to pygeoapi directory and run
```bash
python setup.py install
```

Second navigate to msc-pygeoapi directory and run
```bash
python setup.py install
```

Third source dev environement by running. If not done already correctly configre dev environement
```bash
. ./dev.env
```

Fourth build openapi document. Only needs to be done once
```bash
pygeoapi openapi generate -c $PYGEOAPI_CONFIG > $PYGEOAPI_OPENAPI
```

Fifth start pygeoapi
```bash
pygeoapi serve
```
Visit http://geomet-dev-03.cmc.ec.gc.ca:5000

Note: port 5000 is default port. Check `/deploy/default/msc-pygeoapi-config.yml` to double check port

# Making changes to program

After making changes to msc_pygeoapi/process/weather/extract_raster.py re-install msc-pygeoapi by navigating into the msc-pygeoapi directory and running
```bash
python setup.py install
```
Then re-start pygeoapi by running
```bash
pygeoapi serve
```

# Useful debugging commands

To see if a point is within the bounds of a raster
```bash
gdallocationinfo -wgs84 <path_to_raster> <x-coordinate> <y-coordinate>
```

# Rasterio version
The extract raster process currently only works with rasterio version 1.1.4. If running a newer version of rasterio downgrade using
```bash
pip install rasterio==1.1.4 --force-reinstall
```
