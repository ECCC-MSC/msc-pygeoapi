# build Docker image
```sh
docker build -t eccc-msc/msc-pygeoapi:nightly .
```

# (recommended) build Docker image via docker compose
```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml build --no-cache
```

# startup container
```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml up -d
```

# test OGC API endpoint
```sh
curl "http://geomet-dev-11.cmc.ec.gc.ca:5089"
# Or
curl "https://geomet-dev-11-nightly.cmc.ec.gc.ca/msc-pygeoapi"
```
