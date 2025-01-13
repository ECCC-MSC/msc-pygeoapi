# build Docker image
```bash
docker build -t eccc-msc/msc-pygeoapi:nightly .
```

# (recommended) build Docker image via Docker Compose
```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml build --no-cache
```

# startup container
```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.override.yml up -d
```

# test OGC API endpoint
```bash
curl "http://geomet-dev-31.edc-mtl.ec.gc.ca:5089"
# or
curl "https://geomet-dev-31-nightly.edc-mtl.ec.gc.ca/msc-pygeoapi"
```
