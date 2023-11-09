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
curl "http://geomet-dev-22.cmc.ec.gc.ca:5089"
# or
curl "https://geomet-dev-22-nightly.cmc.ec.gc.ca/msc-pygeoapi"
```
