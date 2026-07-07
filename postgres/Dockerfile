FROM timescale/timescaledb:latest-pg15

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           postgresql-15-postgis-3 \
           postgresql-15-postgis-3-scripts \
    && rm -rf /var/lib/apt/lists/*

USER postgres
