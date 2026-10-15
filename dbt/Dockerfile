FROM python:3.11-slim

WORKDIR /usr/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres==1.9.1

ENTRYPOINT ["dbt"]