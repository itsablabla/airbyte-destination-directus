FROM python:3.11-slim

RUN mkdir -p /airbyte/integration_code /airbyte/config \
 && useradd --create-home --shell /bin/bash airbyte \
 && chown -R airbyte:airbyte /airbyte

WORKDIR /airbyte/integration_code

COPY main.py ./
COPY destination_directus ./destination_directus

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

USER airbyte
LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/destination-directus
