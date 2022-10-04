FROM metricq/metricq-python:v4.2 AS builder
LABEL maintainer="mario.bielert@tu-dresden.de"

USER root
USER root
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/* 

USER metricq
COPY --chown=metricq:metricq . /home/metricq/manager

WORKDIR /home/metricq/manager
RUN pip install --user .

FROM metricq/metricq-python:v4.2

USER metricq
COPY --from=BUILDER --chown=metricq:metricq /home/metricq/.local /home/metricq/.local

ENTRYPOINT [ "/home/metricq/.local/bin/metricq-manager" ]
