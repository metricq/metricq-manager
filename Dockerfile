FROM metricq/metricq-python:latest AS builder
LABEL maintainer="mario.bielert@tu-dresden.de"

USER root
RUN apt-get update && apt-get install -y git wget

USER metricq
COPY --chown=metricq:metricq . /home/metricq/manager

WORKDIR /home/metricq/manager
RUN . /home/metricq/venv/bin/activate && pip install .
RUN wget -O wait-for-it.sh https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh && chmod +x wait-for-it.sh

FROM metricq/metricq-python:latest

USER metricq
COPY --from=builder /home/metricq/venv /home/metricq/venv
COPY --from=builder /home/metricq/manager/wait-for-it.sh /home/metricq/wait-for-it.sh

ARG wait_for_couchdb_url=127.0.0.1:5984
ENV wait_for_couchdb_url=$wait_for_couchdb_url

ARG couchdb_url=http://127.0.0.1:5984
ENV couchdb_url=$couchdb_url

ARG couchdb_user=admin
ENV couchdb_user=$couchdb_user

ARG couchdb_pw=admin
ENV couchdb_pw=$couchdb_pw

ARG wait_for_rabbitmq_url=127.0.0.1:5672
ENV wait_for_rabbitmq_url=$wait_for_rabbitmq_url

ARG rpc_url=amqp://localhost:5672
ENV rpc_url=$rpc_url

ARG data_url=amqp://localhost:5672
ENV data_url=$data_url

VOLUME ["/home/metricq/manager/config"]

CMD /home/metricq/wait-for-it.sh $wait_for_couchdb_url -- \
    /home/metricq/wait-for-it.sh $wait_for_rabbitmq_url -- \
    /home/metricq/venv/bin/metricq-manager \
        --config-path /home/metricq/manager/config \
        --couchdb-url $couchdb_url \
        --couchdb-user $couchdb_user \
        --couchdb-password $couchdb_pw \
        $rpc_url \
        $data_url
