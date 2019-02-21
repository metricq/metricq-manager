FROM metricq-python:latest
LABEL maintainer="mario.bielert@tu-dresden.de"

ARG couchdb_url=http://127.0.0.1:5984
ENV couchdb_url=$couchdb_url

ARG couchdb_user=admin
ENV couchdb_user=$couchdb_user

ARG couchdb_pw=admin
ENV couchdb_pw=$couchdb_pw

ARG rpc_url=amqp://localhost:5672
ENV rpc_url=$rpc_url

ARG data_url=amqp://localhost:5672
ENV data_url=$data_url

USER metricq
COPY --chown=metricq:metricq . /home/metricq/manager

WORKDIR /home/metricq/manager
RUN . /home/metricq/venv/bin/activate && pip install .

CMD /home/metricq/venv/bin/metricq-manager /home/metricq/manager/config --config-path --couchdb-url $couchdb_url --couchdb-user $couchdb_user --couchdb-password $couchdb_pw $rpc_url $data_url
