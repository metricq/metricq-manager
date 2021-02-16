## Usage

Very trivial manager for testing purpose ONLY!

```
cd configurations
../manager.py
```

## Database queue maintainance and migration

`./tools/db-predeclare.py` contains a script to declare queues of a database
after their configuration changed without needing to restart the database.

## Deployment with Docker

### Prerequisites
This needs the metricq-python docker image present on the deployment system
- run `make docker` in MetricQ main repository

### Build an image of the manager itself

The configuration of the is passed as build arguments:

```
docker build -t metricq-manager --build-arg couchdb_url="http://localhost:5984" --build-arg couchdb_user=admin --build-arg couchdb_pw=admin --build-arg rpc_url=amqp://localhost --build-arg data_url=amqp://localhost .
```

If the manager should use the config directory, it can be bound to something outside with the following additional argument in the build command. Note that the effective user id of the manager is 1000.

```
-v /path/on/host:/home/metricq/manager/config
```

### Create a new container from the image

```
docker run -it --restart=unless-stopped metricq-manager
```

> For restart policies see: https://docs.docker.com/engine/reference/commandline/run/#restart-policies---restart
