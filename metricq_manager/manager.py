# metricq
# Copyright (C) 2018 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# metricq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import logging
import time
from itertools import islice
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union

import aio_pika
import aiomonitor
import click
import click_completion
import click_log
from aiocouch import CouchDB, NotFoundError
from metricq import Agent, rpc_handler
from metricq.logging import get_logger
from yarl import URL

from .queue_manager import DataQueueName, HreqQueueName, QueueManager

Metric = str
MetricList = Union[List[Metric], Dict[Metric, Dict[str, Any]]]


class MetricInputAlias(TypedDict):
    input: Optional[Metric]
    name: Metric


logger = get_logger()

click_log.basic_config(logger)
logger.setLevel("INFO")
# Use this if we ever use threads
# logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s %(threadName)-16s %(levelname)-8s %(message)s')
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)

click_completion.init()


class Manager(Agent):
    def __init__(
        self,
        management_url,
        data_url,
        config_path,
        queue_ttl,
        couchdb_url,
        couchdb_user,
        couchdb_password,
    ):
        super().__init__("manager", management_url)

        self.management_queue_name = "management"
        self.management_queue = None

        self.queue_manager: Optional[QueueManager] = None

        # This is very similar to Agent.derive_address, but we also set the data_server_address for clients
        vhost_prefix = "vhost:"
        if data_url.startswith(vhost_prefix):  # vhost only
            # for the manager itself
            self.data_url = str(
                URL(self._management_url).with_path(data_url[len(vhost_prefix) :])
            )
            # for clients
            self.data_server_address = data_url
        else:
            # for the manager itself
            self.data_url = data_url
            # for clients
            self.data_server_address = str(
                URL(data_url).with_password(None).with_user(None)
            )

        self.data_exchange_name = "metricq.data"
        self.history_exchange_name = "metricq.history"

        self.config_path = config_path
        self.queue_ttl = queue_ttl

        self.couchdb_client = CouchDB(
            couchdb_url,
            user=couchdb_user,
            password=couchdb_password,
            loop=self.event_loop,
        )
        self.couchdb_db_config = None
        self.couchdb_db_metadata = None

        # TODO if this proves to be reliable, remove the option
        self._subscription_autodelete = True
        # TODO Make some config stuff
        self._expires_seconds = 3600

    async def fetch_metadata(self, metric_ids):
        return {
            doc.id: doc.data
            async for doc in self.couchdb_db_metadata.docs(metric_ids, create=True)
        }

    async def connect_couchdb(self):
        self.couchdb_db_config = await self.couchdb_client.create(
            "config", exists_ok=True
        )
        self.couchdb_db_metadata = await self.couchdb_client.create(
            "metadata", exists_ok=True
        )

        index = await self.couchdb_db_metadata.design_doc("index", exists_ok=True)
        await index.create_view(
            "source",
            map_function="function (doc) { if(doc.source) { emit(doc.source, doc._id); } }",
            exists_ok=True,
        )

        await index.create_view(
            "historic",
            map_function="function (doc) { if(doc.historic) { emit(doc._id, null); } }",
            exists_ok=True,
        )

        components = await self.couchdb_db_metadata.design_doc(
            "components", exists_ok=True
        )
        await components.create_view(
            "historic",
            map_function="""function (doc) {
  if(doc.historic)
  {
    var name = ''
    var components = doc._id.split('.')
    components.reverse()
    components.forEach(function (key) {
      if (name === '') {
        name = key
      } else {
        name = key + '.' + name
      }
      emit(name, null)
    })
  }
}""",
            exists_ok=True,
        )

    async def connect(self):
        # First, connect to couchdb
        await self.connect_couchdb()

        # After that, we do the MetricQ connection stuff
        await super().connect()

        # TODO persistent?
        self.management_queue = await self._management_channel.declare_queue(
            self.management_queue_name, durable=True, robust=True
        )

        logger.info("creating rpc exchanges")
        self._management_exchange = await self._management_channel.declare_exchange(
            name=self._management_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self._management_broadcast_exchange = (
            await self._management_channel.declare_exchange(
                name=self._management_broadcast_exchange_name,
                type=aio_pika.ExchangeType.FANOUT,
                durable=True,
            )
        )

        await self.management_queue.bind(
            exchange=self._management_exchange, routing_key="#"
        )

        logger.info("establishing data connection to {}", self.data_url)
        data_connection = await self.make_connection(self.data_url)

        assert self.couchdb_db_config is not None
        self.queue_manager = QueueManager(
            data_connection=data_connection,
            config_db=self.couchdb_db_config,
        )

        # Make sure we can open a channel on the data connection.
        # This channel will be reused by the QueueManager for as long as possible.
        await self.queue_manager.ensure_open_channel()

        # Declare the data and history exchanges
        await self.queue_manager.declare_exchanges()

        await self.rpc_consume([self.management_queue])

    async def stop(self, exception):
        await self.queue_manager.close()

        await super().stop(exception)

    async def read_config(self, token, *, allow_missing=False):
        try:
            return (await self.couchdb_db_config[token]).data
        except NotFoundError:
            if allow_missing:
                return {}
            else:
                raise

    async def rpc(self, function, to_token=None, **kwargs):
        if to_token:
            kwargs["exchange"] = self._management_channel.default_exchange
            kwargs["routing_key"] = "{}-rpc".format(to_token)
            kwargs["cleanup_on_response"] = True
        else:
            kwargs["exchange"] = self._management_broadcast_exchange
            kwargs["routing_key"] = function
            kwargs["cleanup_on_response"] = False

        await super().rpc(function=function, **kwargs)

    @rpc_handler("subscribe", "sink.subscribe")
    async def handle_subscribe(self, from_token, metadata=True, **body):
        # TODO figure out why auto-assigned queues cannot be used by the client
        metrics = body.get("metrics", [])

        queue = await self.queue_manager.declare_sink_data_queue(
            client_token=from_token,
            queue_name=body.get("dataQueue"),
            expires=body.get("expires"),
            bindinds=metrics,
        )

        logger.info(
            "Declared data queue {!r} for {} (bound {} metric(s))",
            queue,
            from_token,
            len(metrics),
        )

        metrics_metadata = await self.fetch_metadata(metrics) if metadata else metrics

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": queue,
            "metrics": metrics_metadata,
        }

    @rpc_handler("unsubscribe", "sink.unsubscribe")
    async def handle_unsubscribe(self, from_token, **body):
        queue_name: str = body["dataQueue"]
        metrics: List[str] = body["metrics"]

        logger.debug(
            "Unsubscribing client {!r} from {} metric(s)", from_token, len(metrics)
        )

        await self.queue_manager.sink_unbind_metrics(
            client_token=from_token,
            queue_name=queue_name,
            metrics=metrics,
            publish_end_message=body.get("end", True),
        )

        metrics_metadata = await self.fetch_metadata(metrics)

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": queue_name,
            "metrics": metrics_metadata,
        }

    @rpc_handler("release", "sink.release")
    async def handle_release(self, from_token, **body):
        if self._subscription_autodelete:
            logger.debug(
                "release {} for {} ignored, auto-delete", body["dataQueue"], from_token
            )
        else:
            queue_name = body["dataQueue"]
            if not (queue_name.startswith(from_token) and queue_name.endswith("-data")):
                raise ValueError("Invalid subscription queue name")

            logger.debug("Releasing data queue {} of Sink {!r}", queue_name, from_token)

            await self.queue_manager.sink_delete_data_queue(queue_name)

    @rpc_handler("sink.register")
    async def handle_sink_register(self, from_token, **body):
        response = {
            "dataServerAddress": self.data_server_address,
            "config": await self.read_config(from_token),
        }
        return response

    @rpc_handler("source.register")
    async def handle_source_register(self, from_token, **body):
        response = {
            "dataServerAddress": self.data_server_address,
            "dataExchange": self.queue_manager.data_exchange.name,
            "config": await self.read_config(from_token),
        }
        return response

    def _get_queue_arguments_from_config(self, config):
        arguments = dict()
        try:
            arguments["x-message-ttl"] = int(1000 * config["message_ttl"])
        except KeyError:
            # No TTL set
            pass

        return arguments

    @rpc_handler("transformer.register")
    async def handle_transformer_register(self, from_token, **body):
        response = await self.handle_source_register(from_token, **body)
        return response

    @rpc_handler("transformer.subscribe")
    async def handle_transformer_subscribe(self, from_token, metrics, **body):
        logger.debug("attempting to declare queue for {}", from_token)
        data_queue = await self.queue_manager.declare_transformer_data_queue(
            transformer_token=from_token,
            bindinds=metrics,
        )
        logger.debug("declared queue {} for {}", data_queue, from_token)

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": data_queue,
            "metrics": await self.fetch_metadata(metrics),
        }

    @rpc_handler("source.metrics_list", "transformer.metrics_list")
    async def handle_source_metadata(self, from_token, **body):
        logger.warning("called deprecated source.metrics_list by {}", from_token)
        await self.handle_source_declare_metrics(from_token, **body)

    @rpc_handler("source.declare_metrics", "transformer.declare_metrics")
    async def handle_source_declare_metrics(self, from_token, metrics=None, **body):
        if metrics is None:
            logger.warning("client {} called declare_metrics without metrics")
            return

        # Convert metrics list to simple dict
        if isinstance(metrics, list):
            metrics = {metric: {} for metric in metrics}

        metrics_new = 0
        metrics_updated = 0

        update_date = (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        )

        def update_doc(doc, metadata, update_date):
            if "source" in metadata:
                logger.warn(
                    f"ignoring reserved field 'source' for metadata for '{doc.id}' from '{from_token}'"
                )
                del metadata["source"]

            if "date" not in metadata:
                doc["date"] = update_date

            if "historic" in metadata:
                logger.warn(
                    f"ignoring reserved metadata field 'historic' for '{doc.id}' from {from_token}"
                )
                del metadata["historic"]

            for key, value in metadata.items():
                if not key.startswith("_"):
                    doc[key] = value

        start = time.time()
        async with self.couchdb_db_metadata.update_docs(
            list(metrics.keys()), create=True
        ) as docs:
            async for doc in docs:
                if not doc.exists:
                    metrics_new += 1
                else:
                    metrics_updated += 1

                update_doc(doc, metrics[doc.id], update_date)
                doc["source"] = from_token
        end = time.time()

        if len(docs.status) != len(metrics):
            logger.error(
                "metadata update mismatch in metrics count expected {}, actual {}",
                len(metrics),
                len(docs.status),
            )
        error = False
        for s in docs.status:
            if "error" in s:
                error = True
                logger.error("error updating metadata {}", s)
        logger.info(
            "metadata update took {:.3f} s for {} new and {} existing metrics",
            end - start,
            metrics_new,
            metrics_updated,
        )
        if error:
            raise RuntimeError("metadata update failed")

    @rpc_handler("history.register")
    async def handle_history_register(self, from_token, **body):
        config = await self.read_config(from_token, allow_missing=True)

        logger.debug(
            "attempting to declare queue history response queue for {}", from_token
        )
        history_queue = await self.queue_manager.declare_history_response_queue(
            history_token=from_token
        )
        logger.debug("declared queue {} for {}", history_queue, from_token)

        response = {
            "historyServerAddress": self.data_server_address,
            "dataServerAddress": self.data_server_address,
            "historyExchange": self.queue_manager.history_exchange.name,
            "historyQueue": history_queue,
            "config": config,
        }
        return response

    @rpc_handler("history.get_metric_list")
    async def handle_get_metric_list(self, from_token, **body):
        logger.warning("called deprecated history.get_metric_list by {}", from_token)
        metric_list = [key async for key in self.couchdb_db_metadata.akeys()]
        response = {"metric_list": metric_list}
        return response

    @rpc_handler("get_metrics", "history.get_metrics")
    async def handle_get_metrics(
        self,
        from_token,
        format="array",
        historic=None,
        selector=None,
        prefix=None,
        infix=None,
        limit=None,
        **body,
    ):
        if format not in ("array", "object"):
            raise AttributeError("unknown format requested: {}".format(format))

        if infix is not None and prefix is not None:
            raise AttributeError('cannot get_metrics with both "prefix" and "infix"')

        selector_dict = dict()
        if selector is not None:
            if isinstance(selector, str):
                selector_dict["_id"] = {"$regex": selector}
            elif isinstance(selector, list):
                if len(selector) < 1:
                    raise ValueError("Empty selector list")
                if len(selector) == 1:
                    # That may possibly be faster.
                    selector_dict["_id"] = selector[0]
                else:
                    selector_dict["_id"] = {"$in": selector}
            else:
                raise TypeError(
                    "Invalid selector type: {}, supported: str, list", type(selector)
                )
        if historic is not None:
            if not isinstance(historic, bool):
                raise AttributeError(
                    'invalid type for "historic" argument: should be bool, is {}'.format(
                        type(historic)
                    )
                )

        # TODO can this be unified without compromising performance?
        # Does this even perform well?
        # ALSO: Async :-[
        if selector_dict:
            if historic is not None:
                selector_dict["historic"] = historic
            if prefix is not None or infix is not None:
                raise AttributeError(
                    'cannot get_metrics with both "selector" and "prefix" or "infix".'
                )
            aiter = self.couchdb_db_metadata.find(selector_dict, limit=limit)
            if format == "array":
                metrics = [doc["_id"] async for doc in aiter]
            elif format == "object":
                metrics = {doc["_id"]: doc.data async for doc in aiter}

        else:  # No selector dict, all *fix / historic filtering
            request_limit = limit
            if infix is None:
                request_prefix = prefix
                if historic is not None:
                    endpoint = self.couchdb_db_metadata.view("index", "historic")
                else:
                    endpoint = self.couchdb_db_metadata.all_docs
            else:
                request_prefix = infix
                # These views produce stupid duplicates thus we must filter ourselves and request more
                # to get enough results. We assume for no more than 6 infix segments on average
                if limit is not None:
                    request_limit = 6 * limit
                if historic is not None:
                    endpoint = self.couchdb_db_metadata.view("components", "historic")
                else:
                    raise NotImplementedError(
                        "non-historic infix lookup not yet supported"
                    )
            if format == "array":
                metrics = [
                    key
                    async for key in endpoint.ids(
                        prefix=request_prefix, limit=request_limit
                    )
                ]
                if request_limit != limit:
                    # Object of type islice is not JSON serializable m(
                    metrics = list(islice(sorted(set(metrics)), limit))
            elif format == "object":
                metrics = {
                    doc["_id"]: doc.data
                    async for doc in endpoint.docs(
                        prefix=request_prefix, limit=request_limit
                    )
                }
                if request_limit != limit:
                    metrics = dict(islice(sorted(metrics.items()), limit))

        return {"metrics": metrics}

    async def _mark_db_metrics(self, metric_names):
        """
        "UPDATE metadata SET historic=True WHERE _id in {metric_names}"
        in 38-33 beautiful lines of python code
        """
        start = time.time()
        async with self.couchdb_db_metadata.update_docs(
            metric_names, create=True
        ) as docs:
            async for doc in docs:
                doc["historic"] = True
        end = time.time()

        error = False
        for s in docs.status:
            if "error" in s:
                error = True
                logger.error("error updating metadata {}", s)
        logger.info(
            "historic metadata update took {:.3f} s for {} metrics",
            end - start,
            len(metric_names),
        )
        if error:
            raise RuntimeError("metadata update failed")

    @rpc_handler("db.subscribe")
    async def handle_db_subscribe(self, from_token, metrics, metadata=True, **body):
        data_queue, hreq_queue, metrics = await self.db_subscribe(
            db_token=from_token, metrics=metrics, metadata=metadata
        )

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": data_queue,
            "historyQueue": hreq_queue,
            "metrics": metrics,
        }

    @staticmethod
    def parse_db_bindings(
        bindings: List[Union[Metric, MetricInputAlias]],
    ) -> Tuple[List[Metric], List[Metric]]:
        data_bindings: List[Metric] = []
        history_bindings: List[Metric] = []

        for metric in bindings:
            data, hist = (
                (metric, metric)
                if isinstance(metric, str)
                else (metric.get("input") or metric["name"], metric["name"])
            )

            data_bindings.append(data)
            history_bindings.append(hist)

        return data_bindings, history_bindings

    async def db_subscribe(
        self,
        db_token: str,
        metrics: List[Union[Metric, MetricInputAlias]],
        metadata: bool,
    ) -> Tuple[DataQueueName, HreqQueueName, MetricList]:
        data_bindings, history_bindings = self.parse_db_bindings(bindings=metrics)

        data_queue, history_queue = await self.queue_manager.declare_db_queues(
            db_token=db_token,
            data_bindings=data_bindings,
            history_bindings=history_bindings,
        )

        # TODO unbind other metrics that are no longer relevant
        await self._mark_db_metrics(history_bindings)

        if metadata:
            metrics_metadata = await self.fetch_metadata(history_bindings)
        else:
            metrics_metadata = history_bindings

        return (data_queue, history_queue, metrics_metadata)

    @rpc_handler("db.register")
    async def handle_db_register(self, from_token, **body):
        config = await self.read_config(from_token)
        metric_configs = config["metrics"]

        # TODO once all deployed databases support an explicit subscribe, this part can be removed
        # this emulates the RPC format of db.subscribe
        def convert_metric_config(
            metric_name, metric_config
        ) -> Union[Metric, MetricInputAlias]:
            if "prefix" in metric_config and metric_config["prefix"]:
                raise ValueError("prefix no longer supported in the manager")
            elif "input" in metric_config:
                return {"name": metric_name, "input": metric_config["input"]}
            else:
                return metric_name

        subscribe_metrics = [
            convert_metric_config(*metric_item)
            for metric_item in metric_configs.items()
        ]
        data_queue, history_queue, _ = await self.db_subscribe(
            from_token, metrics=subscribe_metrics, metadata=False
        )
        # End of "removable" legacy stuff --- so we don't use the return value even if we could save some

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": data_queue,
            "historyQueue": history_queue,
            "config": config,
        }


@click.command()
@click.argument("rpc-url", default="amqp://localhost/")
@click.argument("data-url", default="amqp://localhost/")
@click.option("--queue-ttl", default=30 * 60 * 1000)
@click.option(
    "--config-path",
    default=".",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option("--monitor/--no-monitor", default=True)
@click.option("--couchdb-url", default="http://127.0.0.1:5984")
@click.option("--couchdb-user", default="admin")
@click.option("--couchdb-password", default="admin")
@click.option("--log-to-journal/--no-log-to-journal", default=False)
@click_log.simple_verbosity_option(logger)
def manager_cmd(
    rpc_url,
    data_url,
    config_path,
    queue_ttl,
    monitor,
    couchdb_url,
    couchdb_user,
    couchdb_password,
    log_to_journal,
):
    if log_to_journal:
        try:
            from systemd import journal

            logger.handlers[0] = journal.JournaldLogHandler()
        except ImportError:
            logger.error("Can't enable journal logger, systemd package not found!")
    manager = Manager(
        rpc_url,
        data_url,
        config_path,
        queue_ttl,
        couchdb_url,
        couchdb_user,
        couchdb_password,
    )
    if monitor:
        with aiomonitor.start_monitor(manager.event_loop, locals={"manager": manager}):
            manager.run()
    else:
        manager.run()
