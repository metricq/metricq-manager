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

import asyncio
import datetime
import json
import logging
import os
import sys
import time
import uuid
from itertools import groupby, islice

import click

import aio_pika
import aiomonitor
import click_completion
import click_log
from aiocouch import CouchDB
from metricq import Agent, rpc_handler
from metricq.logging import get_logger
from yarl import URL

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

        self.data_connection = None
        self.data_channel = None

        if data_url.startswith("/"):  # vhost only
            # for the manager itself
            self.data_url = str(URL(self._management_url).with_path(data_url))
            # for clients
            self.data_server_address = data_url
        else:
            # for the manager itself
            self.data_url = data_url
            # for clients
            self.data_server_address = str(URL(data_url).with_user(None))

        self.data_exchange_name = "metricq.data"
        self.data_exchange = None

        self.history_exchange_name = "metricq.history"
        self.history_exchange = None

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

    async def connect(self):
        # First, connect to couchdb
        self.couchdb_db_config = await self.couchdb_client.create(
            "config", exists_ok=True
        )
        self.couchdb_db_metadata = await self.couchdb_client.create(
            "metadata", exists_ok=True
        )

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
        self._management_broadcast_exchange = await self._management_channel.declare_exchange(
            name=self._management_broadcast_exchange_name,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        )

        await self.management_queue.bind(
            exchange=self._management_exchange, routing_key="#"
        )

        logger.info("establishing data connection to {}", self.data_url)
        self.data_connection = await self.make_connection(self.data_url)
        self.data_channel = await self.data_connection.channel()

        logger.info("creating data exchanges")
        self.data_exchange = await self.data_channel.declare_exchange(
            name=self.data_exchange_name, type=aio_pika.ExchangeType.TOPIC, durable=True
        )
        self.history_exchange = await self.data_channel.declare_exchange(
            name=self.history_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        await self.rpc_consume([self.management_queue])

    async def stop(self, exception):
        logger.debug("closing data channel and connection in manager")
        if self.data_channel:
            await self.data_channel.close()
            self.data_channel = None
        if self.data_connection:
            await self.data_connection.close()
            self.data_connection = None
        await super().stop(exception)

    async def read_config(self, token):
        return (await self.couchdb_db_config[token]).data

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
        # TODO check if naming is allowed...
        queue_name = body.get("dataQueue")
        if queue_name is not None:
            if not (queue_name.startswith(from_token) and queue_name.endswith("-data")):
                raise ValueError("Invalid subscription queue name")
        else:
            uid = uuid.uuid4().hex
            queue_name = f"{from_token}-{uid}-data"
        logger.debug("attempting to declare queue {} for {}", queue_name, from_token)

        try:
            config = await self.read_config(from_token)
            arguments = self._get_queue_arguments_from_config(config)
        except KeyError:
            arguments = {}
        try:
            expires_seconds = int(body["expires"])
            if expires_seconds <= 0:
                expires_seconds = self._expires_seconds
        except (KeyError, ValueError, TypeError):
            expires_seconds = self._expires_seconds
        if expires_seconds:
            arguments["x-expires"] = expires_seconds * 1000

        queue = await self.data_channel.declare_queue(
            queue_name,
            auto_delete=self._subscription_autodelete,
            arguments=arguments,
            robust=False,
        )
        logger.info("declared queue {} for {}", queue, from_token)
        try:
            metrics = body["metrics"]
            if len(metrics) > 0:
                res = await asyncio.gather(
                    *[
                        queue.bind(exchange=self.data_exchange, routing_key=rk)
                        for rk in metrics
                    ],
                    loop=self.event_loop,
                )
                # TODO remove that later if it works stable
                logger.info(
                    "completed {} subscription bindings, result {}",
                    len(metrics),
                    [(len(list(g)), t) for t, g in groupby([type(r) for r in res])],
                )
        except KeyError:
            logger.warn("Got no metric list, assuming no metrics")
            metrics = []

        if metadata:
            metric_ids = metrics
            metrics = await self.fetch_metadata(metric_ids)

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": queue.name,
            "metrics": metrics,
        }

    @rpc_handler("unsubscribe", "sink.unsubscribe")
    async def handle_unsubscribe(self, from_token, **body):
        channel = await self.data_connection.channel()
        queue_name = body["dataQueue"]
        # TODO add once we get consistent tokens for subscription / unsubscription
        # if not (queue_name.startswith(from_token) and queue_name.endswith("-data")):
        #     raise ValueError("Invalid subscription queue name")

        logger.debug("unbinding queue {} for {}", queue_name, from_token)

        try:
            queue = await channel.declare_queue(
                queue_name,
                auto_delete=self._subscription_autodelete,
                passive=True,
                robust=False,
            )
            assert body["metrics"]
            await asyncio.gather(
                *[
                    queue.unbind(exchange=self.data_exchange, routing_key=rk)
                    for rk in body["metrics"]
                ],
                loop=self.event_loop,
            )
            if body.get("end", True):
                await self.data_channel.default_exchange.publish(
                    aio_pika.Message(body=b"", type="end"), routing_key=queue_name
                )
        except aio_pika.exceptions.ChannelClosed as e:
            logger.error("unsubscribe failed, queue timed out: {}", e)
            raise Exception("queue already timed out")

        metrics = await self.fetch_metadata(body["metrics"])

        self.event_loop.call_soon(asyncio.ensure_future, channel.close())
        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": queue_name,
            "metrics": metrics,
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

            logger.debug("releasing {} for {}", queue_name, from_token)
            queue = await self.data_channel.declare_queue(queue_name, robust=False)
            await queue.delete(if_unused=False, if_empty=False)

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
            "dataExchange": self.data_exchange.name,
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
        config = await self.read_config(from_token)
        arguments = self._get_queue_arguments_from_config(config)

        data_queue_name = f"{from_token}-data"
        logger.debug(
            "attempting to declare queue {} for {}", data_queue_name, from_token
        )
        data_queue = await self.data_channel.declare_queue(
            data_queue_name, durable=True, arguments=arguments, robust=False
        )
        logger.debug("declared queue {} for {}", data_queue, from_token)

        bindings = [
            data_queue.bind(exchange=self.data_exchange, routing_key=metric)
            for metric in metrics
        ]
        await asyncio.gather(*bindings)

        # TODO unbind other metrics that are no longer relevant
        response = dict()
        response["dataServerAddress"] = self.data_server_address
        response["dataQueue"] = data_queue_name
        response["metrics"] = await self.fetch_metadata(metrics)
        return response

    @rpc_handler("source.metrics_list", "transformer.metrics_list")
    async def handle_source_metadata(self, from_token, **body):
        logger.warning("called deprecated source.metrics_list by {}", from_token)
        self.handle_source_declare_metrics(from_token, **body)

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
        try:
            config = await self.read_config(from_token)
        except KeyError:
            config = {}
        arguments = self._get_queue_arguments_from_config(config)

        history_queue_name = f"{from_token}-hrsp"
        logger.debug(
            "attempting to declare queue {} for {}", history_queue_name, from_token
        )
        history_queue = await self.data_channel.declare_queue(
            history_queue_name, auto_delete=True, arguments=arguments, robust=False
        )
        logger.debug("declared queue {} for {}", history_queue, from_token)

        response = {
            "historyServerAddress": self.data_server_address,
            "dataServerAddress": self.data_server_address,
            "historyExchange": self.history_exchange.name,
            "historyQueue": history_queue_name,
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

    async def _declare_db_queues(self, db_token, config=None):
        if not config:
            config = await self.read_config(db_token)

        # TODO actually we do probably want separate message ttl for history requests and data!
        kwargs = {
            "arguments": self._get_queue_arguments_from_config(config),
            "durable": True,
            "robust": False,
        }

        return await asyncio.gather(
            self.data_channel.declare_queue(f"{db_token}-data", **kwargs),
            self.data_channel.declare_queue(f"{db_token}-hreq", **kwargs),
        )

    @rpc_handler("db.subscribe")
    async def handle_db_subscribe(self, from_token, metrics, metadata=True, **body):
        data_queue, history_queue = await self._declare_db_queues(from_token)

        bindings = []
        metric_names = []
        for metric in metrics:
            if isinstance(metric, str):
                data_routing_key = metric
                history_routing_key = metric
            else:
                data_routing_key = metric["name"]
                history_routing_key = metric["name"]

            metric_names.append(history_routing_key)
            bindings.append(
                history_queue.bind(
                    exchange=self.history_exchange, routing_key=history_routing_key
                )
            )
            bindings.append(
                data_queue.bind(
                    exchange=self.data_exchange, routing_key=data_routing_key
                )
            )

        await asyncio.gather(*bindings)
        # TODO unbind other metrics that are no longer relevant
        await self._mark_db_metrics(metric_names)

        if metadata:
            metrics_return = await self.fetch_metadata(metric_names)
        else:
            metrics_return = metric_names

        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": data_queue.name,
            "historyQueue": history_queue.name,
            "metrics": metrics_return,
        }

    @rpc_handler("db.register")
    async def handle_db_register(self, from_token, **body):
        config = await self.read_config(from_token)
        metric_configs = config["metrics"]

        # TODO once all deployed databases support an explicit subscribe, this part can be removed
        # this emulates the RPC format of db.subscribe
        def convert_metric_config(metric_name, metric_config):
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
        await self.handle_db_subscribe(
            from_token, metrics=subscribe_metrics, metadata=False
        )
        # End of "removable" legacy stuff --- so we don't use the return value even if we could save some

        data_queue, history_queue = await self._declare_db_queues(from_token, config)
        return {
            "dataServerAddress": self.data_server_address,
            "dataQueue": data_queue.name,
            "historyQueue": history_queue.name,
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
