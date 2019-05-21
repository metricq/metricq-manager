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
import time
import uuid
from itertools import groupby

import click

import aio_pika
import aiomonitor
import click_completion
import click_log
import cloudant
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

        self.data_url = data_url
        self.data_url_credentialfree = str(URL(data_url).with_user(None))

        self.data_exchange_name = "metricq.data"
        self.data_exchange = None

        self.history_exchange_name = "metricq.history"
        self.history_exchange = None

        self.config_path = config_path
        self.queue_ttl = queue_ttl

        self.couchdb_client = cloudant.client.CouchDB(
            couchdb_user, couchdb_password, url=couchdb_url, connect=True
        )
        self.couchdb_session = self.couchdb_client.session()
        self.couchdb_db_config = self.couchdb_client.create_database(
            "config"
        )  # , throw_on_exists=False)
        self.couchdb_db_metadata = self.couchdb_client.create_database("metadata")

        # TODO if this proves to be reliable, remove the option
        self._subscription_autodelete = True
        # TODO Make some config stuff
        self._expires_seconds = 3600

    async def fetch_metadata(self, metric_ids):
        """ This is async in case we ever make asynchronous couchdb requests """
        metadata = dict()
        # TODO use $in queries for very large requests
        for metric in metric_ids:
            try:
                metadata[metric] = self.couchdb_db_metadata[metric]
                # TODO avoid redundant fetches.. but how can we know?
                # We could check metric in self.couchdb_db_metadata.keys(remote=False)
                # But that function returns a list(!) not a set-like object like the
                # underlying dict or any sane object would
                metadata[metric].fetch()
            except KeyError:
                metadata[metric] = None
        return metadata

    async def connect(self):
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

    async def stop(self):
        logger.debug("closing data channel and connection in manager")
        if self.data_channel:
            await self.data_channel.close()
            self.data_channel = None
        if self.data_connection:
            await self.data_connection.close()
            self.data_connection = None
        await super().stop()

    def read_config(self, token):
        try:
            config_document = self.couchdb_db_config[token]
            config_document.fetch()
            return dict(config_document)
        except KeyError:
            with open(os.path.join(self.config_path, token + ".json"), "r") as f:
                return json.load(f)

    async def rpc(self, function, to_token=None, **kwargs):
        if to_token:
            kwargs["exchange"] = self._management_channel.default_exchange
            kwargs["routing_key"] = "{}-rpc".format(to_token)
            kwargs["cleanup_on_response"] = True
        else:
            kwargs["exchange"] = self._management_broadcast_exchange
            kwargs["routing_key"] = function
            kwargs["cleanup_on_response"] = False

        await self.rpc(function=function, **kwargs)

    @rpc_handler("subscribe", "sink.subscribe")
    async def handle_subscribe(self, from_token, metadata=True, **body):
        # TODO figure out why auto-assigned queues cannot be used by the client
        try:
            queue_name = body["dataQueue"]
        except KeyError:
            queue_name = "subscription-" + uuid.uuid4().hex
        logger.debug("attempting to declare queue {} for {}", queue_name, from_token)

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
            "dataServerAddress": self.data_url_credentialfree,
            "dataQueue": queue.name,
            "metrics": metrics,
        }

    @rpc_handler("unsubscribe", "sink.unsubscribe")
    async def handle_unsubscribe(self, from_token, **body):
        channel = await self.data_connection.channel()
        queue_name = body["dataQueue"]
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
            # Trying to avoid leaking closing futures. Super annoying
            try:
                await channel.closing
            except aio_pika.exceptions.ChannelClosed:
                pass
            raise Exception("queue already timed out")

        metrics = await self.fetch_metadata(body["metrics"])

        self.event_loop.call_soon(asyncio.ensure_future, channel.close())
        return {
            "dataServerAddress": self.data_url_credentialfree,
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
            logger.debug("releasing {} for {}", body["dataQueue"], from_token)
            queue = await self.data_channel.declare_queue(
                body["dataQueue"], robust=False
            )
            await queue.delete(if_unused=False, if_empty=False)

    @rpc_handler("sink.register")
    async def handle_sink_register(self, from_token, **body):
        response = {
            "dataServerAddress": self.data_url_credentialfree,
            "config": self.read_config(from_token),
        }
        return response

    @rpc_handler("source.register")
    async def handle_source_register(self, from_token, **body):
        response = {
            "dataServerAddress": self.data_url_credentialfree,
            "dataExchange": self.data_exchange.name,
            "config": self.read_config(from_token),
        }
        return response

    @rpc_handler("transformer.register")
    async def handle_transformer_register(self, from_token, **body):
        response = await self.handle_source_register(from_token, **body)
        return response

    @rpc_handler("transformer.subscribe")
    async def handle_transformer_subscribe(self, from_token, metrics, **body):
        config = self.read_config(from_token)
        arguments = dict()
        try:
            arguments["x-message-ttl"] = int(1000 * config["message_ttl"])
        except KeyError:
            try:
                # TODO deprecate either :/
                arguments["x-message-ttl"] = int(1000 * config["messageTtl"])
            except KeyError:
                # No TTL set
                pass

        data_queue_name = "data-" + from_token
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
        response["dataServerAddress"] = self.data_url_credentialfree
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

        def update_doc(row):
            nonlocal metrics_new, metrics_updated, metrics, from_token
            metric = row["key"]
            document = metrics[metric]

            for key in list(document.keys()):
                if key.startswith("_"):
                    del document[key]

            document["_id"] = metric
            if "source" in document:
                logger.warn(
                    f"The field 'source' was already set in declare_metrics from '{from_token}' for metric '{metric}'. Overwritting."
                )
            document["source"] = from_token
            if "date" not in document:
                document["date"] = (
                    datetime.datetime.utcnow()
                    .replace(tzinfo=datetime.timezone.utc)
                    .isoformat()
                )
            if "id" in row:
                try:
                    metrics_updated += 1
                    if metric != row["id"]:
                        logger.error(
                            "inconsistent key/id while updating metadata {} != {}",
                            metric,
                            row["id"],
                        )
                    if "deleted" not in row["value"]:
                        document["_rev"] = row["value"]["rev"]
                        try:
                            document["historic"] = row["doc"]["historic"]
                        except KeyError:
                            pass
                except KeyError:
                    logger.error(
                        "something went wrong trying to update existing metadata document {}",
                        metric,
                    )
                    raise
            else:
                metrics_new += 1
            return document

        start = time.time()
        docs = self.couchdb_db_metadata.all_docs(
            keys=list(metrics.keys()), include_docs=True
        )
        new_docs = [update_doc(row) for row in docs["rows"]]
        status = self.couchdb_db_metadata.bulk_docs(new_docs)
        end = time.time()
        if len(status) != len(metrics):
            logger.error(
                "metadata update mismatch in metrics count expected {}, actual {}",
                len(metrics),
                len(status),
            )
        error = False
        for s in status:
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
        history_uuid = from_token
        history_queue_name = "history-" + history_uuid
        logger.debug(
            "attempting to declare queue {} for {}", history_queue_name, from_token
        )
        history_queue = await self.data_channel.declare_queue(
            history_queue_name, auto_delete=True, robust=False
        )
        logger.debug("declared queue {} for {}", history_queue, from_token)

        response = {
            "historyServerAddress": self.data_url_credentialfree,
            "dataServerAddress": self.data_url_credentialfree,
            "historyExchange": self.history_exchange.name,
            "historyQueue": history_queue_name,
            "config": self.read_config(from_token),
        }
        return response

    @rpc_handler("history.get_metric_list")
    async def handle_get_metric_list(self, from_token, **body):
        logger.warning("called deprecated history.get_metric_list by {}", from_token)
        metric_list = self.couchdb_db_metadata.keys(remote=True)
        response = {"metric_list": metric_list}
        return response

    @rpc_handler("get_metrics", "history.get_metrics")
    async def handle_get_metrics(
        self, from_token, format="array", historic=None, selector=None, **body
    ):
        if format not in ("array", "object"):
            raise AttributeError("unknown format requested: {}".format(format))

        selector_dict = dict()
        if selector is not None:
            if isinstance(selector, str):
                selector_dict["_id"] = {"$regex": selector}
            elif isinstance(selector, list):
                if len(selector) < 1:
                    raise ValueError("Empty selector list")
                if len(selector) == 1:
                    # That's *much* faster... really :(
                    selector_dict["_id"] = selector
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
            selector_dict["historic"] = historic

        # TODO can this be unified without compromising performance?
        # Does this even perform well?
        # ALSO: Async :-[
        if selector_dict:
            result = self.couchdb_db_metadata.get_query_result(
                selector_dict, page_size=100_000
            )
            if format == "array":
                metrics = [doc["_id"] for doc in result]
            if format == "object":
                metrics = {doc["_id"]: doc for doc in result}
        else:
            if format == "array":
                metrics = self.couchdb_db_metadata.keys(remote=True)
            if format == "object":
                metrics = {doc["_id"]: doc for doc in self.couchdb_db_metadata}

        return {"metrics": metrics}

    async def _mark_db_metrics(self, metric_names):
        """
        "UPDATE metadata SET historic=True WHERE _id in {metric_names}"
        in 38 beautiful lines of python code
        """

        def update_doc(row):
            nonlocal metric_names
            metric = row["key"]

            document = dict()
            document["_id"] = metric
            if "id" in row:
                try:
                    if metric != row["id"]:
                        logger.error(
                            "inconsistent key/id while updating metadata {} != {}",
                            metric,
                            row["id"],
                        )
                    if "deleted" not in row["value"]:
                        document["_rev"] = row["value"]["rev"]
                        document.update(row["doc"])
                except KeyError:
                    logger.error(
                        "something went wrong trying to update existing metadata document {}",
                        metric,
                    )
                    raise
            else:
                document["date"] = (
                    datetime.datetime.utcnow()
                    .replace(tzinfo=datetime.timezone.utc)
                    .isoformat()
                )
            # Do the one thing we actually want to do
            document["historic"] = True
            return document

        start = time.time()
        docs = self.couchdb_db_metadata.all_docs(keys=metric_names, include_docs=True)
        new_docs = [update_doc(row) for row in docs["rows"]]
        status = self.couchdb_db_metadata.bulk_docs(new_docs)
        end = time.time()
        if len(status) != len(metric_names):
            logger.error(
                "metadata update mismatch in metrics count expected {}, actual {}",
                len(metric_names),
                len(status),
            )
        error = False
        for s in status:
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

    @rpc_handler("db.register")
    async def handle_db_register(self, from_token, metadata=False, **body):
        db_uuid = from_token

        config = self.read_config(from_token)
        metric_configs = config["metrics"]
        if not metric_configs:
            raise ValueError("db not properly configured, metrics empty")

        history_queue_name = "history-" + db_uuid
        logger.debug(
            "attempting to declare queue {} for {}", history_queue_name, from_token
        )
        history_queue = await self.data_channel.declare_queue(
            history_queue_name, durable=True, robust=False
        )
        logger.debug("declared queue {} for {}", history_queue, from_token)

        data_queue_name = "data-" + db_uuid
        logger.debug(
            "attempting to declare queue {} for {}", data_queue_name, from_token
        )
        data_queue = await self.data_channel.declare_queue(
            data_queue_name, durable=True, robust=False
        )
        logger.debug("declared queue {} for {}", data_queue, from_token)

        if isinstance(metric_configs, list):
            # old legacy mode
            metric_names = history_routing_keys = data_routing_keys = [
                metric["name"] for metric in metric_configs
            ]
        else:
            history_routing_keys = []
            data_routing_keys = []
            metric_names = []
            for name, metric_config in metric_configs.items():
                if "prefix" in metric_config and metric_config["prefix"]:
                    history_routing_keys.append(name + ".#")
                    data_routing_keys.append(name + ".#")
                    # TODO fetch pattern from DB
                    # This won't work with the db metadata
                elif "input" in metric_config:
                    history_routing_keys.append(name)
                    data_routing_keys.append(metric_config["input"])
                    metric_names.append(name)
                else:
                    history_routing_keys.append(name)
                    data_routing_keys.append(name)
                    metric_names.append(name)

        binds = []

        for routing_key in history_routing_keys:
            binds.append(
                history_queue.bind(
                    exchange=self.history_exchange, routing_key=routing_key
                )
            )
        for routing_key in data_routing_keys:
            binds.append(
                data_queue.bind(exchange=self.data_exchange, routing_key=routing_key)
            )

        await asyncio.gather(*binds)
        await self._mark_db_metrics(metric_names)

        # TODO unbind other metrics that are no longer relevant

        response = {
            "dataServerAddress": self.data_url_credentialfree,
            "dataQueue": data_queue_name,
            "historyQueue": history_queue_name,
            "config": config,
        }
        if metadata:
            response["metrics"] = await self.fetch_metadata(metric_names)
        return response


@click.command()
@click.argument("rpc-url", default="amqp://localhost/")
@click.argument("data-url", default="amqp://localhost:5672/")
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
