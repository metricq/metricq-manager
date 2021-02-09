# Copyright (C) 2020, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# SPDX-License-Identifier: GPL-3.0-or-later

"""The queue manager is responsible for executing the AMQP queue operation as necessary for client RPCs.

This includes:

    * declaring and deleting data queues
    * binding the correct routing keys to data queues on subscription to a set of metrics

For each client, the final queue parameters depends on several parameters.
These are:

1. The client configuration:
    :meth:`read_config` retrieves a configuration key for a client from the database :attr:`QueueManager.config_db`.
    It is the source of the AMQP queue type and message TTL settings (See :class:`.ConfigParser` for details).

2. RPC arguments:
    These are passed to methods of this class.
    See for example the :code:`expires`-argument on :meth:`declare_sink_data_queue`.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

from aio_pika import (
    Exchange,
    ExchangeType,
    Message,
    Queue,
    RobustChannel,
    RobustConnection,
)
from aio_pika.exceptions import AMQPChannelError, ChannelClosed
from aiocouch.database import Database, NotFoundError
from metricq import get_logger

from .config_parser import ConfigParser

logger = get_logger(__name__)

Seconds = Union[int, float]

HreqQueue = Queue
DataQueue = Queue

DataExchange = Exchange
HistoryExchange = Exchange

Metric = str
DataQueueName = str
HreqQueueName = str


class QueueManager:
    """The queue manager.

    Args:
        data_connection:
            An open AMQP connection.
            All queue operations are performed on the vhost to which it connects.
        config_db:
            An open connection to the CouchDB database that contains the per-client configuration.
    """

    def __init__(
        self,
        *,
        data_connection: RobustConnection,
        config_db: Database,
    ):
        self.data_connection = data_connection
        self.config = config_db

        # The exchanges to which metric data are sent.
        # They need to be declared with :meth:`declare_exchanges` before use.
        self._data_exchange: Optional[DataExchange] = None
        self._history_exchange: Optional[HistoryExchange] = None

    async def close(self):
        """Shutdown this queue manager.

        This closes the data connection.
        """
        logger.debug("Closing data connection {!r}", self.data_connection)
        await self.data_connection.close()

    @asynccontextmanager
    async def temporary_channel(self, reuse: Optional[RobustChannel] = None):
        if reuse is not None:
            logger.debug("Reusing channel {!r} as temporary channel", reuse)
            yield reuse
            return

        channel: RobustChannel = await self.data_connection.channel()
        logger.debug("Opened temporary channel {!r}", channel)

        try:
            yield channel
        except ChannelClosed:
            logger.warning("Temporary channel {!r} closed unexpectedly!", channel)
            raise
        finally:
            logger.debug("Closing temporary channel {!r}", channel)
            try:
                await channel.close()
            except AMQPChannelError as e:
                logger.warning("Failed to close temporary channel {!r}: {}", channel, e)

    async def declare_queue(
        self,
        name: str,
        arguments: Dict[str, Any],
        auto_delete: bool = False,
        durable: bool = False,
        channel: Optional[RobustChannel] = None,
    ) -> Queue:
        """Declare a new queue."""

        async with self.temporary_channel(reuse=channel) as channel:
            return await channel.declare_queue(
                name=name,
                arguments=arguments,
                auto_delete=auto_delete,
                durable=durable,
                robust=False,
            )

    async def _declare_exchange(self, name: str) -> Exchange:
        logger.info("Declaring exchange {!r}", name)
        async with self.temporary_channel() as channel:
            return await channel.declare_exchange(
                name=name, type=ExchangeType.TOPIC, durable=True
            )

    async def declare_exchanges(
        self,
        data_exchange_name: str = "metricq.data",
        history_exchange_name: str = "metricq.history",
    ) -> Tuple[DataExchange, HistoryExchange]:
        """Declare the data and history exchanges.

        Note:
            This needs to be called before any operation that binds metrics, is called.
        """
        logger.info(
            "Declaring data ({!r}) and history ({!r}) exchanges",
            data_exchange_name,
            history_exchange_name,
        )
        if self._data_exchange is None:
            self._data_exchange = await self._declare_exchange(data_exchange_name)

        if self._history_exchange is None:
            self._history_exchange = await self._declare_exchange(history_exchange_name)

        return (self._data_exchange, self._history_exchange)

    @property
    def data_exchange(self) -> DataExchange:
        """The data exchange.

        Sources and Transformers publish metric data to this exchange.

        Raises:
            RuntimeError:
                The data exchange has not been declared with :meth:`declare_exchanges`.
        """
        if self._data_exchange is None:
            raise RuntimeError("Data exchange has not been declared")

        return self._data_exchange

    @property
    def history_exchange(self) -> HistoryExchange:
        """The history exchange.

        Databases publish `historic` metric data to this exchange.

        Raises:
            RuntimeError:
                The history exchange has not been declared with :meth:`declare_exchanges`.
        """
        if self._history_exchange is None:
            raise RuntimeError("History exchange has not been declared")

        return self._history_exchange

    async def read_config(
        self, client_token: str, role: str, allow_missing: bool = False
    ) -> ConfigParser:
        """Read the configuration for a client from the configuration database.

        Args:
            client_token:
                Token of the clients whose configuration to read.
            role:
                Role of the queue whose arguments are to be parsed from the configuration.
                Currently, roles :literal:`"data"` (data queues), :literal:`"hreq"`
                (history request queues) and :literal:`"hrsp"` (history response queues)
                are used.
                Most importantly, a clients queue type is determined by the key
                :literal:`x-metricq.{role}-queue-type`: a setting of

                .. code-block: json

                    { "x-metricq": { "data-queue-type": "quorum" } }

                will force a clients data queue to be declared as a "quorum queue".
            allow_missing:
                If true and the database contains no entry for this :code:`client_token`,
                a default configuration is returned.
        Returns:
            A :class:`ConfigParser` for structured access to the configuration.
        """
        config = {}
        try:
            config = (await self.config[client_token]).data
        except NotFoundError:
            if not allow_missing:
                raise

        return ConfigParser(config=config, role=role, client_token=client_token)

    async def sink_declare_data_queue(
        self,
        client_token: str,
        queue_name: Optional[str],
        expires: Optional[Seconds] = None,
        bindinds: Optional[List[Metric]] = None,
    ) -> DataQueueName:
        """Declare a Sink's data queue and bind the requested metrics to it.

        Args:
            client_token:
                The Sink's token.
            queue_name:
                An optional queue name.
                If given, the queue is declared with this name, reusing it if it already exists.
            expires:
                The number of seconds after which the queue will be deleted once the Sink disconnects.
            bindinds:
                An optional list of metrics names this Sink subscribes to.
                These are bound to the newly declared data queue.

        Returns:
            The name of the newly declared data queue.
        """
        if not client_token.startswith("sink-"):
            logger.warning('Sink token {!r} should start with "sink-"', client_token)

        config = await self.read_config(client_token, role="data", allow_missing=True)

        queue_name = config.queue_name(unique=True, validate=queue_name)
        arguments = dict(config.classic_arguments())

        if isinstance(expires, (int, float)) and expires > 0:
            arguments["x-expires"] = int(1000 * expires)
        elif expires is None:
            pass
        else:
            logger.warning(
                "Invalid message expiry requested from {!r}: {!r} is not a positive number of seconds",
                client_token,
                expires,
            )

        async with self.temporary_channel() as channel:
            data_queue = await self.declare_queue(
                queue_name,
                auto_delete=True,
                durable=False,
                arguments=arguments,
                channel=channel,
            )

            if bindinds:
                await self._bind_metrics(
                    metrics=bindinds,
                    queue=data_queue,
                    exchange=self.data_exchange,
                    channel=channel,
                )

            return data_queue.name

    async def _bind_metrics(
        self,
        metrics: List[str],
        queue: Queue,
        exchange: Exchange,
        *,
        channel: RobustChannel,
    ):
        """Bind a list of metrics from the given exchange to a queue.

        Warning:
            The ``queue`` must have been declared on the provided ``channel``!

        """
        assert queue.channel is channel.channel, (
            "Metrics must be bound to a queue using the *SAME* channel the queue was declared with. "
            "This is a BUG! "
            f"(queue.channel={queue.channel!r}, channel.channel={channel.channel!r})"
        )

        logger.info(
            "Binding metrics to queue {!r} on exchange {!r}",
            queue,
            exchange.name,
        )
        bind_results = await asyncio.gather(
            *(queue.bind(exchange=exchange, routing_key=metric) for metric in metrics)
        )

        logger.info(
            "Successfully bound {}/{} metric(s) to queue {!r}",
            len(bind_results),
            len(metrics),
            queue,
        )

    async def sink_unbind_metrics(
        self,
        client_token: str,
        queue_name: str,
        metrics: List[str],
        publish_end_message: bool,
    ):
        """Unbinding metrics from a data queue when a Sink unsubscribes from them.

        Args:
            client_token:
                Token of this Sink.
            queue_name:
                The Sink's current data queue.
            metrics:
                The metrics to unsubscribe from.
            publish_end_message:
                If :code:`True`, send an explicit :literal:`end`-message to the Sink after unsubscription.
        """
        try:
            async with self.temporary_channel() as channel:
                queue = await channel.get_queue(queue_name)
                logger.debug(
                    "Unbinding {} metric(s) from data queue {!r} for client {!r}",
                    len(metrics),
                    queue_name,
                    client_token,
                )

                await asyncio.gather(
                    *(
                        queue.unbind(exchange=self.data_exchange, routing_key=metric)
                        for metric in metrics
                    )
                )

                if publish_end_message:
                    await self.publish_end_message(
                        queue_name=queue_name, channel=channel
                    )

        except Exception as e:
            logger.error(
                "Failed to unbind {} metric(s) from queue {!r} of client {!r}: {}",
                len(metrics),
                queue_name,
                client_token,
                e,
            )
            raise e

    async def publish_end_message(self, queue_name: str, channel: RobustChannel):
        await channel.default_exchange.publish(
            Message(body=b"", type="end"), routing_key=queue_name
        )

    async def sink_delete_data_queue(self, queue_name: str):
        """Delete a Sink's data queue."""
        async with self.temporary_channel() as channel:
            queue = await channel.get_queue(queue_name)
            await queue.delete(if_unused=False, if_empty=False)

    async def history_declare_response_queue(self, history_token: str) -> HreqQueueName:
        """Declare a HistoryClient's history response queue.

        Historic metric metadata will arrive on this queue when requested by the :literal:`get_metrics` RPC.

        Args:
            history_token:
                Token of the HistoryClient.
        """
        config = await self.read_config(history_token, role="hrsp", allow_missing=True)
        queue_name = config.queue_name(unique=False)
        arguments = dict(config.classic_arguments())

        hreq_queue = await self.declare_queue(
            queue_name,
            arguments=arguments,
            auto_delete=True,
        )

        return hreq_queue.name

    async def declare_durable_queue(
        self, config: ConfigParser, channel: Optional[RobustChannel] = None
    ) -> DataQueue:
        """Declare a queue with arguments parsed from the given configuration.

        The queue will be :literal:`durable`, i.e. survive broker restarts.
        The queue_name will be deterministic for each client,
        declaring the queue twice for the same client will result in the same name.

        Args:
            config:
                A client configuration object from which queue arguments are parsed.
        """
        queue_name = config.queue_name(unique=False)
        arguments = dict(config.arguments())

        return await self.declare_queue(
            queue_name,
            arguments=arguments,
            durable=True,
            channel=channel,
        )

    async def transformer_declare_data_queue(
        self,
        transformer_token: str,
        bindinds: Optional[List[Metric]] = None,
    ) -> DataQueueName:
        """Declare a Transformer's data queue and bind the requested metrics to it.

        Args:
            transformer_token:
                Token of the Transformer.
            bindinds:
                An optional list of metrics to bind on this queue.
        """
        async with self.temporary_channel() as channel:
            data_queue = await self.declare_durable_queue(
                config=await self.read_config(
                    client_token=transformer_token, role="data"
                ),
                channel=channel,
            )

            if bindinds:
                # TODO Also unbind other metrics that are no longer relevant
                await self._bind_metrics(
                    metrics=bindinds,
                    queue=data_queue,
                    exchange=self.data_exchange,
                    channel=channel,
                )

            return data_queue.name

    async def db_declare_queues(
        self,
        db_token: str,
        data_bindings: Optional[List[Metric]] = None,
        history_bindings: Optional[List[Metric]] = None,
    ) -> Tuple[DataQueueName, HreqQueueName]:
        """Declare a Database's data- and history request queue.

        Optionally bind the given metrics to either queue.

        Args:
            db_token:
                Token of the Database.
            data_bindings:
                Bindings for metrics to be saved by the data queue.
            history_bindings:
                Bindings for requests for historical metric data.
        Returns:
            The newly declared queues.
        """
        data_config = await self.read_config(client_token=db_token, role="data")
        hreq_config = data_config.replace(role="hreq")

        async with self.temporary_channel() as channel:
            data_queue = await self.declare_durable_queue(
                config=data_config, channel=channel
            )
            hreq_queue = await self.declare_durable_queue(
                config=hreq_config, channel=channel
            )

            bind_tasks = []
            if data_bindings is not None:
                logger.info(
                    "Binding {} metric(s) to data queue for database {!r}",
                    len(data_bindings),
                    db_token,
                )
                bind_tasks.append(
                    self._bind_metrics(
                        metrics=data_bindings,
                        queue=data_queue,
                        exchange=self.data_exchange,
                        channel=channel,
                    )
                )

            if history_bindings is not None:
                logger.info(
                    "Binding {} metric(s) to history queue for database {!r}",
                    len(history_bindings),
                    db_token,
                )
                bind_tasks.append(
                    self._bind_metrics(
                        metrics=history_bindings,
                        queue=hreq_queue,
                        exchange=self.history_exchange,
                        channel=channel,
                    )
                )

            if bind_tasks:
                await asyncio.gather(*bind_tasks)

            return (data_queue.name, hreq_queue.name)
