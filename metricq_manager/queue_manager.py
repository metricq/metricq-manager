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

from aio_pika import Queue, RobustChannel, Exchange, RobustConnection
from aio_pika.channel import Channel
from aio_pika.exceptions import ChannelClosed
from aio_pika.exchange import ExchangeType
from aio_pika.message import Message
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


class SinkUnbindFailed(RuntimeError):
    pass


class DataQueueNotFound(RuntimeError):
    pass


class QueueManager:
    """The queue manager.

    Args:
        data_connection:
            An open AMQP connection.
            All queue operations are performed on the vhost to which it connects.
        config_db:
            An open connection to the CouchDB database that contains the per-client configuration.
        channel:
            An open channel on :code:`data_connection`.
            If omitted, a new channel is opened on demand.
    """

    def __init__(
        self,
        *,
        data_connection: RobustConnection,
        config_db: Database,
        channel: Optional[RobustChannel] = None,
    ):
        self.data_connection = data_connection
        self.config = config_db
        self._channel = channel

        # The exchanges to which metric data are sent.
        # The need to be declared with :meth:`declare_exchanges` before use.
        self._data_exchange: Optional[DataExchange] = None
        self._history_exchange: Optional[HistoryExchange] = None

    async def close(self):
        """Shutdown this queue manager.

        This closes any open channels and the data connection.
        """
        if self._channel is not None:
            logger.debug(
                "Closing channel on data connection {!r}", self.data_connection
            )
            await self._channel.close()
            self._channel = None

        logger.debug("Closing data connection {!r}", self.data_connection)
        await self.data_connection.close()

    async def ensure_open_channel(self):
        """Ensure that there is an open channel to perform queue operations."""
        logger.debug("Ensuring an open channel on {!r}", self.data_connection)
        await self.channel()

    async def channel(self) -> RobustChannel:
        """Return a channel on the data connection, opening a new one if necessary."""
        if self._channel is None:
            logger.debug(
                "Opening new channel on data connection {!r} for QueueManager",
                self.data_connection,
            )
            self._channel = await self.data_connection.channel()

        assert self._channel is not None
        return self._channel

    async def declare_queue(
        self,
        name: str,
        arguments: Dict[str, Any],
        auto_delete: bool = False,
        durable: bool = False,
    ) -> Queue:
        """Declare a new queue."""
        channel = await self.channel()
        return await channel.declare_queue(
            name=name,
            arguments=arguments,
            auto_delete=auto_delete,
            durable=durable,
            robust=False,
        )

    async def _declare_exchange(self, name: str) -> Exchange:
        logger.info("Declaring exchange {!r}", name)
        channel = await self.channel()
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
            This needs to be called before any operation that binds metrics is called.
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

    @asynccontextmanager
    async def fetch_data_queue(self, queue_name: str):
        """Opens a new channel to reliably fetch an existing data queue.

        If the queue does not exist, this raises :class:`.DataQueueNotFound`.
        """

        # Open a new (temporary) channel on the data connection.
        # If fetching the queue fails, the channel will be closed by the remote,
        # and `get_queue` raises `ChannelClosed`.
        channel: Channel = await self.data_connection.channel()

        try:
            queue = await channel.get_queue(queue_name)
        except ChannelClosed as e:
            raise DataQueueNotFound(f"Failed to fetch data queue {queue_name!r}") from e

        try:
            yield queue
        finally:
            # Close the temporary channel in a background task
            async def close():
                await channel.close()
                logger.debug("Closed temporary channel {!r}", channel)

            asyncio.create_task(close())

    async def declare_sink_data_queue(
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

        queue_name = config.queue_name(unique=True, default=queue_name)
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

        channel = await self.channel()
        data_queue = await channel.declare_queue(
            queue_name,
            auto_delete=True,
            arguments=arguments,
        )

        if bindinds:
            await self._bind_metrics(
                metrics=bindinds,
                queue=data_queue,
                exchange=self.data_exchange,
            )

        return data_queue.name

    async def _bind_metrics(
        self,
        metrics: List[str],
        queue: Queue,
        exchange: Exchange,
    ):
        """Bind a list of metrics from the given exchange to a queue."""
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
            async with self.fetch_data_queue(queue_name) as queue:
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
                    await self.publish_end_message(queue_name=queue_name)

        except DataQueueNotFound as e:
            err = SinkUnbindFailed(f"Data queue {queue_name!r} does not exist")
            logger.error(
                "Failed to unbind {} metric(s) for client {!r}: {} (cause: {})",
                len(metrics),
                queue_name,
                client_token,
                err,
                e,
            )
            raise err from e
        except ChannelClosed as e:
            raise SinkUnbindFailed(
                f"Channel closed while unbinding metrics from queue {queue_name!r} of client {client_token!r}"
            ) from e

    async def publish_end_message(self, queue_name: str):
        await self.channel.default_exchange.publish(
            Message(body=b"", type="end"), routing_key=queue_name
        )

    async def sink_delete_data_queue(self, queue_name: str):
        """Delete a Sink's data queue."""
        async with self.fetch_data_queue(queue_name) as queue:
            await queue.delete(if_unused=False, if_empty=False)

    async def declare_history_response_queue(self, history_token: str) -> HreqQueueName:
        """Declare a HistoryClient's history response queue.

        Historic metric data will arrive on this queue when requested by the :literal:`get_metrics` RPC.

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

    async def declare_durable_queue(self, config: ConfigParser) -> DataQueue:
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
        )

    async def declare_transformer_data_queue(
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
        data_queue = await self.declare_durable_queue(
            config=await self.read_config(client_token=transformer_token, role="data")
        )

        if bindinds:
            # TODO Also unbind other metrics that are no longer relevant
            await self._bind_metrics(
                metrics=bindinds, queue=data_queue, exchange=self.data_exchange
            )

        return data_queue.name

    async def declare_db_queues(
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

        data_queue = await self.declare_durable_queue(config=data_config)
        hreq_queue = await self.declare_durable_queue(config=hreq_config)

        bind_tasks = []
        if data_bindings is not None:
            logger.info("Binding metrics to data queue for database {!r}", db_token)
            bind_tasks.append(
                self._bind_metrics(
                    metrics=data_bindings,
                    queue=data_queue,
                    exchange=self.data_exchange,
                )
            )

        if history_bindings is not None:
            logger.info("Binding metrics to history queue for database {!r}", db_token)
            bind_tasks.append(
                self._bind_metrics(
                    metrics=history_bindings,
                    queue=hreq_queue,
                    exchange=self.history_exchange,
                )
            )

        if bind_tasks:
            await asyncio.gather(*bind_tasks)

        return (data_queue.name, hreq_queue.name)
