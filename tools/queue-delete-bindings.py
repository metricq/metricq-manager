#!/usr/bin/env python3
# Copyright (c) 2021, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#     * Neither the name of metricq nor the names of its contributors
#       may be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import asyncio
import logging
from typing import List, Optional, Tuple
from urllib.parse import quote
from aio_pika import Channel, Exchange, RobustConnection, RobustChannel

import aiohttp
import click
import click_completion
import click_log
import metricq
from aiohttp.client import ClientSession
from pamqp.specification import AMQPChannelError
from metricq_manager.manager import Metric
from metricq_manager.queue_manager import ExchangeName
from yarl import URL

logger = metricq.get_logger()
logger.setLevel(logging.WARN)
click_log.basic_config(logger)
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)

click_completion.init()

QueueName = str


class MetricQDeleteBindings(metricq.Agent):
    def __init__(
        self,
        server: str,
        data_vhost: str,
        dry_run: bool,
        http_api_url: Optional[str] = None,
    ):
        self.http_api_url = (
            URL(server).with_scheme("https").with_port(None)
            if http_api_url is None
            else URL(http_api_url)
        )
        self.data_vhost = data_vhost
        self.dry_run = dry_run
        super().__init__(
            token="management-db-queue-delete", management_url=server, add_uuid=True
        )

        self.data_connection: Optional[RobustConnection] = None

    def data_vhost_quoted(self) -> str:
        return quote(self.data_vhost, safe="")

    async def connect(self):
        await super().connect()

        data_url = URL(self._management_url).with_path(self.data_vhost)

        self.data_connection = await self.make_connection(str(data_url))

    async def fetch_queue_bindings(
        self, http_session: ClientSession, exchange: str, queue: str
    ) -> List[Metric]:
        vhost = self.data_vhost_quoted()
        get_url = self.http_api_url.with_path(
            f"/api/bindings/{vhost}/e/{exchange}/q/{queue}/", encoded=True
        )
        logger.info(
            "Fetching queue bindings from {!r}",
            get_url.with_user("***").with_password("***").human_repr(),
        )
        async with http_session.get(get_url) as response:
            bindings_json = await response.json()
            if not response.ok:
                error = bindings_json.get("error")
                raise RuntimeError(f"RabbitMQ API returned an error: {error}")
            return [binding["routing_key"] for binding in bindings_json]

    async def delete_queue_bindings(
        self,
        queue_name: QueueName,
        exchange: ExchangeName,
        http_session: ClientSession,
        channel: RobustChannel,
    ):
        bindings = await self.fetch_queue_bindings(
            http_session=http_session, exchange=exchange, queue=queue_name
        )

        if self.dry_run:
            logger.info(
                "Dry-run: would delete {} binding(s) from queue {!r}:",
                len(bindings),
                queue_name,
            )
            return

        logger.info(
            "Deleting {} binding(s) from exchange {!r} to queue {!r}",
            len(bindings),
            exchange,
            queue_name,
        )
        queue = await channel.declare_queue(queue_name, passive=True)
        await asyncio.gather(
            *(
                queue.unbind(exchange=exchange, routing_key=metric)
                for metric in bindings
            )
        )
        logger.info("Done deleting bindings from queue {!r}", queue_name)

    async def delete_bindings(self, *queues: Tuple[QueueName, ExchangeName]):
        auth = (
            aiohttp.BasicAuth(
                login=self.http_api_url.user, password=self.http_api_url.password
            )
            if self.http_api_url.user and self.http_api_url.password
            else None
        )

        channel = await self.data_connection.channel()
        try:
            logger.info("Declaring exchanges...")
            await asyncio.gather(
                *[
                    channel.declare_exchange(exchange, passive=True)
                    for _, exchange in queues
                ]
            )

            async with ClientSession(auth=auth) as session:
                delete_tasks = {
                    self.delete_queue_bindings(
                        queue_name=queue,
                        exchange=exchange,
                        http_session=session,
                        channel=channel,
                    )
                    for queue, exchange in queues
                }

                logger.info(
                    "Deleting all bindings between the following: {}",
                    ", ".join(f"{exchange} to {queue}" for queue, exchange in queues),
                )
                await asyncio.gather(*delete_tasks)
        finally:
            try:
                await channel.close()
            except AMQPChannelError:
                pass


@click.command()
@click_log.simple_verbosity_option(logger, default="warning")
@click.option(
    "--server",
    default="amqp://admin:admin@localhost/",
    help="Management address of the MetricQ server",
    metavar="<AMQP URL>",
)
@click.option(
    "--rabbitmq-api",
    default=None,
    help="RabbitMQ API address of the MetricQ server (derived from --server if omitted)",
    metavar="<HTTP URL>",
)
@click.option(
    "--data-vhost",
    default="data",
    help="vhost on which the queues live",
    metavar="VHOST",
)
@click.option(
    "--data-exchange",
    default="metricq.data",
    help="Data exchange for metric data",
    metavar="NAME",
)
@click.option(
    "--history-exchange",
    default="metricq.history",
    help="History exchange for history requests",
    metavar="NAME",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Do not delete bindings, print what would've been done",
)
@click.argument("queue_list", nargs=-1, metavar="QUEUE ...")
def preregister_command(
    server,
    rabbitmq_api,
    queue_list: List[QueueName],
    data_vhost: str,
    data_exchange: ExchangeName,
    history_exchange: ExchangeName,
    dry_run: bool,
):
    """Delete all bindings to one or multiple (data/history request) queues."""

    queues = []
    for queue in queue_list:
        if queue.endswith("data"):
            exchange = data_exchange
        elif queue.endswith("hreq"):
            exchange = history_exchange
        else:
            raise click.BadArgumentUsage(
                f"Cannot derive exchange from queue name {queue!r}, QUEUE should end in 'data' or 'hreq'"
            )
        queues.append((queue, exchange))

    async def run():
        deleter = MetricQDeleteBindings(
            server=server,
            data_vhost=data_vhost,
            dry_run=dry_run,
            http_api_url=rabbitmq_api,
        )

        try:
            await deleter.connect()
            await deleter.delete_bindings(*queues)
        except Exception as e:
            logger.error("Failed to delete bindings of queue: {}", e)
        finally:
            await deleter.stop(None)

    asyncio.run(run())


if __name__ == "__main__":
    preregister_command()
