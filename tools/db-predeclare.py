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
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import click
import click_completion
import click_log
import metricq
from aiocouch.couchdb import CouchDB
from aiocouch.database import Database
from aiohttp.client import ClientSession
from yarl import URL

from metricq_manager.manager import Metric
from metricq_manager.queue_manager import QueueManager

logger = metricq.get_logger()
logger.setLevel(logging.WARN)
click_log.basic_config(logger)
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)

click_completion.init()


class MetricqDbPreregister(metricq.Agent):
    def __init__(
        self,
        server: str,
        data_exchange: str,
        history_exchange: str,
        declare_history_bindings: bool,
        data_vhost: str,
        couchdb: CouchDB,
        dry_run: bool,
        http_api_url: Optional[str] = None,
    ):
        self.http_api_url = (
            URL(server).with_scheme("https").with_port(None)
            if http_api_url is None
            else URL(http_api_url)
        )
        self.data_vhost = data_vhost
        self.data_exchange = data_exchange
        self.history_exchange = history_exchange
        self.declare_history_bindings = declare_history_bindings
        self.couchdb_client = couchdb
        self.dry_run = dry_run
        super().__init__(
            token="management-db-predeclare", management_url=server, add_uuid=True
        )

        self.queue_manager: Optional[QueueManager] = None

    def data_vhost_quoted(self) -> str:
        return quote(self.data_vhost, safe="")

    async def connect(self):
        await super().connect()

        data_url = URL(self._management_url).with_path(self.data_vhost)

        self.queue_manager = QueueManager(
            data_connection=await self.make_connection(str(data_url)),
            config_db=await self.config_db(),
        )

        await self.queue_manager.declare_exchanges(
            data_exchange_name=self.data_exchange,
            history_exchange_name=self.history_exchange,
        )

    async def config_db(self) -> Database:
        return await self.couchdb_client.create("config", exists_ok=True)

    async def fetch_queue_bindings(
        self, http_session: ClientSession, exchange: str, queue: str
    ) -> List[Metric]:
        vhost = self.data_vhost_quoted()
        async with http_session.get(
            self.http_api_url.with_path(
                f"/api/bindings/{vhost}/e/{exchange}/q/{queue}/", encoded=True
            )
        ) as response:
            bindings_json = await response.json()
            if not response.ok:
                error = bindings_json.get("error")
                raise RuntimeError(f"RabbitMQ API returned an error: {error}")
            return [binding["routing_key"] for binding in bindings_json]

    async def fetch_bindings(
        self,
        db_token: str,
        *,
        bindings_source: Optional[str] = None,
        data_bindings_source: Optional[str] = None,
        history_bindings_source: Optional[str] = None,
    ) -> Tuple[List[Metric], Optional[List[Metric]]]:
        auth = (
            aiohttp.BasicAuth(
                login=self.http_api_url.user, password=self.http_api_url.password
            )
            if self.http_api_url.user and self.http_api_url.password
            else None
        )

        if bindings_source is not None:
            data_queue = f"{bindings_source}-data"
            hreq_queue = f"{bindings_source}-hreq"
        else:
            data_queue = data_bindings_source or f"{db_token}-data"
            hreq_queue = history_bindings_source or f"{db_token}-hreq"

        async with ClientSession(auth=auth) as session:
            data_bindings_fetch_task = self.fetch_queue_bindings(
                http_session=session,
                exchange=self.data_exchange,
                queue=data_queue,
            )

            if self.declare_history_bindings:
                history_bindings_fetch_task = self.fetch_queue_bindings(
                    http_session=session,
                    exchange=self.history_exchange,
                    queue=hreq_queue,
                )

                return await asyncio.gather(
                    data_bindings_fetch_task, history_bindings_fetch_task
                )
            else:
                return await data_bindings_fetch_task, None

    async def predeclare(
        self,
        db_token: str,
        **kwargs,
    ):
        data_bindings, history_bindings = await self.fetch_bindings(
            db_token=db_token,
            **kwargs,
        )
        logger.info(
            "Database {!r} had {} data and {} history binding(s)",
            db_token,
            len(data_bindings),
            len(history_bindings) if history_bindings is not None else "no",
        )

        if self.dry_run:
            logger.info(
                "Dry-run: would have declared the following for database {!r}:",
                db_token,
            )
            logger.info("Data bindings: {}", data_bindings)
            logger.info("History bindings: {}", history_bindings)
        else:
            logger.info("Pre-declaring queues for database {!r}", db_token)
            await self.queue_manager.db_declare_queues(
                db_token=db_token,
                data_bindings=data_bindings,
                history_bindings=history_bindings,
            )

    async def stop(self, exception):
        if self.couchdb_client:
            await self.couchdb_client.close()
        if self.queue_manager:
            await self.queue_manager.close()
        await super().stop(exception)


def parse_predeclare_args(db_token: str) -> Dict[str, Optional[str]]:
    parts = db_token.split(":", maxsplit=1)
    db_token = parts[0]

    try:
        params = dict([kv.split("=", maxsplit=1)[0:2] for kv in parts[1].split(",")])  # type: ignore

        logger.info(f"{params=}")
        return {
            "db_token": db_token,
            "bindings_source": params.get("from"),
            "data_bindings_source": params.get("data"),
            "history_bindings_source": params.get("hreq"),
        }
    except (KeyError, IndexError):
        return {"db_token": db_token}


@click.command()
@click_log.simple_verbosity_option(logger, default="warning")
@click.option(
    "--server",
    default="amqp://admin:admin@localhost/",
    help="Management address of the MetricQ server",
)
@click.option(
    "--rabbitmq-api",
    default=None,
    help="RabbitMQ API address of the MetricQ server, will be derived from --server if omitted",
)
@click.option(
    "--data-vhost",
    default="/",
    help="The data vhost that database queues should be declared on",
)
@click.option(
    "--data-exchange",
    default="metricq.data",
    help="Data exchange for metric data",
)
@click.option(
    "--history-exchange",
    default="metricq.history",
    help="History exchange for history requests",
)
@click.option(
    "--history-bindings/--no-history-bindings",
    default=True,
    help="Whether to declare history bindings for this database",
)
@click.option(
    "--couchdb-url",
    default="http://localhost:5984",
    help="Address of the configuration backend",
)
@click.option("--couchdb-user", default="admin", help="Configuration backend username")
@click.option(
    "--couchdb-password", default="admin", help="Configuration backend password"
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Do not declare queues, print what would've been done",
)
@click.argument("DB_SPEC", nargs=-1)
def preregister_command(
    server,
    rabbitmq_api,
    db_spec,
    data_vhost,
    data_exchange,
    history_exchange,
    history_bindings: bool,
    couchdb_url,
    couchdb_user,
    couchdb_password,
    dry_run: bool,
):
    """Declare database queues in advance.

    Use this script if you changed the configuration of a database and need to
    restart it with minimal data loss.  This script will declare a database's
    queues with their updated configuration.  They will inherit all bindings
    from the already existing (but outdated) queues and start to receive metric
    data.  After restarting the database, it will be assigned the updated
    queues, discard duplicate metric data and proceed without dataloss.

    DB_SPEC describes for what database to predeclare queues.  It has the form
    of DB_TOKEN[:PARAM=VALUE,...]], where DB_TOKEN is the name of a
    database connected to the network.  Optionally suffix the database token
    with a parameter-value list to control from which queues bindings are inherited:

        * data=...: queue to inherit data bindings from (default: {DB_TOKEN}-data)

        * hreq=...: queue to inherit history requests bindings from (default: {DB_TOKEN}-hreq)

        * from=...:  alternate database token to inherit both data and history bindings from

    Example:

        Pre-declare queues for the database "db-clone", but copy both data and
        history bindings from the database "db-original" instead of "db-clone".

        $ db-predeclare db-clone:from=db-original
    """

    predeclare_args = [parse_predeclare_args(spec) for spec in db_spec]

    async def run():
        couchdb = CouchDB(couchdb_url, user=couchdb_user, password=couchdb_password)

        d = MetricqDbPreregister(
            server=server,
            data_exchange=data_exchange,
            history_exchange=history_exchange,
            declare_history_bindings=history_bindings,
            couchdb=couchdb,
            data_vhost=data_vhost,
            dry_run=dry_run,
            http_api_url=rabbitmq_api,
        )

        try:
            await d.connect()
            await asyncio.gather(*(d.predeclare(**args) for args in predeclare_args))
        except Exception:
            logger.exception("Failed to preregister database queue")
        finally:
            await d.stop(None)

    asyncio.run(run())


if __name__ == "__main__":
    preregister_command()
