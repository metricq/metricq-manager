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
import logging

import click

import aiomonitor
import click_completion
import click_log
from metricq.logging import get_logger

from .manager import Manager

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel("INFO")
# Use this if we ever use threads
# logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s %(threadName)-16s %(levelname)-8s %(message)s')
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)

click_completion.init()


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
