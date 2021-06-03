# metricq
# Copyright (C) 2021 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
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

from typing import List
from urllib.parse import quote

from aiohttp import BasicAuth, ClientSession
from yarl import URL
import metricq
from metricq.logging import get_logger


from .types import Metric

logger = get_logger(__name__)


class RabbitMQRestAPI:
    def __init__(self, server: URL, data_vhost: str):
        self.http_api_url = (
            URL(server).with_scheme("https" if server.scheme == "amqps" else "http").with_port(15672 if server.port else None)
        )

        self.auth = (
            BasicAuth(
                login=self.http_api_url.user, password=self.http_api_url.password
            )
            if self.http_api_url.user and self.http_api_url.password
            else None
        )

        self.data_vhost = data_vhost

    def data_vhost_quoted(self) -> str:
        return quote(self.data_vhost, safe="")

    async def fetch_queue_bindings(
        self, exchange: str, queue: str
    ) -> List[Metric]:

        vhost = self.data_vhost_quoted()
        get_url = self.http_api_url.with_path(
            f"/api/bindings/{vhost}/e/{exchange}/q/{queue}/", encoded=True
        )
        logger.info(
            "Fetching queue bindings from {!r}",
            get_url.with_user("***").with_password("***").human_repr(),
        )
        async with ClientSession(auth=self.auth) as http_session:
            async with http_session.get(get_url) as response:
                bindings_json = await response.json()
                if not response.ok:
                    error = bindings_json.get("error")
                    raise RuntimeError(f"RabbitMQ API returned an error: {error}")
                return [binding["routing_key"] for binding in bindings_json]
