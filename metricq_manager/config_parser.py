# Copyright (C) 2020, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# SPDX-License-Identifier: GPL-3.0-or-later

"""In order to correctly manage and allocate resources on the MetricQ network,
the manager sometimes needs per-client information.  This "client configuration"
is stored in a CouchDB backend, in the database :literal:`"config"`.

For each client, the configuration sits in the same document as the usual
client configuration (as returned by the `config` RPC), but under the special key
`x-metricq`:

.. code-block:: json

    {
        "foo": "bar",
        "baz": 1,
        "x-metricq": { "data-queue-type": "quorum" }
    }

A :class:`ConfigParser` allows to parse such a client configuration and gives
structured access to the relevant queue-specific configuration keys.
In order to create a new :class:`ConfigParser`:
Since a :class:`ConfigParser` is to be used when declaring a specific queue for
client, a so-called "queue role" has to be supplied: it namespaces different
configuration keys based on the intended role of this queue.

Currently, queue roles with defined meaning are:

    * :literal:`"data"`: data queues
    * :literal:`"hreq"`: history request queues
    * :literal:`"hrsp"`: history response queues

For example, the "queue type" (as given by the AMQP queue argument
:literal:`"x-queue-type"`) is retrieved from the key
:literal:`x-metricq.{role}-queue-type`.

This makes it possible the make only the data queue of a database a "quorum"
queue, leaving the history request queue as a "classic" queue by setting

.. code-block:: json

    {
        "x-metricq": { "data-queue-type": "quorum" }
    }

in the client configuration.

Some configuration keys are interdependent.
Most notably, :literal:`x-metricq.{role}-message-ttl` is ignored if the queue
type :literal:`"quorum"` is requested.
See the document of :meth:`ConfigParser.arguments` below.

Currently, the following configuration keys can be accessed:

    :literal:`x-metricq.{role}-queue-type`:
        The intended AMQP queue type, see :meth:`ConfigParser.queue_type`.

    :literal:`x-metricq.{role}-message-ttl` `(classic queues only)`:
        The number of seconds after which unconsumed messages will be deleted
        from this queue.

If you intended on adding more keys to be parsed, please consider the following:

    * Add a method that parses value of this configuration key.
        If the key is not set, either return a sensible default or :code:`None`
        to mark its absence.
    * Parse the value *completely*.
        Either handle all edge-cases and raise :exc:`ValueError` and
        :exc:`TypeError` as appropriate, or log and ignore invalid values and
        treat the key as unset.

        Values returned by your method should be usable as queue-argument
        as-is, no futher preparation required.

        A good example for this is :meth:`ConfigParser.message_ttl`.
        It multiplies the value of :literal:`x-metricq.{role}-message-ttl` by
        1000 before returning it, since in the client configuration it is
        stored as a number of seconds, but the queue argument
        :literal:`x-message-ttl` expects a number of milliseconds.
        If the value is either not a number or a or it is a negative number,
        it will be ignored and :code:`None` is returned.
    * Make sure the queue argument is compatible with the intended queue type.
        Some arguments cannot be set for all queue types.
        Add it to the iterators returned by :meth:`ConfigParser.classic_arguments`
        and :meth:`ConfigParser.quorum_arguments` depending on that.
"""

from contextlib import suppress
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar
from uuid import uuid4

from metricq import get_logger

T = TypeVar("T")

logger = get_logger(__name__)

ClientConfig = Dict[str, Any]
"""A client configuration object as retrieved from the database.
"""


class QueueType(Enum):
    """The AMQP queue types known to the manager.

    By default, queues are declared as "classic" queues.
    If desired, "quorum" queues offer higher data safety.
    """

    CLASSIC = auto()
    QUORUM = auto()

    @staticmethod
    def default() -> "QueueType":
        return QueueType.CLASSIC

    @staticmethod
    def from_str(queue_type: str) -> "QueueType":
        if queue_type == "classic":
            return QueueType.CLASSIC
        elif queue_type == "quorum":
            return QueueType.QUORUM
        else:
            raise ValueError(f"Invalid queue type {queue_type!r}")

    def to_string(self) -> str:
        if self is QueueType.CLASSIC:
            return "classic"
        elif self is QueueType.QUORUM:
            return "quorum"
        else:
            assert False, f"Invalid QueueType {self!r}"


class ConfigParser:
    """High-level structured access to per-client queue configuration.

    Args:
        config:
            A client configuration object.
        role:
            The intended queue role.
        client_token:
            The client's token.
        top_level_key:
            The top-level key under which manager-specific configuration is found.
    """

    def __init__(
        self,
        config: ClientConfig,
        role: str,
        client_token: str,
        top_level_key: str = "x-metricq",
    ):
        self.config = config
        self.role = role
        self.client_token = client_token
        self.top_level_key = top_level_key

    def replace(self, role: Optional[str] = None) -> "ConfigParser":
        """Return a copy of this ConfigParser, with the role replaced."""
        role = self.role if role is None else role
        return ConfigParser(
            config=self.config,
            role=role,
            client_token=self.client_token,
            top_level_key=self.top_level_key,
        )

    def get(
        self,
        key: str,
        *,
        deprecated: Optional[List[str]] = None,
        default: Optional[T] = None,
    ) -> Optional[T]:
        """Retrieve a configuration value by key, optionally falling back to a default value.

        Args:
            key:
                The configuration key whose value to retrieve.
                This is namespaced under the top-level key, so
                :code:`config.get("foo")` retrieves from :literal:`"{config.top_level_key}.foo"`.
            deprecated:
                A list of `legacy` keys to try `before` accessing the given key.

                Note:
                    These keys are not namespaced under the top-level key.
                    :code:`config.get("foo", deprecated=["frob"])`
                    retrieves from :literal:`frob`, *not*
                    :literal:`{config.top_level_key}.frob`.
            default:
                A default value to be returned if the key is not set in the configuration.
        """
        if deprecated:
            for deprecated_key in deprecated:
                with suppress(KeyError):
                    value = self.config[deprecated_key]
                    logger.warning(
                        'Client configuration for {!r} has legacy key {!r} set, use "{}.{}" instead!',
                        self.client_token,
                        deprecated_key,
                        self.top_level_key,
                        key,
                    )
                    return value

        top_level: Optional[ClientConfig] = self.config.get(self.top_level_key)
        if top_level is not None:
            return top_level.get(key, default)
        else:
            return default

    def classic_arguments(self) -> Iterator[Tuple[str, Any]]:
        """An iterator over `key-value` pairs of arguments for queues of type
        :literal:`"classic"`, as parsed from the configuration object.
        """
        if (message_ttl := self.message_ttl()) is not None:
            yield ("x-message-ttl", message_ttl)

    def message_ttl(self) -> Optional[int]:
        """Parse message `time-to-live <https://www.rabbitmq.com/ttl.html#queue-ttl>`_
        argument for messages in a classic queue.

        Does not apply to quorum queues.
        """
        message_ttl: Any = self.get(
            f"{self.role}-message-ttl", deprecated=["message_ttl"]
        )

        if message_ttl is None:
            return None
        elif isinstance(message_ttl, (float, int)):
            ttl = int(1000 * message_ttl)

            if ttl >= 0:
                return ttl
            else:
                logger.warning(
                    "Client {!r} has per-queue message TTL that is not a positive number of seconds (got {})",
                    self.client_token,
                    ttl,
                )
                return None
        else:
            logger.warning(
                "Client {!r} has message TTL set which is not a number of seconds: got {} of type {!r}",
                self.client_token,
                message_ttl,
                type(message_ttl),
            )
            return None

    def quorum_arguments(self) -> Iterator[Tuple[str, Any]]:
        """An iterator over `key-value` pairs of arguments for queues of type
        :literal:`"quorum"`, as parsed from the configuration object.
        """
        yield ("x-queue-type", "quorum")

    def arguments(self) -> Iterator[Tuple[str, Any]]:
        """An iterator over `key-value` pairs of arguments for queues, as
        parsed from the configuration object.

        Depending on the queue type set in :literal:`{top_level_key}.{role}-queue-type`,
        this yields either "classic" queue arguments
        (:meth:`classic_arguments`) or "quorum" queue arguments
        (:meth:`quorum_arguments`).
        """
        queue_type = self.queue_type()
        if queue_type is QueueType.CLASSIC:
            return self.classic_arguments()
        elif queue_type is QueueType.QUORUM:
            return self.quorum_arguments()
        else:
            assert False, f"Unhandled queue type: {queue_type!r}"

    def queue_type(self) -> QueueType:
        """Parse the requested queue type from a configuration."""
        queue_type = self.get(f"{self.role}-queue-type")
        if queue_type is None:
            return QueueType.default()
        elif isinstance(queue_type, str):
            return QueueType.from_str(queue_type)
        else:
            raise ValueError(f"Queue type for {self.client_token!r} must be a string")

    def queue_name(
        self,
        *,
        unique: bool = True,
        default: Optional[str] = None,
    ) -> str:
        """Return a suitable name for the queue under construction.

        All queue names returned from this method are in the form of
        :literal:`{self.client_token}[-*]-{self.role}`.

        Args:
            unique:
                If set, the queue name will contain a random string of letters,
                unique for each call of this method.
            default:
                If set, the given name will be checked against the format mentioned above.
                If it matches, it is returned as-is, otherwise a :exc:`ValueError` is raised.

        Returns:
            The formatted queue name.
        """
        if default:
            if default.startswith(self.client_token) and default.endswith(self.role):
                return default
            else:
                logger.warning(
                    f"Invalid queue name for client {self.client_token!r}: "
                    f'{default!r} does not match "{self.client_token}[-*]-{self.role}"'
                )
                return default
        else:
            return "-".join(self._queue_name_parts(unique=unique))

    def _queue_name_parts(self, unique: bool):
        yield self.client_token
        if unique:
            yield uuid4().hex

        # When changing the queue type of a client queue, its next declaration is
        # going to fail with error code 406 (Precondition failed), since it does
        # not match the arguments of the existing queue.
        #
        # We include the queue type in the queue name to sidestep the problem.
        # Changing the queue type then results in a new queue (with a predictable
        # name) being declared and no conflicts arise.  A client re-registering
        # itself with a changed queue type then gets assigned the newly declared
        # queue.  Of course, the old queue still exists and needs to be deleted
        # manually.
        #
        # For backwards compatibility, we only include the queue type if it is
        # different from the default queue type, which in the past was only
        # available queue type.  This way, all clients that do not declare a
        # special queue type keep the old queue names.
        queue_type = self.queue_type()
        if queue_type != QueueType.default():
            yield self.queue_type().to_string()

        yield self.role
