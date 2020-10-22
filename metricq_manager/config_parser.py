from contextlib import suppress
from enum import Enum, auto
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar
from uuid import uuid4

from metricq import get_logger

T = TypeVar("T")

logger = get_logger(__name__)

ConfigDict = Dict[str, Any]


class QueueType(Enum):
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


class ConfigParser:
    def __init__(
        self,
        config: ConfigDict,
        role: str,
        client_token: str,
        top_level_key: str = "x-metricq",
    ):
        self.config = config
        self.role = role
        self.client_token = client_token
        self.top_level_key = top_level_key

    def replace(self, role: Optional[str] = None) -> "ConfigParser":
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

        top_level: Optional[ConfigDict] = self.config.get(self.top_level_key)
        if top_level is not None:
            return top_level.get(key, default)
        else:
            return default

    def classic_arguments(self) -> Iterator[Tuple[str, Any]]:
        if (message_ttl := self.message_ttl()) is not None:
            yield ("x-message-ttl", message_ttl)

    def message_ttl(self) -> Optional[int]:
        message_ttl: Any = self.get(
            f"{self.role}-message-ttl", deprecated=["message_ttl"]
        )

        if message_ttl is None:
            return None
        elif isinstance(message_ttl, (float, int)):
            return int(1000 * message_ttl)
        else:
            logger.warning(
                "Client {!r} has message TTL set which is not a number of seconds: got {} of type {!r}",
                self.client_token,
                message_ttl,
                type(message_ttl),
            )
            return None

    def quorum_arguments(self) -> Iterator[Tuple[str, Any]]:
        yield ("x-queue-type", "quorum")

    def arguments(self) -> Iterator[Tuple[str, Any]]:
        queue_type = self.queue_type()
        if queue_type is QueueType.CLASSIC:
            return self.classic_arguments()
        elif queue_type is QueueType.QUORUM:
            return self.quorum_arguments()
        else:
            assert False, f"Unhandled queue type: {queue_type!r}"

    def queue_type(self) -> QueueType:
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
        if default:
            if default.startswith(self.client_token) and default.endswith(self.role):
                return default
            else:
                logger.warning(
                    f"Invalid queue name for client {self.client_token!r}: "
                    f'{default!r} does not match "{self.client_token}[-*]-{self.role}"'
                )
                return default
        elif unique:
            return f"{self.client_token}-{uuid4().hex}-{self.role}"
        else:
            return f"{self.client_token}-{self.role}"
