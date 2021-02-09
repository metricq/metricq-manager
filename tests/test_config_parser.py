import logging
from dataclasses import dataclass
from typing import Any, List, Optional
from uuid import UUID, uuid4

import pytest
from pytest_mock import MockerFixture

from metricq_manager.config_parser import ClientConfig, ConfigParser, QueueType

DEFAULT_CLIENT_TOKEN = "client-test"


@pytest.fixture
def default_config_parser() -> ConfigParser:
    return ConfigParser(config={}, role="test", client_token=DEFAULT_CLIENT_TOKEN)


def test_config_parser_replace_role(default_config_parser: ConfigParser):
    assert default_config_parser.role == "test"

    copy = default_config_parser.replace(role="foo")

    assert copy.role == "foo", "ConfigParser role has not been repleced successfully"
    assert (
        default_config_parser.role == "test"
    ), "Replacing role changed the role of the source object"
    assert copy.client_token == default_config_parser.client_token
    assert copy.config == default_config_parser.config


def x_metricq(config: ClientConfig) -> ClientConfig:
    """Return a client configuration that has the "x-metricq" top-level key set"""
    return {"x-metricq": config}


@dataclass
class _DefaultTestParam:
    config: ClientConfig
    expected: Any
    key: str = "foo"
    default: Optional[str] = None
    deprecated: Optional[List[str]] = None


@pytest.mark.parametrize(
    ("param"),
    [
        # Find key "foo" under top-level key "x-metricq"
        _DefaultTestParam(config=x_metricq({"foo": "bar"}), expected="bar"),
        # Key "foo" is missing; return None
        _DefaultTestParam(config={}, expected=None),
        _DefaultTestParam(config=x_metricq({}), expected=None),
        # Key "foo" is missing; return the given default
        _DefaultTestParam(config={}, default="DEFAULT", expected="DEFAULT"),
        _DefaultTestParam(config=x_metricq({}), default="DEFAULT", expected="DEFAULT"),
        # config contains deprecated key that takes precedence over "x-metricq.foo"
        _DefaultTestParam(
            config={"legacy": "bar", "x-metricq": {"foo": "frob"}},
            deprecated=["legacy"],
            expected="bar",
        ),
        # deprecated key is not present, return the non-deprecated key "x-metricq.foo"
        _DefaultTestParam(
            config=x_metricq({"foo": "bar"}), deprecated=["legacy"], expected="bar"
        ),
    ],
)
def test_config_parser_get_default(param: _DefaultTestParam):
    config_parser = ConfigParser(
        config=param.config, role="test", client_token=DEFAULT_CLIENT_TOKEN
    )

    assert (
        config_parser.get(
            param.key,
            deprecated=param.deprecated,
            default=param.default,
        )
        == param.expected
    )


@pytest.mark.parametrize(
    ("config", "queue_type"),
    [
        # Use default queue type
        (x_metricq({}), QueueType.default()),
        # Explicitly specify the queue type
        (x_metricq({"test-queue-type": "classic"}), QueueType.CLASSIC),
        (x_metricq({"test-queue-type": "quorum"}), QueueType.QUORUM),
        # Ignore queue type for roles other than "test"
        (x_metricq({"OTHER-queue-type": "quorum"}), QueueType.default()),
    ],
)
def test_parse_queue_type(config, queue_type):
    config_parser = ConfigParser(
        config=config, role="test", client_token=DEFAULT_CLIENT_TOKEN
    )

    assert config_parser.queue_type() is queue_type


def test_parse_queue_type_unknown():
    config_parser = ConfigParser(
        config=x_metricq({"test-queue-type": "UNKNOWN"}),
        role="test",
        client_token=DEFAULT_CLIENT_TOKEN,
    )
    with pytest.raises(ValueError):
        config_parser.queue_type()


@pytest.mark.parametrize(
    "config_value",
    [
        "invalid",
        42.0,
        True,
    ],
)
def test_parse_invalid_queue_type(config_value):
    config_parser = ConfigParser(
        config=x_metricq({"test-queue-type": config_value}),
        role="test",
        client_token=DEFAULT_CLIENT_TOKEN,
    )
    with pytest.raises(ValueError):
        config_parser.queue_type()


@pytest.mark.parametrize(
    "queue_type", [queue_type for queue_type in QueueType.__members__.values()]
)
def test_queue_type_to_string_exhaustive(queue_type):
    """Assert that every queue type is covered by `to_string`."""
    assert isinstance(queue_type.to_string(), str)


FIXED_UUID = uuid4()
OVERRIDE_CLIENT_TOKEN = "client-test-foo-bar-override-test"


@pytest.mark.parametrize(
    ("validate", "unique", "queue_name", "queue_type"),
    [
        (OVERRIDE_CLIENT_TOKEN, True, OVERRIDE_CLIENT_TOKEN, QueueType.default()),
        (OVERRIDE_CLIENT_TOKEN, False, OVERRIDE_CLIENT_TOKEN, QueueType.default()),
        (None, False, f"{DEFAULT_CLIENT_TOKEN}-quorum-test", QueueType.QUORUM),
        (
            None,
            True,
            f"{DEFAULT_CLIENT_TOKEN}-{FIXED_UUID.hex}-test",
            QueueType.default(),
        ),
        (None, False, f"{DEFAULT_CLIENT_TOKEN}-quorum-test", QueueType.QUORUM),
        (
            None,
            True,
            f"{DEFAULT_CLIENT_TOKEN}-{FIXED_UUID.hex}-quorum-test",
            QueueType.QUORUM,
        ),
    ],
)
def test_queue_name(
    validate, unique, queue_name, queue_type: QueueType, mocker: MockerFixture
):
    mocker.patch("metricq_manager.config_parser.uuid4", lambda: FIXED_UUID)

    config_parser = ConfigParser(
        config=x_metricq({"test-queue-type": queue_type.to_string()}),
        role="test",
        client_token=DEFAULT_CLIENT_TOKEN,
    )

    assert config_parser.queue_name(unique=unique, validate=validate) == queue_name


def test_queue_name_unique(default_config_parser):
    first_name = default_config_parser.queue_name(unique=True)
    second_name = default_config_parser.queue_name(unique=True)

    assert first_name != second_name


def test_queue_name_fixed(default_config_parser):
    first_name = default_config_parser.queue_name(unique=False)
    second_name = default_config_parser.queue_name(unique=False)

    assert first_name == second_name


@pytest.mark.parametrize(
    "invalid_queue_name",
    [
        # Default does not start with the client token
        "XXX-wrong-prefix-test",
        # Default does not end in queue role
        "client-wrong-suffix-XXX",
    ],
)
def test_queue_name_default_invalid(
    default_config_parser: ConfigParser, invalid_queue_name, caplog
):
    with caplog.at_level(logging.WARNING, logger="metricq_manager.config_parser"):
        default_config_parser.queue_name(validate=invalid_queue_name)
        assert "Invalid queue name" in caplog.text


@pytest.mark.parametrize(
    ("config", "expected_message_ttl"),
    [
        # Default, no TTL set
        ({}, None),
        # Explicit TTL
        (x_metricq({"test-message-ttl": 1}), 1000),
        # Deprecated key, still works
        ({"message_ttl": 1}, 1000),
        # Invalid TTL, ignored
        (x_metricq({"test-message-ttl": "invalid"}), None),
    ],
)
def test_parse_classic_queue_message_ttl(config, expected_message_ttl):
    config_parser = ConfigParser(config, role="test", client_token=DEFAULT_CLIENT_TOKEN)

    assert config_parser.message_ttl() == expected_message_ttl


@pytest.mark.parametrize(
    ("config", "arguments"),
    [
        ({}, {}),
        (x_metricq({"test-message-ttl": 1}), {"x-message-ttl": 1000}),
    ],
)
def test_parse_classic_arguments(config, arguments):
    config_parser = ConfigParser(config, role="test", client_token=DEFAULT_CLIENT_TOKEN)

    assert dict(config_parser.classic_arguments()) == arguments


def test_parse_quorum_queue_arguments():
    config_parser = ConfigParser(
        config=x_metricq({"test-queue-type": "quorum"}),
        role="test",
        client_token=DEFAULT_CLIENT_TOKEN,
    )

    arguments = dict(config_parser.quorum_arguments())
    assert arguments["x-queue-type"] == "quorum"
