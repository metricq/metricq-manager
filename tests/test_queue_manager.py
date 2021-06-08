from unittest.mock import AsyncMock, call, create_autospec

import pytest
from aio_pika import RobustChannel, RobustConnection
from aio_pika.exchange import ExchangeType
from aiocouch.database import Database, NotFoundError
from aiocouch.document import Document
from pytest_mock import MockFixture

from metricq_manager.queue_manager import QueueManager

pytestmark = pytest.mark.asyncio


def mock_db(config=None):
    config = config or {}

    async def get(self, key):
        try:
            return create_autospec(Document, spec_set=True, data=config[key])
        except KeyError:
            raise NotFoundError()

    return create_autospec(Database, spec_set=True, __getitem__=get)


@pytest.fixture
def empty_db() -> Database:
    return mock_db()


@pytest.fixture
def data_connection() -> RobustConnection:
    return create_autospec(RobustConnection, spec_set=True)


async def test_close(empty_db):
    async def close():
        pass

    async def channel(self):
        assert (
            False
        ), "No channel should be opened unless QueueManager.channel() is called"

    data_connection = create_autospec(
        RobustConnection, spec_set=True, close=close, channel=channel
    )
    manager = QueueManager(data_connection=data_connection, config_db=empty_db)

    await manager.close()
    data_connection.close.assert_called_once()


async def test_read_config(data_connection, mocker: MockFixture):
    CONFIG = mocker.sentinel.CONFIG
    manager = QueueManager(
        data_connection=data_connection, config_db=mock_db({"foo": CONFIG})
    )

    assert (await manager.read_config("foo", role="test")).config is CONFIG


async def test_read_config_missing(data_connection, empty_db):
    manager = QueueManager(data_connection=data_connection, config_db=empty_db)

    with pytest.raises(KeyError):
        await manager.read_config("does-not-exist-in-db", role="test")


async def test_read_config_allow_missing(data_connection, empty_db):
    manager = QueueManager(data_connection=data_connection, config_db=empty_db)

    assert (
        await manager.read_config(
            "does-not-exist-in-db", role="test", allow_missing=True
        )
    ).config == {}


async def test_declare_exchanges(data_connection, empty_db):
    channel = create_autospec(RobustChannel, spec_set=True)
    data_connection.channel = AsyncMock(return_value=channel)

    manager = QueueManager(data_connection=data_connection, config_db=empty_db)

    # Assert that exchanges are not available before they are declared
    with pytest.raises(RuntimeError):
        manager.data_exchange
    with pytest.raises(RuntimeError):
        manager.history_exchange

    data_exchange, history_exchange = await manager.declare_exchanges(
        data_exchange_name="data", history_exchange_name="history"
    )

    # Check that the exchanges have been declared with the correct parameters
    for declare_call in (
        call.declare_exchange(name="data", durable=True, type=ExchangeType.TOPIC),
        call.declare_exchange(name="history", durable=True, type=ExchangeType.TOPIC),
    ):
        assert declare_call in channel.method_calls


async def test_declare_sink_queue(data_connection, empty_db):
    channel = create_autospec(RobustChannel, spec_set=True)

    data_connection.attach_mock(AsyncMock(return_value=channel), "channel")

    manager = QueueManager(
        data_connection=data_connection,
        config_db=empty_db,
    )

    await manager.sink_declare_data_queue("sink-foo", queue_name=None)

    args, kwargs = channel.declare_queue.call_args
    assert not kwargs["robust"]
    assert kwargs["auto_delete"]
    assert kwargs["arguments"]["x-expires"] == QueueManager.DEFAULT_SINK_DATA_QUEUE_TTL


async def test_declare_sink_queue_default_queue_name(data_connection, empty_db):
    channel = create_autospec(RobustChannel)
    data_connection.attach_mock(
        AsyncMock(return_value=channel),
        "channel",
    )

    manager = QueueManager(
        data_connection=data_connection,
        config_db=empty_db,
    )

    QUEUE_NAME_OVERRIDE = "sink-foo-override-data"
    await manager.sink_declare_data_queue("sink-foo", queue_name=QUEUE_NAME_OVERRIDE)

    args, kwargs = channel.declare_queue.call_args
    assert QUEUE_NAME_OVERRIDE in args[:1] or kwargs["name"] == QUEUE_NAME_OVERRIDE


async def test_declare_history_queue_no_config(data_connection, empty_db):
    channel = create_autospec(RobustChannel, spec_set=True)

    data_connection.attach_mock(AsyncMock(return_value=channel), "channel")

    manager = QueueManager(data_connection=data_connection, config_db=empty_db)

    await manager.history_declare_response_queue("history-foo")

    # Make sure history response queues expire by default
    args, kwargs = channel.declare_queue.call_args
    assert (
        kwargs["arguments"]["x-expires"]
        == QueueManager.DEFAULT_HISTORY_RESPONSE_QUEUE_TTL
    )


async def test_declare_transformer_queue_no_config(data_connection, empty_db):
    channel = create_autospec(RobustChannel, spec_set=True)

    data_connection.attach_mock(AsyncMock(return_value=channel), "channel")

    manager = QueueManager(
        data_connection=data_connection, config_db=mock_db({"transformer-foo": {}})
    )

    await manager.transformer_declare_data_queue("transformer-foo")

    assert channel.declare_queue.await_args.kwargs["durable"]
