from aio_pika.exchange import ExchangeType
import pytest
from pytest_mock import MockFixture
from unittest.mock import call, create_autospec

from aio_pika import RobustChannel, RobustConnection
from aiocouch.database import Database, NotFoundError
from aiocouch.document import Document

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
    assert manager._channel is None, "Channel was not closed properly"
    data_connection.close.assert_called_once()


async def test_close_open_channel(empty_db):
    async def close():
        pass

    async def channel(self):
        assert False, "No new channel should be opened when an open channel was given"

    data_connection = create_autospec(
        RobustConnection, spec_set=True, close=close, channel=channel
    )
    open_channel = create_autospec(RobustChannel, spec_set=True)
    manager = QueueManager(
        data_connection=data_connection, config_db=empty_db, channel=open_channel
    )

    await manager.ensure_open_channel()
    assert manager._channel is open_channel

    await manager.close()
    open_channel.close.assert_called_once()
    assert manager._channel is None, "Channel was not closed properly"
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

    manager = QueueManager(
        data_connection=data_connection, config_db=empty_db, channel=channel
    )

    # Assert that exchanges are not available before they are declared
    with pytest.raises(RuntimeError):
        manager.data_exchange
    with pytest.raises(RuntimeError):
        manager.history_exchange

    data_exchange, history_exchange = await manager.declare_exchanges(
        data_exchange_name="data", history_exchange_name="history"
    )

    # Check that the exchanges have been declared with the correct parameters
    declare_calls = [
        call.declare_exchange(name="data", durable=True, type=ExchangeType.TOPIC),
        call.declare_exchange(name="history", durable=True, type=ExchangeType.TOPIC),
    ]
    assert all(decl in channel.method_calls for decl in declare_calls)


async def test_declare_sink_queue(data_connection, empty_db):
    async def declare_queue(auto_delete, robust, *_args, **_kwargs):
        assert not robust
        assert auto_delete

    manager = QueueManager(
        data_connection=data_connection,
        config_db=empty_db,
        channel=create_autospec(RobustChannel, declare_queue=declare_queue),
    )

    await manager.declare_sink_data_queue("sink-foo", queue_name=None)


async def test_declare_sink_queue_default_queue_name(data_connection, empty_db):
    QUEUE_NAME_OVERRIDE = "sink-foo-override-data"

    async def declare_queue(queue_name, *_args, **_kwargs):
        assert queue_name == QUEUE_NAME_OVERRIDE

    manager = QueueManager(
        data_connection=data_connection,
        config_db=empty_db,
        channel=create_autospec(RobustChannel, declare_queue=declare_queue),
    )

    await manager.declare_sink_data_queue("sink-foo", queue_name=QUEUE_NAME_OVERRIDE)


async def test_declare_history_queue_no_config(data_connection, empty_db):
    async def declare_queue(*_args, **_kwargs):
        pass

    channel = create_autospec(RobustChannel, declare_queue=declare_queue)

    manager = QueueManager(
        data_connection=data_connection, config_db=empty_db, channel=channel
    )

    await manager.declare_history_response_queue("history-foo")
