import json
import os
import uuid
from functools import partial
import traceback
import sys

import click
import click_completion
import click_log

import asyncio
import aio_pika
import aiomonitor

from .logging import logger

click_completion.init()


class RPCHandlers:
    rpc_handlers = dict()

    @classmethod
    def register(cls, name):
        def wrapper(func):
            assert asyncio.iscoroutinefunction(func)
            RPCHandlers.rpc_handlers[name] = func
            return func
        return wrapper


class Manager(RPCHandlers):
    def __init__(self, rpc_url, data_url, rpc_queue_name,
                 management_exchange, broadcast_exchange, data_exchange, config_path):
        self.rpc_url = rpc_url
        self.rpc_queue_name = rpc_queue_name
        self.data_url = data_url
        self.management_exchange_name = management_exchange
        self.management_exchange = None
        self.broadcast_exchange_name = broadcast_exchange
        self.broadcast_exchange= None
        self.data_exchange = data_exchange
        self.config_path = config_path

        self.loop = None
        self.rpc_connection = None
        self.rpc_channel = None
        self.rpc_queue = None
        self.rpc_response_queue = None
        self.data_connection = None
        self.data_channel = None
        self.response_handlers = dict()

    async def run(self, loop):
        logger.info("establishing rpc connection to {} / {}", self.rpc_url, self.rpc_queue_name)
        self.rpc_connection = await aio_pika.connect_robust(self.rpc_url, loop=self.loop)
        self.rpc_channel = await self.rpc_connection.channel()
        self.rpc_queue = await self.rpc_channel.declare_queue(self.rpc_queue_name)
        self.rpc_response_queue = await self.rpc_channel.declare_queue(exclusive=True)

        logger.info("creating exchanges")
        self.management_exchange = await self.rpc_channel.declare_exchange(name=self.management_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        self.broadcast_exchange = await self.rpc_channel.declare_exchange(name=self.broadcast_exchange_name, type=aio_pika.ExchangeType.FANOUT)
        await self.rpc_queue.bind(exchange=self.management_exchange, routing_key="#")

        logger.info("establishing data connection to {}", self.data_url)
        self.data_connection = await aio_pika.connect_robust(self.data_url, loop=self.loop)
        self.data_channel = await self.data_connection.channel()
        await self.data_channel.declare_exchange(self.data_exchange, type=aio_pika.ExchangeType.TOPIC)

        consume_rpc = self.rpc_queue.consume(self.handle_rpc)
        consume_rpc_response = self.rpc_response_queue.consume(self.handle_rpc_response)
        await asyncio.wait([consume_rpc, consume_rpc_response])

    def read_config(self, token):
        with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
            return json.load(f)

    async def rpc(self, target, function, body=None, handler=print):
        if body is None:
            body = dict()
        body['function'] = function
        if target:
            exchange = self.rpc_channel.default_exchange
            routing_key = target
        else:
            exchange = self.broadcast_exchange
            routing_key = function

        correlation_id = 'dh2.rpc.{}'.format(uuid.uuid4().hex)
        self.response_handlers[correlation_id] = handler
        logger.info('sending rpc to {} / {}: {}', target, correlation_id, body)
        await exchange.publish(aio_pika.Message(body=json.dumps(body).encode(), correlation_id=correlation_id,
                                                reply_to=self.rpc_response_queue.name, content_type="application/json"),
                               routing_key=routing_key)

    async def handle_rpc(self, message: aio_pika.Message):
        with message.process(requeue=True):
            body_str = message.body.decode()
            token = message.properties.app_id
            logger.info('received request from {}: {}', token, body_str)
            body = json.loads(body_str)
            fun = body['function']

            response = await self.rpc_handlers[fun](self, token, body)

            if response is None:
                response = dict()

            await self.rpc_channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(response).encode(),
                                 correlation_id=message.correlation_id,
                                 content_type="application/json"),
                routing_key=message.properties.reply_to)

    async def handle_rpc_response(self, message: aio_pika.Message):
        with message.process(requeue=True):
            body_str = message.body.decode()
            logger.info('received response from {}: {}', message.properties.app_id, body_str)
            body = json.loads(body_str)
            try:
                handler = self.response_handlers[message.correlation_id.decode()]
            except KeyError:
                logger.warn("discarding unexpected RPC reply from {} / {}: {}",
                            message.properties.app_id, message.correlation_id,message.body)
                return
            handler(message.properties.app_id, body)
            # TODO we should eventually delete the entry eventually ... but if its broadcast response

    @RPCHandlers.register('subscribe')
    async def handle_subscribe(self, token, body):
        # TODO figure out why auto-assigned queues cannot be used by the client
        queue_name = 'subscription-' + uuid.uuid4().hex
        logger.debug('attempting to declare queue {} for {}', queue_name, token)
        queue = await self.data_channel.declare_queue(queue_name)
        logger.debug('declared queue {} for {}', queue, token)
        if not body['metrics']:
            # TODO throw some error
            assert False
        await asyncio.wait([queue.bind(exchange=self.data_exchange, routing_key=rk) for rk in body['metrics']])
        return {'dataQueue': queue.name, 'metrics': body['metrics']}

    @RPCHandlers.register('unsubscribe')
    async def handle_unsubscribe(self, token, body):
        queue_name = body['dataQueue']
        logger.debug('unbinding queue {} for {}', queue_name, token)
        queue = await self.data_channel.declare_queue(queue_name)
        assert body['metrics']
        await asyncio.wait([queue.unbind(exchange=self.data_exchange, routing_key=rk) for rk in body['metrics']])
        await self.data_channel.default_exchange.publish(aio_pika.Message(body=b'', type='end'),
                                                         routing_key=queue_name)
        return {'dataServerAddress': self.data_url}

    @RPCHandlers.register('release')
    async def handle_release(self, token, body):
        logger.debug('releasing {} for {}', body['dataQueue'], token)
        queue = await self.data_channel.declare_queue(body['dataQueue'])
        await queue.delete(if_unused=False, if_empty=False)

    @RPCHandlers.register('source.register')
    async def handle_register(self, token, body):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange,
                   "config": self.read_config(token),
        }
        return response


def panic(loop, context):
    print("EXCEPTION: {}".format(context['message']))
    if context['exception']:
        print(context['exception'])
        traceback.print_tb(context['exception'].__traceback__)
    loop.stop()


@click.command()
@click.argument('rpc-url', default='amqp://localhost/')
@click.argument('data-url', default='amqp://localhost:5672/')
@click.option('--rpc-queue', default='management')
@click.option('--broadcast-exchange', default='dh2.broadcast')
@click.option('--management-exchange', default='dh2.management')
@click.option('--data-exchange', default='dh2.data')
@click.option('--config-path', default='.', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--monitor/--no-monitor', default=True)
@click_log.simple_verbosity_option(logger)
def manager_cmd(rpc_url, data_url, rpc_queue, management_exchange, broadcast_exchange, data_exchange, config_path, monitor):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(panic)
    m = Manager(rpc_url, data_url, rpc_queue, management_exchange, broadcast_exchange, data_exchange, config_path)
    loop.create_task(m.run(loop))
    logger.info("starting management loop")
    if monitor:
        with aiomonitor.start_monitor(loop, locals={'manager': m}):
            loop.run_forever()
    else:
        loop.run_forever()
