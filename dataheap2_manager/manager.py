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
    def __init__(self, rpc_url, data_url, rpc_queue_name, data_queue_name, data_exchange, config_path):
        self.rpc_url = rpc_url
        self.rpc_queue_name = rpc_queue_name
        self.data_url = data_url
        self.data_queue_name = data_queue_name
        self.data_exchange = data_exchange
        self.config_path = config_path

        self.loop = None
        self.rpc_connection = None
        self.rpc_channel = None
        self.rpc_queue = None
        self.data_connection = None
        self.data_channel = None

    async def run(self, loop):
        logger.info("establishing rpc connection to {} / {}", self.rpc_url, self.rpc_queue_name)
        self.rpc_connection = await aio_pika.connect_robust(self.rpc_url, loop=self.loop)
        self.rpc_channel = await self.rpc_connection.channel()
        self.rpc_queue = await self.rpc_channel.declare_queue(self.rpc_queue_name)

        logger.info("establishing data connection to {}", self.data_url)
        self.data_connection = await aio_pika.connect_robust(self.data_url, loop=self.loop)
        self.data_channel = await self.data_connection.channel()
        await self.data_channel.declare_exchange(self.data_exchange, type=aio_pika.ExchangeType.TOPIC)

        await self.rpc_queue.consume(partial(self.handle_rpc, self.rpc_channel.default_exchange))

    def read_config(self, token):
        with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
            return json.load(f)

    async def handle_rpc(self, exchange: aio_pika.Exchange, message : aio_pika.Message):
        with message.process(requeue=True):
            properties = message.properties
            body = message.body
            token = properties.app_id

            if isinstance(body, bytes):
                body = body.decode()
            rpc = json.loads(body)
            logger.info('received {} from {}', rpc, token)

            fun = rpc['function']
            response = await self.rpc_handlers[fun](token, rpc)

            if response:
                await exchange.publish(aio_pika.Message(body=json.dumps(response).encode(),
                                                        correlation_id=message.correlation_id),
                                       routing_key=properties.reply_to)

    @RPCHandlers.register('subscribe')
    async def handle_subscribe(self, token, rpc):
        # TODO figure out why auto-assigned queues cannot be used by the client
        queue = await self.data_channel.declare_queue('dataheap2.subscription.' + uuid.uuid4().hex)
        logger.debug('declared queue {} for {}', queue, token)
        if not rpc['metrics']:
            # TODO throw some error
            assert False
        await asyncio.wait([queue.bind(self.data_exchange, rk) for rk in rpc['metrics']])
        return {'dataQueue': queue.name, 'metrics': rpc['metrics']}

    @RPCHandlers.register('unsubscribe')
    async def handle_unsubscribe(self, token, rpc):
        logger.debug('unbinding queue {} for {}', rpc['dataQueue'], token)
        queue = await self.data_channel.declare_queue(rpc['dataQueue'])
        for rk in rpc['metrics']:
            await queue.unbind(exchange=self.data_exchange, routing_key=rk)

    @RPCHandlers.register('release')
    async def handle_release(self, token, rpc):
        logger.debug('releasing {} for {}', rpc['dataQueue'], token)
        queue = await self.data_channel.declare_queue(rpc['dataQueue'])
        await queue.delete()

    @RPCHandlers.register('register')
    async def handle_register(self, token, rpc):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange,
                   "dataQueue": self.data_queue_name,
                   "sourceConfig": self.read_config(token),
                   "sinkConfig": self.read_config(token),
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
@click.option('--rpc-queue', default='managementQueue')
@click.option('--data-queue', default='dataDrop2')
@click.option('--data-exchange', default='dataRouter')
@click.option('--config-path', default='.', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--monitor/--no-monitor', default=True)
@click_log.simple_verbosity_option(logger)
def manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path, monitor):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(panic)
    m = Manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path)
    loop.create_task(m.run(loop))
    logger.info("starting management loop")
    if monitor:
        with aiomonitor.start_monitor(loop):
            loop.run_forever()
    else:
        loop.run_forever()
