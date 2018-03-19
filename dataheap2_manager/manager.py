import json
import os
import uuid
from functools import partial

import click
import click_completion
import click_log

import asyncio
import aio_pika

from .logging import logger

click_completion.init()


class Manager:
    def __init__(self, rpc_url, data_url, rpc_queue_name, data_queue_name, data_exchange, config_path):
        self.rpc_callbacks = {
            'register': self.handle_register,
            'subscribe': self.handle_subscribe,
            'unsubscribe': self.handle_unsubscribe,
            'release': self.handle_release,
        }

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
        with message.process():
            properties = message.properties
            body = message.body
            token = properties.app_id

            if isinstance(body, bytes):
                body = body.decode()
            rpc = json.loads(body)
            logger.info('recieved {} from {}', rpc, token)

            fun = rpc['function']
            ret = self.rpc_callbacks[fun](token, rpc)

            if ret:
                response_function, response = ret
                await exchange.publish(aio_pika.Message(body=json.dumps(response),
                                                        correlation_id=response_function),
                                       routing_key=properties.reply_to)

    def handle_subscribe(self, token, rpc):
        # TODO figure out why auto-assigned queues cannot be used by the client
        frame = self.data_channel.declare_queue('dataheap2.subscription.' + uuid.uuid4().hex)
        queue = frame.method.queue
        logger.debug('declared queue {} for {}', queue, token)
        for rk in rpc['metrics']:
            self.data_channel.queue_bind(exchange=self.data_exchange, queue=queue, routing_key=rk)
        return 'subscribed', {'dataQueue': queue, 'metrics': rpc['metrics']}

    def handle_unsubscribe(self, token, rpc):
        queue = rpc['dataQueue']
        logger.debug('unbinding queue {} for {}', queue, token)
        for rk in rpc['metrics']:
            self.data_channel.queue_unbind(queue, exchange=self.data_exchange, routing_key=rk)

    def handle_release(self, token, rpc):
        queue = rpc['dataQueue']
        logger.debug('releasing {} for {}', queue, token)
        self.data_channel.queue_delete(queue)

    def handle_register(self, token, rpc):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange,
                   "dataQueue": self.data_queue_name,
                   "sourceConfig": self.read_config(token),
                   "sinkConfig": self.read_config(token),
        }
        return 'config', response


@click.command()
@click.argument('rpc-url', default='amqp://localhost/')
@click.argument('data-url', default='amqp://localhost:5672/')
@click.option('--rpc-queue', default='managementQueue')
@click.option('--data-queue', default='dataDrop2')
@click.option('--data-exchange', default='dataRouter')
@click.option('--config-path', default='.', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click_log.simple_verbosity_option(logger)
def manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path):
    loop = asyncio.get_event_loop()
    m = Manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path)
    loop.create_task(m.run(loop))
    logger.info("Running RPC server")
    loop.run_forever()
