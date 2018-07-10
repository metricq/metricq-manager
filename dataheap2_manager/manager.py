import json
import os
import uuid
import logging

import click
import click_completion
import click_log

import asyncio
import aio_pika
import aiomonitor

import cloudant

from dataheap2 import Agent, rpc_handler
from dataheap2.logging import get_logger

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel('INFO')
# Use this if we ever use threads
# logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s %(threadName)-16s %(levelname)-8s %(message)s')
logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')

click_completion.init()


class Manager(Agent):
    def __init__(self, management_url, data_url, config_path, queue_ttl, couchdb_url, couchdb_user, couchdb_password):
        super().__init__('manager', management_url)

        self.management_queue_name = 'management'
        self.management_queue = None

        self.data_connection = None
        self.data_channel = None

        self.data_url = data_url

        self.data_exchange_name = 'dh2.data'
        self.data_exchange = None

        self.history_exchange_name = 'dh2.history'
        self.history_exchange = None

        self.config_path = config_path
        self.queue_ttl = queue_ttl

        self.couchdb_client = cloudant.client.CouchDB(couchdb_user, couchdb_password, url=couchdb_url, connect=True)
        self.couchdb_session = self.couchdb_client.session()
        self.couchdb_db_config = self.couchdb_client.create_database("config")#, throw_on_exists=False)
        self.couchdb_db_metadata = self.couchdb_client.create_database("metadata")

        # TODO if this proves to be reliable, remove the option
        self._subscription_autodelete = True
        # TODO Make some config stuff
        self._expires_seconds = 3600

    async def connect(self):
        await super().connect()

        # TODO persistent?
        self.management_queue = await self._management_channel.declare_queue(self.management_queue_name, durable=True)

        logger.info('creating rpc exchanges')
        self._management_exchange = await self._management_channel.declare_exchange(
            name=self._management_exchange_name, type=aio_pika.ExchangeType.TOPIC, durable=True)
        self._management_broadcast_exchange = await self._management_channel.declare_exchange(
            name=self._management_broadcast_exchange_name, type=aio_pika.ExchangeType.FANOUT, durable=True)

        await self.management_queue.bind(exchange=self._management_exchange, routing_key="#")

        logger.info("establishing data connection to {}", self.data_url)
        self.data_connection = await self._connect(self.data_url)
        self.data_channel = await self.data_connection.channel()

        logger.info("creating data exchanges")
        self.data_exchange = await self.data_channel.declare_exchange(
            name=self.data_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        self.history_exchange = await self.data_channel.declare_exchange(
            name=self.history_exchange_name, type=aio_pika.ExchangeType.TOPIC)

        await self._management_consume([self.management_queue])

    def read_config(self, token):
        try:
            config_document = self.couchdb_db_config[token]
            return dict(config_document)
        except KeyError:
            with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
                return json.load(f)

    async def rpc(self, function, response_callback, to_token=None, **kwargs):
        if to_token:
            kwargs['exchange'] = self._management_channel.default_exchange
            kwargs['routing_key'] = '{}-rpc'.format(to_token)
            kwargs['cleanup_on_response'] = True
        else:
            kwargs['exchange'] = self._management_broadcast_exchange
            kwargs['routing_key'] = function
            kwargs['cleanup_on_response'] = False

        await self._rpc(function, response_callback, **kwargs)

    @rpc_handler('subscribe')
    async def handle_subscribe(self, from_token, **body):
        # TODO figure out why auto-assigned queues cannot be used by the client
        queue_name = 'subscription-' + uuid.uuid4().hex
        logger.debug('attempting to declare queue {} for {}', queue_name, from_token)

        arguments = {}
        try:
            expires_seconds = body['expires']
        except KeyError:
            expires_seconds = self._expires_seconds
        if expires_seconds:
            arguments['x-expires'] = expires_seconds * 1000

        queue = await self.data_channel.declare_queue(queue_name,
                                                      auto_delete=self._subscription_autodelete,
                                                      arguments=arguments)
        logger.debug('declared queue {} for {}', queue, from_token)
        if not body['metrics']:
            # TODO throw some error
            assert False
        await asyncio.wait([queue.bind(exchange=self.data_exchange, routing_key=rk) for rk in body['metrics']],
                           loop=self.event_loop)
        return {'dataQueue': queue.name, 'metrics': body['metrics']}

    @rpc_handler('unsubscribe')
    async def handle_unsubscribe(self, from_token, **body):
        channel = await self.data_connection.channel()
        queue_name = body['dataQueue']
        logger.debug('unbinding queue {} for {}', queue_name, from_token)

        try:
            queue = await channel.declare_queue(queue_name, passive=True)
            assert body['metrics']
            await asyncio.wait([queue.unbind(exchange=self.data_exchange, routing_key=rk) for rk in body['metrics']],
                               loop=self.event_loop)
            await self.data_channel.default_exchange.publish(aio_pika.Message(body=b'', type='end'),
                                                             routing_key=queue_name)
        except aio_pika.exceptions.ChannelClosed as e:
            logger.error('unsubscribe failed, queue timed out: {}', e)
            # Trying to avoid leaking closing futures. Super annoying
            try:
                await channel.closing
            except aio_pika.exceptions.ChannelClosed:
                pass
            raise Exception("queue already timed out")

        self.event_loop.call_soon(channel.close)
        return {'dataServerAddress': self.data_url}

    @rpc_handler('release')
    async def handle_release(self, from_token, **body):
        if self._subscription_autodelete:
            logger.debug('release {} for {} ignored, auto-delete', body['dataQueue'], from_token)
        else:
            logger.debug('releasing {} for {}', body['dataQueue'], from_token)
            queue = await self.data_channel.declare_queue(body['dataQueue'])
            await queue.delete(if_unused=False, if_empty=False)

    @rpc_handler('source.register')
    async def handle_source_register(self, from_token, **body):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange.name,
                   "config": self.read_config(from_token),
        }
        return response

    @rpc_handler('source.metrics_list')
    async def handle_source_metadata(self, from_token, **body):
        if "metrics" not in body:
            return
        for metric in body['metrics']:
            cdb_data = {
                "_id": metric,
            }
            self.couchdb_db_metadata.create_document(cdb_data)

    @rpc_handler('history.get_metric_list')
    async def handle_http_get_metric_list(self, from_token, **body):
        metric_list = self.couchdb_db_metadata.keys(remote=True)
        response = {
                   "metric_list": metric_list,
        }
        return response


    @rpc_handler('db.register')
    async def handle_db_register(self, from_token, **body):
        db_uuid = from_token
        history_queue_name = 'history-' + db_uuid
        logger.debug('attempting to declare queue {} for {}', history_queue_name, from_token)
        history_queue = await self.data_channel.declare_queue(history_queue_name)
        logger.debug('declared queue {} for {}', history_queue, from_token)

        data_queue_name = 'data-' + db_uuid
        logger.debug('attempting to declare queue {} for {}', data_queue_name, from_token)
        data_queue = await self.data_channel.declare_queue(data_queue_name)
        logger.debug('declared queue {} for {}', data_queue, from_token)

        for metric in self.read_config(from_token)['metrics']:
            await history_queue.bind(exchange=self.history_exchange, routing_key=metric['name'])
            await data_queue.bind(exchange=self.data_exchange, routing_key=metric['name'])

        response = {
                   "dataServerAddress": self.data_url,
                   "dataQueue": data_queue_name,
                   "historyQueue": history_queue_name,
                   "config": self.read_config(from_token),
        }
        return response


@click.command()
@click.argument('rpc-url', default='amqp://localhost/')
@click.argument('data-url', default='amqp://localhost:5672/')
@click.option('--queue-ttl', default=30 * 60 * 1000)
@click.option('--config-path', default='.', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--monitor/--no-monitor', default=True)
@click.option('--couchdb-url', default='http://127.0.0.1:5984')
@click.option('--couchdb-user', default='admin')
@click.option('--couchdb-password', default='admin')
@click_log.simple_verbosity_option(logger)
def manager_cmd(rpc_url, data_url, config_path, queue_ttl, monitor, couchdb_url, couchdb_user, couchdb_password):
    manager = Manager(rpc_url, data_url, config_path, queue_ttl, couchdb_url, couchdb_user, couchdb_password)
    if monitor:
        with aiomonitor.start_monitor(manager.event_loop, locals={'manager': manager}):
            manager.run()
    else:
        manager.run()
