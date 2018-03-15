import json
import os
import uuid

import click
import click_completion
import click_log

import pika

from .logging import logger

click_completion.init()


class Manager:
    def __init__(self, rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path):
        self.data_url = data_url
        self.data_queue = data_queue
        self.data_exchange = data_exchange
        self.config_path = config_path

        logger.info("establishing rpc connection to {}", rpc_url)
        self.rpc_connection = pika.BlockingConnection(pika.URLParameters(rpc_url))
        self.rpc_channel = self.rpc_connection.channel()
        # Try to get rid of the nasty peer resetting our connections
        self.rpc_channel.basic_qos(prefetch_count=1)

        logger.info("establishing data connection to {}", data_url)
        self.data_connection = pika.BlockingConnection(pika.URLParameters(data_url))
        self.data_channel = self.data_connection.channel()
        # Try to get rid of the nasty peer resetting our connections
        self.data_channel.basic_qos(prefetch_count=1)

        logger.info("subscribing to rpc queue: {}", rpc_queue)
        self.rpc_channel.queue_declare(queue=rpc_queue)
        self.rpc_channel.basic_consume(self.handle_rpc, queue=rpc_queue, no_ack=False)

        self.data_channel.exchange_declare(exchange=data_exchange, exchange_type='topic')

        # self.data_channel.queue_declare(queue=data_queue)
        # self.data_channel.queue_bind(exchange=data_exchange, queue=data_queue, routing_key='#')

        self.rpc_callbacks = {
            'register': self.handle_register,
            'subscribe': self.handle_subscribe,
            'unsubscribe': self.handle_unsubscribe,
            'release': self.handle_release,
        }

    def run(self):
        logger.info('waiting for messages. to exit press CTRL+C')
        self.rpc_channel.start_consuming()

    def read_config(self, token):
        with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
            return json.load(f)

    def handle_rpc(self, channel, method, properties, body):
        token = properties.app_id
        if isinstance(body, bytes):
            body = body.decode()
        rpc = json.loads(body)
        logger.info('recieved {} from {}', rpc, token)

        fun = rpc['function']
        response_function, response = self.rpc_callbacks[fun](token, rpc)

        if response_function:
            channel.basic_publish(exchange='',
                                  properties=pika.BasicProperties(correlation_id=response_function),
                                  routing_key=properties.reply_to,
                                  body=json.dumps(response))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def handle_subscribe(self, token, rpc):
        # TODO figure out why auto-assigned queues cannot be used by the client
        frame = self.data_channel.queue_declare('dataheap2.subscription.' + uuid.uuid4().hex)
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
        return None, None

    def handle_release(self, token, rpc):
        queue = rpc['dataQueue']
        logger.debug('releasing {} for {}', queue, token)
        self.data_channel.queue_delete(queue)
        return None, None

    def handle_register(self, token, rpc):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange,
                   "dataQueue": self.data_queue,
                   "sourceConfig": self.read_config(token),
                   "sinkConfig": self.read_config(token),
        }
        return 'config', response


def validate_url(ctx, param, value):
    try:
        pika.URLParameters(value)
        return value
    except Exception:
        raise click.BadParameter("Invalid URL")


@click.command()
@click.argument('rpc-url', default='amqp://localhost/', callback=validate_url)
@click.argument('data-url', default='amqp://localhost:5672/', callback=validate_url)
@click.option('--rpc-queue', default='managementQueue')
@click.option('--data-queue', default='dataDrop2')
@click.option('--data-exchange', default='dataRouter')
@click.option('--config-path', default='.', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click_log.simple_verbosity_option(logger)
def manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path):
    while True:
        try:
            m = Manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path)
            m.run()
        except pika.exceptions.ConnectionClosed:
            pass
