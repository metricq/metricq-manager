import json
import os

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

        logger.info("establishing data connection to {}", data_url)
        self.data_connection = pika.BlockingConnection(pika.URLParameters(data_url))
        self.data_channel = self.data_connection.channel()

        logger.info("subscribing to rpc queue: {}", rpc_queue)
        self.rpc_channel.queue_declare(queue=rpc_queue)
        self.rpc_channel.basic_consume(self.handle_rpc, queue=rpc_queue, no_ack=False)

        self.data_channel.queue_declare(queue=data_queue)
        self.data_channel.exchange_declare(exchange=data_exchange, exchange_type='topic')
        self.data_channel.queue_bind(exchange=data_exchange, queue=data_queue, routing_key='#')

        self.rpc_callbacks = {
            'register': self.handle_register,
        }

    def run(self):
        logger.info('waiting for messages. to exit press CTRL+C')
        self.rpc_channel.start_consuming()

    def read_config(self, token):
        with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
            return json.load(f)

    def handle_rpc(self, channel, method, properties, body):
        token = properties.app_id

        rpc = json.loads(body)
        logger.info('recieved {} from {}', rpc, token)

        fun = rpc['function']
        response = self.rpc_callbacks[fun](channel, token, rpc)

        channel.basic_publish(exchange='',
                              properties=pika.BasicProperties(correlation_id='config'),
                              routing_key=properties.reply_to,
                              body=json.dumps(response))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def handle_register(self, token, rpc):
        response = {
                   "dataServerAddress": self.data_url,
                   "dataExchange": self.data_exchange,
                   "dataQueue": self.data_queue,
                   "sourceConfig": self.read_config(token),
                   "sinkConfig": self.read_config(token),
        }
        return response


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
    m = Manager(rpc_url, data_url, rpc_queue, data_queue, data_exchange, config_path)
    m.run()
