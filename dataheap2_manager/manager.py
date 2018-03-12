import click
import json
import pika

RABBITMQ_RPC_URI = "amqp://localhost/"
RABBITMQ_DATA_URI = "amqp://localhost:5672/"

RABBITMQ_RPC_QUEUE = "managementQueue"
RABBITMQ_DATA_QUEUE = "dataDrop2"

RABBITMQ_DATA_EXCHANGE = "dataRouter"


def read_config(token):
    with open(token + ".json", 'r') as f:
        return json.load(f)


def rpc_callback(ch, method, properties, body):
    token = properties.app_id

    response = dict()
    response["dataServerAddress"] = RABBITMQ_DATA_URI
    response["dataExchange"] = RABBITMQ_DATA_EXCHANGE
    response["dataQueue"] = RABBITMQ_DATA_QUEUE
    response["sourceConfig"] = read_config(token)
    response["sinkConfig"] = read_config(token)

    ch.basic_publish(exchange='',
                     properties=pika.BasicProperties(correlation_id='config'),
                     routing_key=properties.reply_to,
                     body=json.dumps(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(" [x] Received {} from {}".format(body, token))


@click.command()
def manager():
    rpc_connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_RPC_URI))
    rpc_channel = rpc_connection.channel()

    data_connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_DATA_URI))
    data_channel = data_connection.channel()

    rpc_channel.queue_declare(queue=RABBITMQ_RPC_QUEUE)
    rpc_channel.basic_consume(rpc_callback,
                              queue=RABBITMQ_RPC_QUEUE,
                              no_ack=False)

    data_channel.queue_declare(queue=RABBITMQ_DATA_QUEUE)
    data_channel.exchange_declare(exchange=RABBITMQ_DATA_EXCHANGE,
                                  exchange_type='topic')
    data_channel.queue_bind(exchange=RABBITMQ_DATA_EXCHANGE,
                            queue=RABBITMQ_DATA_QUEUE,
                            routing_key='#')

    print(' [*] Waiting for messages. To exit press CTRL+C')
    rpc_channel.start_consuming()
