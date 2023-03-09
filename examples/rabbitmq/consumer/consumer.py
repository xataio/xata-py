#!/usr/bin/env python


import os
import logging
import pika
import json
import sys

from xata.client import XataClient
from xata.helpers import BulkProcessor

logging.basicConfig(format='%(asctime)s [%(process)d] %(levelname)s: %(message)s', level=logging.INFO)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE', 'task_queue')

# setup Xata
xata = XataClient(db_name=os.environ.get('XATA_DB_NAME'))
bp = BulkProcessor(
    xata, 
    thread_pool_size=int(os.environ.get('XATA_BP_THREADS')),
    flush_interval=int(os.environ.get('XATA_BP_FLUSH_INTERVAL')),
    batch_size=int(os.environ.get('XATA_BP_BATCH_SIZE')),
)

def callback(ch, method, props, body):
    msg = json.loads(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)
    bp.put_record(msg["table"], msg["record"])

    logging.debug("received message: %s" % msg)

if __name__ == '__main__':
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
        logging.info("ready to start ingesting ..")
        channel.start_consuming()
    except KeyboardInterrupt:
        print('done.')
        connection.close()
        bp.flush_queue()
        sys.exit(0)
