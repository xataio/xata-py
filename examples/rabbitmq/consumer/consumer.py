#
# Licensed to Xatabase, Inc under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Xatabase, Inc licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You
# may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json
import logging
import os
import sys

import pika

from xata.client import XataClient
from xata.helpers import BulkProcessor

logging.basicConfig(format="%(asctime)s [%(process)d] %(levelname)s: %(message)s", level=logging.INFO)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "task_queue")

# setup Xata
xata = XataClient(db_name=os.environ.get("XATA_DB_NAME"))
bp = BulkProcessor(
    xata,
    thread_pool_size=int(os.environ.get("XATA_BP_THREADS")),
    flush_interval=int(os.environ.get("XATA_BP_FLUSH_INTERVAL")),
    batch_size=int(os.environ.get("XATA_BP_BATCH_SIZE")),
)


def callback(ch, method, props, body):
    msg = json.loads(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)
    bp.put_record(msg["table"], msg["record"])

    logging.debug("received message: %s" % msg)


if __name__ == "__main__":
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
        logging.info("ready to start ingesting ..")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("done.")
        connection.close()
        bp.flush_queue()
        sys.exit(0)
