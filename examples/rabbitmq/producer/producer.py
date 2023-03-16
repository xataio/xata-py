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
import random
import sys
import time
from datetime import datetime, timezone
from random import randrange

import pika
from faker import Faker

logging.basicConfig(format="%(asctime)s [%(process)d] %(levelname)s: %(message)s", level=logging.INFO)

N_COMPANIES = int(os.environ.get("N_COMPANIES", 25))
N_TICK_INTERVAL = int(os.environ.get("N_TICK_INTERVAL", 15))
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "task_queue")
EXCHANGES = ["DAX", "DOW-JONES", "NASDAQ", "FTSE", "DAX", "SSX"]

fake = Faker()

# setup RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


def main():
    # Generate company symbols
    logging.info("generating %d companies.", N_COMPANIES)
    companies = {}
    while len(companies.keys()) < N_COMPANIES:
        name = fake.company()
        symbol = name[0 : randrange(2, 5)].upper()
        if symbol.isalnum() and symbol not in companies:
            companies[symbol] = {
                "id": symbol,
                "name": name,
                "address": fake.address(),
                "catch_phrase": fake.catch_phrase(),
                "ceo": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.company_email(),
                "exchange": random.choice(EXCHANGES),
            }

            msg = json.dumps({"table": "companies", "record": companies[symbol]})
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=msg,
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
            )
    # wait for consistency
    time.sleep(N_TICK_INTERVAL)

    # Init Prices
    logging.info("generate baseline for individual stock prices.")
    prices = {}
    for key in companies.keys():
        prices[key] = {
            "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
            "symbol": key,
            "price": round(randrange(100, 9999) / randrange(9, 99), 3),
            "delta": 0.0,
            "percentage": 0.0,
        }

        msg = json.dumps({"table": "prices", "record": prices[key]})
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=msg,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
        )

    # Move prices
    while True:
        time.sleep(N_TICK_INTERVAL)
        logging.info("generating price movements.")
        for key in companies.keys():
            c = (randrange(1, 111) / 100) * (-1 if fake.boolean(chance_of_getting_true=25) else 1)
            previous_price = float(prices[key]["price"])
            p = previous_price + ((previous_price / 100) * c)
            d = p - previous_price
            prices[key] = {
                "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
                "symbol": key,
                "price": round(p, 3),
                "delta": round(d, 3),
                "percentage": c,
            }

            msg = json.dumps({"table": "prices", "record": prices[key]})
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=msg,
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("done.")
        connection.close()
        sys.exit(0)
