#!/usr/bin/env python
import logging
import pika
import random
import datetime
import orjson
import time

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

q_name = "stack-overflow-72792217"

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.queue_declare(queue=q_name)

dataToSend = f"{random.getrandbits(64):=032b}"
count = 0

while True:
    messageToSend = orjson.dumps(
        {"count": count, "data": dataToSend, "timestamp": datetime.datetime.now()}
    )
    count = count + 1
    channel.basic_publish(exchange="", routing_key=q_name, body=messageToSend)
    print("Messages Published: " + str(count))
    time.sleep(2)

connection.close()
