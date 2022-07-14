# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import threading
import time
import pika
from pika.exchange_type import ExchangeType

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

q_name = "stack-overflow-72792217"


def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if ch.is_open:
        ch.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass


def do_work(ch, delivery_tag, body, threads):
    thread_id = threading.get_ident()
    LOGGER.info(
        "Thread id: %s Delivery tag: %s Message body: %s", thread_id, delivery_tag, body
    )
    # Sleeping to simulate 10 seconds of work
    time.sleep(10)
    cb = functools.partial(ack_message, ch, delivery_tag)
    ch.connection.add_callback_threadsafe(cb)
    del threads[delivery_tag]


def on_message(ch, method, body, threads):
    delivery_tag = method.delivery_tag
    t = threading.Thread(target=do_work, args=(ch, delivery_tag, body, threads))
    t.start()
    threads[delivery_tag] = t


credentials = pika.PlainCredentials("guest", "guest")
# Note: setting a short heartbeat to prove that heartbeats are still
# sent even though the worker simulates long-running work
parameters = pika.ConnectionParameters(
    "localhost", credentials=credentials, heartbeat=5
)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()
channel.queue_declare(queue=q_name, auto_delete=False)
channel.basic_qos(prefetch_count=1)

threads = {}
maybe_running_threads = []
try:
    # Note: this is 5 seconds, not 5 minutes :-)
    for method, properties, body in channel.consume(q_name, inactivity_timeout=5):
        if (method, properties, body) == (None, None, None):
            maybe_running_threads = list(threads.values())
            maybe_running_threads_len = len(maybe_running_threads)
            LOGGER.info("reached inactivity timeout: %d", maybe_running_threads_len)
            if maybe_running_threads_len == 0:
                LOGGER.info("reached inactivity timeout and no threads are running, exiting!")
                break
        else:
            on_message(channel, method, body, threads)
    else:
        # Iterator stopped
        LOGGER.warning("iterator stopped unexpectedly!")
except KeyboardInterrupt:
    channel.stop_consuming()

# Wait for all to complete, just in case any are running
for thread in maybe_running_threads:
    thread.join()

connection.close()
