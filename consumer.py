import time
from RabbitMQClient import RabbitMQClient


def foo(body):
    print("performing long running task...")
    time.sleep(120)
    print("Finished performing task...")


rabbitmq_client = RabbitMQClient(host="localhost", port=5672, user="guest", password="guest", heartbeat=15)

rabbitmq_client.consume_data(queue="test_queue", exchange="test_exchange", consumer_handler=foo)
