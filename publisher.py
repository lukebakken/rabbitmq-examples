from RabbitMQClient import RabbitMQClient

rabbitmq_client = RabbitMQClient(host="localhost", port=5672, user="guest", password="guest", heartbeat=15)

rabbitmq_client.setup_queues_n_exchanges(queue="test_queue", exchange="test_exchange", empty_queue=False)

rabbitmq_client.publish_data(data="test_data", exchange="test_exchange", routing_key="test_queue")