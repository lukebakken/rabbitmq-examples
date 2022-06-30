import pika
import requests
import threading
import functools
from typing import Callable


class RabbitMQClient:
    def __init__(self, host: str, port: int, user: str, password: str, heartbeat: int = 60, **kwargs):
        self.__credentials = {"host": host, "port": port, "user": user, "password": password}
        self.heartbeat = heartbeat
        self.__consumer_handler = None
        self.connection, self.channel = self.__create_connection(heartbeat=self.heartbeat, **self.__credentials)
        self.rabbitmq_api_url = "http://localhost:15672"

    def __del__(self):
        try:
            self.connection.close()
        except:
            pass

    # Create new connection
    @staticmethod
    def __create_connection(**kwargs):
        param = pika.ConnectionParameters(kwargs.get("host"), kwargs.get("port"), '/',
                                          pika.PlainCredentials(kwargs.get("user"), kwargs.get("password")),
                                          heartbeat=kwargs.get("heartbeat"))
        conn = pika.BlockingConnection(param)
        return conn, conn.channel()

    def reconnect(self):
        self.connection, self.channel = self.__create_connection(heartbeat=self.heartbeat, **self.__credentials)

    def get_queue_size(self, queue: str) -> int:
        res = requests.get(f"{self.rabbitmq_api_url}/api/queues/%2f/{queue}",
                           auth=("guest", "guest"))
        return res.json()['messages']

    def __setup_dlx_flx(self, queue: str) -> pika.adapters.blocking_connection.BlockingChannel:
        def setup_dlx():
            channel.exchange_declare(exchange="dlx", exchange_type='direct')
            channel.queue_declare(queue=f"dlq_{queue}",
                                  arguments={
                                      'x-message-ttl': 3000,
                                      'x-dead-letter-exchange': "test_exchange",
                                  })

            channel.queue_bind(exchange="dlx",
                               queue=f"dlq_{queue}",
                               routing_key=queue)

        def setup_flx():
            channel.exchange_declare(exchange="flx", exchange_type='direct')
            channel.queue_declare(queue=f"flq_{queue}", durable=True)

            channel.queue_bind(exchange="flx",
                               queue=f"flq_{queue}",
                               routing_key=queue)

        connection = self.connection
        channel = connection.channel()
        setup_dlx()
        setup_flx()
        return channel

    def setup_queues_n_exchanges(self, queue: str, exchange: str, empty_queue: bool = False):
        if self.channel.is_closed:
            self.connection, self.channel = self.__create_connection(heartbeat=self.heartbeat, **self.__credentials)
        channel = self.channel
        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        channel.queue_declare(queue=queue, durable=True,
                              arguments={'x-dead-letter-exchange': 'dlx'})
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=queue)
        if empty_queue:
            requests.delete(f"{self.rabbitmq_api_url}/api/queues/%2f/{queue}/contents",
                            auth=("guest", "guest"))

    def publish_data(self, data, routing_key: str, exchange: str):
        if self.channel.is_closed:
            self.connection, self.channel = self.__create_connection(heartbeat=self.heartbeat, **self.__credentials)
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=data,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))

    @staticmethod
    def __ack_message(channel, delivery_tag, ack):
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            if ack:
                channel.basic_ack(delivery_tag)
            else:
                channel.basic_nack(delivery_tag, requeue=False)
        else:
            pass

    def __on_message(self, channel, method_frame, properties, body, args):
        def do_work():
            print("Received and working...")
            delivery_tag, binding_key = method_frame.delivery_tag, method_frame.routing_key
            ack = True
            try:
                self.__consumer_handler(body)
            except Exception as e:
                print(f"Could not process consumed message: {e}")
                if properties.headers and properties.headers.get('x-death')[0]['count'] >= 2:
                    print("Consuming failed after retrying 2 times. Moving to failed queue..")
                    cb = functools.partial(self.publish_data, body,
                                           binding_key, "flx")
                    connection.add_callback_threadsafe(cb)
                else:
                    print("requeuing...")
                    ack = False
            cb = functools.partial(self.__ack_message, channel, delivery_tag, ack)
            connection.add_callback_threadsafe(cb)

        (connection, threads) = args
        t = threading.Thread(target=do_work)
        t.start()
        threads.append(t)

    def consume_data(self, queue: str, exchange: str, consumer_handler: Callable):
        channel = self.channel
        self.__consumer_handler = consumer_handler
        self.__setup_dlx_flx(queue=queue)
        channel.exchange_declare(exchange=exchange, exchange_type='direct')
        # This method creates or checks a queue and configures a dead letter exchange queue to it.
        # We may also specify a routing key to be used when dead-lettering messages.
        # If this is not set, the message's own routing keys will be used.
        channel.queue_declare(queue=queue, durable=True,
                              arguments={'x-dead-letter-exchange': 'dlx'})

        # Binds the queue to the specified exchange
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=queue)
        channel.basic_qos(prefetch_count=1)
        threads = []
        on_message_callback = functools.partial(self.__on_message, args=(self.connection, threads))
        channel.basic_consume(queue=queue, on_message_callback=on_message_callback, auto_ack=False)

        try:
            print(f' [*] Waiting for messages from queue: {queue}. To exit press CTRL+C')
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        # Wait for all to complete
        for thread in threads:
            thread.join()
