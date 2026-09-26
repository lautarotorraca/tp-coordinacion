import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


def _raise_operation_error(error):
    if isinstance(error, pika.exceptions.AMQPConnectionError):
        raise MessageMiddlewareDisconnectedError() from error
    raise MessageMiddlewareMessageError() from error


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.consuming = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def send(self, message):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def start_consuming(self, on_message_callback):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        def callback(channel, method, properties, body):
            tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=tag)

            def nack():
                channel.basic_nack(delivery_tag=tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self.consuming = True
            self.channel.start_consuming()
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError() from error

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError() from error


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consuming = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    def send(self, message):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def start_consuming(self, on_message_callback):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

        def callback(channel, method, properties, body):
            tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=tag)

            def nack():
                channel.basic_nack(delivery_tag=tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self.consuming = True
            self.channel.start_consuming()
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError() from error

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError() from error
