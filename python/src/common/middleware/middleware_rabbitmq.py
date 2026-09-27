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
    def __init__(
        self,
        host,
        queue_name,
        prefetch_count=None,
        publisher_confirms=False,
    ):
        self.queue_name = queue_name
        self.consuming = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
        if prefetch_count is not None:
            self.channel.basic_qos(prefetch_count=prefetch_count)
        if publisher_confirms:
            self.channel.confirm_delivery()

    def send(self, message):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            published = self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                mandatory=True,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            if published is False:
                raise MessageMiddlewareMessageError()
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
            return

        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError() from error

    def close(self):
        if self.connection.is_closed:
            return
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError() from error


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(
        self,
        host,
        exchange_name,
        routing_keys,
        prefetch_count=None,
        publisher_confirms=False,
    ):
        if not routing_keys:
            raise ValueError("At least one routing key is required")

        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consuming = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

        for routing_key in routing_keys:
            self.channel.queue_declare(queue=routing_key)
            self.channel.queue_bind(
                exchange=exchange_name,
                queue=routing_key,
                routing_key=routing_key,
            )

        if prefetch_count is not None:
            self.channel.basic_qos(prefetch_count=prefetch_count)
        if publisher_confirms:
            self.channel.confirm_delivery()

    def send(self, message):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        try:
            for routing_key in self.routing_keys:
                published = self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                    ),
                )
                if published is False:
                    raise MessageMiddlewareMessageError()
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def start_consuming(self, on_message_callback):
        if self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

        if len(self.routing_keys) != 1:
            raise MessageMiddlewareMessageError(
                "A consumer must have exactly one routing key"
            )

        queue_name = self.routing_keys[0]

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
            return

        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError() from error

    def close(self):
        if self.connection.is_closed:
            return
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError() from error
