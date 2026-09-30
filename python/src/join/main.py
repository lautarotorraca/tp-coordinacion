import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])
CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
CONTROL_ROUTES = [f"{SUM_PREFIX}_{sum_id}" for sum_id in range(SUM_AMOUNT)]


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, publisher_confirms=True
        )
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            CONTROL_EXCHANGE,
            CONTROL_ROUTES,
            publisher_confirms=True,
        )
        self.states = {}
        self.completed_queries = set()

    def _get_state(self, query_id):
        """Busca los tops parciales recibidos para una consulta."""

        if query_id not in self.states:
            self.states[query_id] = {}
        return self.states[query_id]

    def _validate_partial_top(self, message):
        """Valida los campos que Join necesita para reunir un top parcial."""

        if message.get("type") != message_protocol.internal.MsgType.PARTIAL_TOP:
            raise ValueError("Unknown message type")

        query_id = message.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            raise ValueError("Invalid query ID")

        aggregation_id = message.get("aggregation_id")
        if not isinstance(aggregation_id, int) or not (
            0 <= aggregation_id < AGGREGATION_AMOUNT
        ):
            raise ValueError("Invalid aggregation ID")

        items = message.get("items")
        if not isinstance(items, list) or len(items) > TOP_SIZE:
            raise ValueError("Invalid partial top")

        return query_id, aggregation_id, items

    def _build_final_top(self, partial_tops):
        """Ordena los candidatos de Aggregation y arma el top final."""

        candidates = []
        seen_fruits = set()
        for items in partial_tops.values():
            for fruit, amount in items:
                if fruit in seen_fruits:
                    raise ValueError("Fruit received from multiple partitions")
                seen_fruits.add(fruit)
                candidates.append(fruit_item.FruitItem(fruit, int(amount)))

        candidates.sort(reverse=True)
        return [
            [item.fruit, item.amount]
            for item in candidates[:TOP_SIZE]
        ]

    def _process_partial_top(self, query_id, aggregation_id, items):
        """Guarda un top parcial y publica el resultado al completar la consulta."""

        if query_id in self.completed_queries:
            return

        state = self._get_state(query_id)
        if aggregation_id not in state:
            state[aggregation_id] = items

        if len(state) < AGGREGATION_AMOUNT:
            return

        final_top = self._build_final_top(state)
        result = message_protocol.internal.create_final_top_message(
            query_id, final_top
        )
        self.output_queue.send(message_protocol.internal.serialize(result))
        query_done = message_protocol.internal.create_query_done_message(query_id)
        self.control_output.send(
            message_protocol.internal.serialize(query_done)
        )
        logging.info(
            "join_finish query_id=%s partials=%s candidates=%s result_size=%s",
            query_id,
            len(state),
            sum(len(items) for items in state.values()),
            len(final_top),
        )
        del self.states[query_id]
        self.completed_queries.add(query_id)

    def process_message(self, message, ack, nack):
        """Procesa y confirma un top parcial si no hubo errores."""

        try:
            data = message_protocol.internal.deserialize(message)
            query_id, aggregation_id, items = self._validate_partial_top(data)
            self._process_partial_top(query_id, aggregation_id, items)
            ack()
        except Exception:
            logging.exception("Could not process message")
            nack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        finally:
            self.input_queue.close()
            self.output_queue.close()
            self.control_output.close()

    def stop(self):
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: join_filter.stop())
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
