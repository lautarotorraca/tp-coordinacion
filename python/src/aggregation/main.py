import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.states = {}

    def _get_state(self, query_id):
        """Busca los datos de un cliente o crea un espacio nuevo."""

        if query_id not in self.states:
            self.states[query_id] = {
                "items_by_fruit": {},
                "processed_sums": set(),
            }
        return self.states[query_id]

    def _process_partials(self, query_id, sum_id, items):
        """Suma los resultados que mandó un Sum."""

        state = self._get_state(query_id)
        if sum_id in state["processed_sums"]:
            return

        for fruit, amount in items:
            new_item = fruit_item.FruitItem(fruit, int(amount))
            if fruit in state["items_by_fruit"]:
                state["items_by_fruit"][fruit] = (
                    state["items_by_fruit"][fruit] + new_item
                )
            else:
                state["items_by_fruit"][fruit] = new_item

        state["processed_sums"].add(sum_id)

    def _process_eof(self, query_id):
        """Calcula el top y lo manda a Join."""

        state = self._get_state(query_id)
        sorted_items = sorted(
            state["items_by_fruit"].values(), reverse=True
        )

        top = []
        for item in sorted_items[:TOP_SIZE]:
            top.append([item.fruit, item.amount])

        partial_top = message_protocol.internal.create_partial_top_message(
            query_id, ID, top
        )
        self.output_queue.send(message_protocol.internal.serialize(partial_top))
        del self.states[query_id]

    def process_message(self, message, ack, nack):
        """Procesa un mensaje y lo confirma si no hubo errores."""

        try:
            data = message_protocol.internal.deserialize(message)
            if data["type"] == message_protocol.internal.MsgType.SUM_PARTIALS:
                self._process_partials(
                    data["query_id"], data["sum_id"], data["items"]
                )
            elif data["type"] == message_protocol.internal.MsgType.AGG_EOF:
                self._process_eof(data["query_id"])
            else:
                raise ValueError("Unknown message type")
            ack()
        except Exception:
            logging.exception("Could not process message")
            nack()

    def start(self):
        """Empieza a leer los resultados que mandan los Sum."""

        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
