import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.states = {}

    def _get_state(self, query_id):
        """Busca los datos de un cliente o crea un espacio nuevo."""

        if query_id not in self.states:
            self.states[query_id] = {
                "items_by_fruit": {},
                "processed_records": 0,
            }
        return self.states[query_id]

    def _process_data(self, query_id, fruit, amount):
        """Suma un dato al total de su fruta."""

        state = self._get_state(query_id)
        new_item = fruit_item.FruitItem(fruit, int(amount))
        if fruit in state["items_by_fruit"]:
            state["items_by_fruit"][fruit] = (
                state["items_by_fruit"][fruit] + new_item
            )
        else:
            state["items_by_fruit"][fruit] = new_item
        state["processed_records"] += 1

    def _process_eof(self, query_id, expected_records):
        """Manda los resultados y avisa que el cliente terminó."""

        state = self._get_state(query_id)
        if state["processed_records"] != expected_records:
            raise ValueError("Not all records were processed")

        items = []
        for final_item in state["items_by_fruit"].values():
            items.append([final_item.fruit, final_item.amount])

        partials = message_protocol.internal.create_sum_partials_message(
            query_id, ID, items
        )
        eof = message_protocol.internal.create_agg_eof_message(query_id)

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize(partials))
            data_output_exchange.send(message_protocol.internal.serialize(eof))

        del self.states[query_id]

    def process_message(self, message, ack, nack):
        """Procesa un mensaje y lo confirma si no hubo errores."""

        try:
            data = message_protocol.internal.deserialize(message)
            if data["type"] == message_protocol.internal.MsgType.DATA:
                self._process_data(
                    data["query_id"], data["fruit"], data["amount"]
                )
            elif data["type"] == message_protocol.internal.MsgType.INPUT_EOF:
                self._process_eof(
                    data["query_id"], data["expected_records"]
                )
            else:
                raise ValueError("Unknown message type")
            ack()
        except Exception:
            logging.exception("Could not process message")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
