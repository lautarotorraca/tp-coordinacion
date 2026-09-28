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
CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
CONTROL_ROUTES = [f"{SUM_PREFIX}_{sum_id}" for sum_id in range(SUM_AMOUNT)]


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
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
        """Busca los datos de un cliente o crea un espacio nuevo."""

        if query_id not in self.states:
            self.states[query_id] = {
                "items_by_fruit": {},
                "processed_batches": set(),
                "initial_sums": set(),
                "processed_records": 0,
                "expected_records": None,
            }
        return self.states[query_id]

    def _process_partials(
        self,
        query_id,
        sum_id,
        sequence,
        record_count,
        expected_records,
        items,
    ):
        """Suma un chunk y finaliza al cubrir todos los registros esperados."""

        if query_id in self.completed_queries:
            return
        if not isinstance(sum_id, int) or not 0 <= sum_id < SUM_AMOUNT:
            raise ValueError("Invalid Sum ID")
        if not isinstance(sequence, int) or sequence < 0:
            raise ValueError("Invalid chunk sequence")
        if not isinstance(record_count, int) or record_count < 0:
            raise ValueError("Invalid chunk record count")
        if not isinstance(expected_records, int) or expected_records < 0:
            raise ValueError("Invalid expected record count")
        if not isinstance(items, list):
            raise ValueError("Invalid chunk items")

        state = self._get_state(query_id)
        batch_id = (sum_id, sequence)
        if batch_id in state["processed_batches"]:
            return

        if state["expected_records"] is None:
            state["expected_records"] = expected_records
        elif state["expected_records"] != expected_records:
            raise ValueError("Conflicting expected record count")

        new_processed_records = state["processed_records"] + record_count
        if new_processed_records > expected_records:
            raise ValueError("Processed record count exceeds expected records")

        for fruit, amount in items:
            new_item = fruit_item.FruitItem(fruit, int(amount))
            if fruit in state["items_by_fruit"]:
                state["items_by_fruit"][fruit] = (
                    state["items_by_fruit"][fruit] + new_item
                )
            else:
                state["items_by_fruit"][fruit] = new_item

        state["processed_batches"].add(batch_id)
        state["processed_records"] = new_processed_records
        if sequence == 0:
            state["initial_sums"].add(sum_id)

        if (
            len(state["initial_sums"]) == SUM_AMOUNT
            and state["processed_records"] == expected_records
        ):
            self._finish_query(query_id)

    def _finish_query(self, query_id):
        """Calcula el top cuando la barrera distribuida está completa."""

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
        query_done = message_protocol.internal.create_query_done_message(query_id)
        self.control_output.send(
            message_protocol.internal.serialize(query_done)
        )
        logging.info(
            "aggregation_finish query_id=%s aggregation_id=%s records=%s "
            "chunks=%s fruits=%s",
            query_id,
            ID,
            state["processed_records"],
            len(state["processed_batches"]),
            len(state["items_by_fruit"]),
        )
        del self.states[query_id]
        self.completed_queries.add(query_id)

    def process_message(self, message, ack, nack):
        """Procesa un mensaje y lo confirma si no hubo errores."""

        try:
            data = message_protocol.internal.deserialize(message)
            if data["type"] == message_protocol.internal.MsgType.SUM_PARTIALS:
                self._process_partials(
                    data["query_id"],
                    data["sum_id"],
                    data["sequence"],
                    data["record_count"],
                    data["expected_records"],
                    data["items"],
                )
            else:
                raise ValueError("Unknown message type")
            ack()
        except Exception:
            logging.exception("Could not process message")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
