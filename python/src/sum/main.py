import logging
import os
import signal
import threading

from common import fruit_item, message_protocol, middleware


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
CONTROL_ROUTES = [f"{SUM_PREFIX}_{sum_id}" for sum_id in range(SUM_AMOUNT)]


class SumFilter:
    def __init__(self):
        # Estas conexiones pertenecen al thread principal, que consume DATA.
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            INPUT_QUEUE,
            prefetch_count=1,
        )
        self.data_control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            CONTROL_EXCHANGE,
            CONTROL_ROUTES,
            publisher_confirms=True,
        )
        self.data_outputs = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                AGGREGATION_PREFIX,
                [f"{AGGREGATION_PREFIX}_{aggregation_id}"],
                publisher_confirms=True,
            )
            for aggregation_id in range(AGGREGATION_AMOUNT)
        ]

        self.states = {}
        self.states_lock = threading.Lock()

        self.control_input = None
        self.control_thread = None
        self.control_error = None
        self.control_ready = threading.Event()
        self.stop_requested = threading.Event()

    def _get_state_locked(self, query_id):
        """Busca o crea estado local. Requiere tener states_lock."""

        if query_id not in self.states:
            self.states[query_id] = {
                "items_by_fruit": {},
                "processed_records": 0,
                "draining": False,
                "expected_records": None,
                "next_sequence": 1,
                "initial_chunk": None,
            }
        return self.states[query_id]

    def _send_control(self, output, message):
        """Serializa y publica un mensaje del protocolo de control."""

        output.send(message_protocol.internal.serialize(message))

    def _send_chunk(self, outputs, chunk):
        serialized = message_protocol.internal.serialize(chunk)
        for output in outputs:
            output.send(serialized)

    def _process_data(self, query_id, fruit, amount):
        """Acumula un dato o publica un delta si ya se recibió DRAIN."""

        chunk = None
        with self.states_lock:
            state = self._get_state_locked(query_id)
            new_item = fruit_item.FruitItem(fruit, int(amount))

            if state["draining"]:
                sequence = state["next_sequence"]
                state["next_sequence"] += 1
                chunk = message_protocol.internal.create_sum_partials_message(
                    query_id,
                    ID,
                    sequence,
                    1,
                    state["expected_records"],
                    [[new_item.fruit, new_item.amount]],
                )
            else:
                current_item = state["items_by_fruit"].get(fruit)
                if current_item is None:
                    state["items_by_fruit"][fruit] = new_item
                else:
                    state["items_by_fruit"][fruit] = current_item + new_item

            state["processed_records"] += 1

        if chunk is not None:
            self._send_chunk(self.data_outputs, chunk)

    def _process_eof(self, query_id, expected_records):
        """Difunde el fin de entrada a la cola privada de cada Sum."""

        drain = message_protocol.internal.create_drain_message(
            query_id, expected_records
        )
        self._send_control(self.data_control_output, drain)

    def process_message(self, message, ack, nack):
        """Procesa mensajes de la cola compartida de datos."""

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
                raise ValueError("Unknown data message type")
            ack()
        except Exception:
            logging.exception("Could not process data message")
            nack()

    def _process_drain(self, query_id, expected_records, data_outputs):
        """Toma el snapshot inicial que Aggregation usará como barrera."""

        if expected_records < 0:
            raise ValueError("Expected records cannot be negative")

        with self.states_lock:
            state = self._get_state_locked(query_id)
            if state["expected_records"] not in (None, expected_records):
                raise ValueError("Conflicting expected record count")

            if state["initial_chunk"] is None:
                state["draining"] = True
                state["expected_records"] = expected_records
                items = [
                    [item.fruit, item.amount]
                    for item in state["items_by_fruit"].values()
                ]
                state["initial_chunk"] = (
                    message_protocol.internal.create_sum_partials_message(
                        query_id,
                        ID,
                        0,
                        state["processed_records"],
                        expected_records,
                        items,
                    )
                )
                state["items_by_fruit"].clear()

            initial_chunk = state["initial_chunk"]

        self._send_chunk(data_outputs, initial_chunk)
        logging.info(
            "sum_drain query_id=%s sum_id=%s records=%s fruits=%s",
            query_id,
            ID,
            initial_chunk["record_count"],
            len(initial_chunk["items"]),
        )

    def _process_query_done(self, query_id):
        with self.states_lock:
            self.states.pop(query_id, None)

    def process_control_message(
        self,
        message,
        ack,
        nack,
        data_outputs,
    ):
        """Procesa mensajes dirigidos a la réplica por el canal de control."""

        try:
            data = message_protocol.internal.deserialize(message)
            message_type = data["type"]

            if message_type == message_protocol.internal.MsgType.DRAIN:
                self._process_drain(
                    data["query_id"],
                    data["expected_records"],
                    data_outputs,
                )
            elif message_type == message_protocol.internal.MsgType.QUERY_DONE:
                self._process_query_done(data["query_id"])
            else:
                raise ValueError("Unknown control message type")
            ack()
        except Exception:
            logging.exception("Could not process control message")
            nack()

    def _run_control_consumer(self):
        """Crea y usa todas las conexiones pertenecientes al thread de control."""

        control_input = None
        data_outputs = []

        try:
            for aggregation_id in range(AGGREGATION_AMOUNT):
                data_outputs.append(
                    middleware.MessageMiddlewareExchangeRabbitMQ(
                        MOM_HOST,
                        AGGREGATION_PREFIX,
                        [f"{AGGREGATION_PREFIX}_{aggregation_id}"],
                        publisher_confirms=True,
                    )
                )

            control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                CONTROL_EXCHANGE,
                [CONTROL_ROUTES[ID]],
                prefetch_count=1,
            )
            self.control_input = control_input
            self.control_ready.set()

            if self.stop_requested.is_set():
                return

            def on_control_message(message, ack, nack):
                self.process_control_message(
                    message,
                    ack,
                    nack,
                    data_outputs,
                )

            control_input.start_consuming(on_control_message)
        except Exception as error:
            self.control_error = error
            self.control_ready.set()
            logging.exception("Control consumer stopped unexpectedly")
            self.stop_requested.set()
            self.input_queue.stop_consuming()
        finally:
            if control_input is not None:
                control_input.close()
            for data_output in data_outputs:
                data_output.close()

    def stop(self):
        """Solicita la detención de los consumidores de datos y control."""

        self.stop_requested.set()
        self.input_queue.stop_consuming()
        if self.control_input is not None:
            self.control_input.stop_consuming()

    def start(self):
        self.control_thread = threading.Thread(
            target=self._run_control_consumer,
            name=f"sum-{ID}-control",
        )
        self.control_thread.start()

        if not self.control_ready.wait(timeout=30):
            self.stop()
            raise RuntimeError("Control consumer did not become ready")
        if self.control_error is not None:
            self.stop()
            raise self.control_error
        try:
            self.input_queue.start_consuming(self.process_message)
        finally:
            self.stop()
            self.control_thread.join(timeout=10)
            if self.control_thread.is_alive():
                logging.error("Control consumer did not stop in time")
            self.data_control_output.close()
            for data_output in self.data_outputs:
                data_output.close()
            self.input_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.stop())

    try:
        sum_filter.start()
    except KeyboardInterrupt:
        sum_filter.stop()
    return 0


if __name__ == "__main__":
    main()
