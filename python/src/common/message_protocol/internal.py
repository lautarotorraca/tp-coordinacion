import json


class MsgType:
    """Mensajes que se mandan los procesos del sistema."""

    DATA = "DATA"
    INPUT_EOF = "INPUT_EOF"
    DRAIN = "DRAIN"
    SUM_PARTIALS = "SUM_PARTIALS"
    QUERY_DONE = "QUERY_DONE"
    PARTIAL_TOP = "PARTIAL_TOP"
    FINAL_TOP = "FINAL_TOP"


def create_data_message(query_id, fruit, amount):
    """Arma un mensaje con una fruta y su cantidad para que Sum la procese."""

    return {
        "type": MsgType.DATA,
        "query_id": query_id,
        "fruit": fruit,
        "amount": amount,
    }


def create_input_eof_message(query_id, expected_records):
    """Avisa que el cliente terminó y cuántos datos mandó."""

    return {
        "type": MsgType.INPUT_EOF,
        "query_id": query_id,
        "expected_records": expected_records,
    }


def create_drain_message(query_id, expected_records):
    """Avisa a todos los Sum que no se producirán más datos."""

    return {
        "type": MsgType.DRAIN,
        "query_id": query_id,
        "expected_records": expected_records,
    }


def create_sum_partials_message(
    query_id,
    sum_id,
    sequence,
    record_count,
    expected_records,
    items,
):
    """Arma un chunk disjunto de resultados calculados por un Sum."""

    return {
        "type": MsgType.SUM_PARTIALS,
        "query_id": query_id,
        "sum_id": sum_id,
        "sequence": sequence,
        "record_count": record_count,
        "expected_records": expected_records,
        "items": items,
    }


def create_query_done_message(query_id):
    """Permite que los Sum liberen el estado de una consulta terminada."""

    return {
        "type": MsgType.QUERY_DONE,
        "query_id": query_id,
    }


def create_partial_top_message(query_id, aggregation_id, items):
    """Arma el top que calculó un Aggregation."""

    return {
        "type": MsgType.PARTIAL_TOP,
        "query_id": query_id,
        "aggregation_id": aggregation_id,
        "items": items,
    }


def create_final_top_message(query_id, items):
    """Arma el top final que Join manda al gateway."""

    return {
        "type": MsgType.FINAL_TOP,
        "query_id": query_id,
        "items": items,
    }


def serialize(message):
    """Pasa un mensaje a JSON y lo devuelve como bytes."""

    return json.dumps(message).encode("utf-8")


def deserialize(message):
    """Pasa los bytes JSON recibidos a un objeto de Python."""

    return json.loads(message.decode("utf-8"))
