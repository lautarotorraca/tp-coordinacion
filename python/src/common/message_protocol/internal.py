import json


class MsgType:
    """Mensajes que se mandan los procesos del sistema."""

    DATA = "DATA"
    INPUT_EOF = "INPUT_EOF"
    START_DRAIN = "START_DRAIN"
    DRAIN = "DRAIN"
    DRAIN_ACK = "DRAIN_ACK"
    FLUSH = "FLUSH"
    FLUSH_DONE = "FLUSH_DONE"
    SUM_PARTIALS = "SUM_PARTIALS"
    AGG_EOF = "AGG_EOF"
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


def create_start_drain_message(query_id, expected_records):
    """Pide al coordinador que inicie el cierre de una consulta."""

    return {
        "type": MsgType.START_DRAIN,
        "query_id": query_id,
        "expected_records": expected_records,
    }


def create_drain_message(query_id):
    """Pide a un Sum que informe su progreso para una consulta."""

    return {
        "type": MsgType.DRAIN,
        "query_id": query_id,
    }


def create_drain_ack_message(query_id, sum_id, processed_records):
    """Informa al coordinador el progreso local de un Sum."""

    return {
        "type": MsgType.DRAIN_ACK,
        "query_id": query_id,
        "sum_id": sum_id,
        "processed_records": processed_records,
    }


def create_flush_message(query_id):
    """Habilita a un Sum a publicar sus resultados parciales."""

    return {
        "type": MsgType.FLUSH,
        "query_id": query_id,
    }


def create_flush_done_message(query_id, sum_id):
    """Confirma que un Sum publicó sus resultados parciales."""

    return {
        "type": MsgType.FLUSH_DONE,
        "query_id": query_id,
        "sum_id": sum_id,
    }


def create_sum_partials_message(query_id, sum_id, items):
    """Arma un mensaje con los resultados calculados por un Sum."""

    return {
        "type": MsgType.SUM_PARTIALS,
        "query_id": query_id,
        "sum_id": sum_id,
        "items": items,
    }


def create_agg_eof_message(query_id):
    """Avisa a Aggregation que no van a llegar más resultados."""

    return {
        "type": MsgType.AGG_EOF,
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
