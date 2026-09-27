import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        """Guarda un ID para el cliente y cuenta los datos que manda."""

        self.query_id = str(uuid.uuid4())
        self.record_count = 0

    def serialize_data_message(self, message):
        """Agrega el ID del cliente y arma el mensaje para Sum."""

        [fruit, amount] = message
        self.record_count += 1
        data_message = message_protocol.internal.create_data_message(
            self.query_id, fruit, amount
        )
        return message_protocol.internal.serialize(data_message)

    def serialize_eof_message(self, message):
        """Arma el mensaje que avisa que el cliente terminó."""

        eof_message = message_protocol.internal.create_input_eof_message(
            self.query_id, self.record_count
        )
        return message_protocol.internal.serialize(eof_message)

    def deserialize_result_message(self, message):
        """Devuelve el top si pertenece a este cliente."""

        result = message_protocol.internal.deserialize(message)
        if result.get("type") != message_protocol.internal.MsgType.FINAL_TOP:
            return None
        if result.get("query_id") != self.query_id:
            return None
        return result["items"]
