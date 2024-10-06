"""Connection wrapper for NATS protocol client"""

from queue import Queue
from threading import Event
import pynats.protocol.nats as nats_protocol
import pynats.transport as transport


class NATSClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str = "",
        password: str = "",
        auth_token: str = "",
        use_tls: bool = False,
    ) -> None:
        recv_queue = Queue(50)
        send_queue = Queue(50)
        self.connected = Event()
        self.__transport = transport.Transport(host, port, recv_queue, send_queue)
        self.__nats_protocol = nats_protocol.Protocol(
            self.__transport, user, password, auth_token, use_tls, self.connected
        )

    def start(self) -> None:
        self.__nats_protocol.start()

        self.connected.wait()

    def close(self) -> None:
        self.__nats_protocol.close()
        self.__nats_protocol.join()

    def send(self, subject: str, payload: bytes, reply_to: str = None) -> None:
        if not (
            isinstance(subject, str)
            and isinstance(payload, bytes)
            and (isinstance(reply_to, str) or reply_to is None)
        ):
            print("'subject' must be a string and 'payload' must be bytes")
            return

        self.__nats_protocol.send(subject, payload, reply_to)

    def subscibe(self, subject: str, queue_group: str = None):
        self.__nats_protocol.sub(subject, queue_group)

    def unsubscribe(self, subject: str, messages_to_wait_for: int = 0):
        self.__nats_protocol.unsub(subject, messages_to_wait_for)
