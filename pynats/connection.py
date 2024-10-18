"""Connection wrapper for NATS protocol client"""

import logging
import ssl
from queue import Queue
from threading import Event
from typing import Callable, Optional

import pynats.protocol.nats as nats_protocol
import pynats.transport as transport


class NATSClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: Optional[str] = "",
        password: Optional[str] = "",
        auth_token: Optional[str] = "",
        tls: Optional[ssl.SSLContext] = None,
    ) -> None:
        recv_queue = Queue(50)
        send_queue = Queue(50)
        self.connected = Event()
        self.__logger = logging.getLogger("pynats")
        self.__transport = transport.Transport(host, port, recv_queue, send_queue)
        self.__nats_protocol = nats_protocol.Protocol(
            self.__transport, user, password, auth_token, tls, self.connected
        )

    def start(self) -> None:
        self.__logger.debug("Starting NATS client")
        self.__nats_protocol.start()

        self.connected.wait()

    def close(self) -> None:
        self.__logger.debug("Closing NATS client")
        self.__nats_protocol.close()
        self.__nats_protocol.join()

    def send(
        self, subject: str, payload: bytes, header: dict = None, reply_to: str = None
    ) -> None:
        if not (
            isinstance(subject, str)
            and isinstance(payload, bytes)
            and (isinstance(reply_to, str) or reply_to is None)
        ):
            self.__logger.error(
                "'subject' must be a string and 'payload' must be bytes"
            )
            return False

        if header is not None and not self.__nats_protocol.info_options.headers:
            self.__logger.warning(
                "Headers were provided, but the server indicated that it doesn't want headers. Dropping headers and sending message"
            )
            header = None

        self.__nats_protocol.send(subject, payload, header, reply_to)
        return True

    def addCallback(self, callback: Callable) -> bool:
        if not isinstance(callback, Callable):
            self.__logger.error("Provided callback is not a Callable")
            return False
        self.__nats_protocol.addCB(callback)
        return True

    def subscibe(self, subject: str, queue_group: str = None):
        self.__nats_protocol.sub(subject, queue_group)

    def unsubscribe(self, subject: str, messages_to_wait_for: int = 0):
        self.__nats_protocol.unsub(subject, messages_to_wait_for)
