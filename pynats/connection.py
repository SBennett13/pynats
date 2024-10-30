"""Connection wrapper for NATS protocol client"""

import logging
import ssl
from queue import Queue
from threading import Event
from typing import Callable, Optional, Union

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
        callback: Optional[Callable] = None,
    ) -> None:
        """Create a NATS Client

        Inputs:
            host: The hostname of the NATS server
            port: The port of the NATS server
            user: The user to authenticate with (If the server requires authentication)
            password: The password for the provided user (if the server requires authentication)
            auth_token: The auth token, if the server requires it
            tls: A ssl.SSLContext to wrap the TCP socket with to upgrade the connection to TLS
            callback: A callable method as a catch-all message callback


        NOTE: It is recommended that your "callback" methods just append to your own queue rather than
        actually process messages so that the socket select doesn't get blocked by function execution.
        """
        recv_queue = Queue(50)
        send_queue = Queue(50)
        self.connected = Event()
        self.__logger = logging.getLogger("pynats")
        self.__transport = transport.Transport(host, port, recv_queue, send_queue)
        self.__nats_protocol = nats_protocol.Protocol(self.__transport, user, password, auth_token, tls, self.connected)
        self.__nats_protocol.addCB(callback)

    def start(self) -> None:
        """Start the NATS protocol. Connect the socket, wait for the INFO frame, then send the CONNECT frame"""
        self.__logger.debug("Starting NATS client")
        self.__nats_protocol.start()

        self.connected.wait()

    def close(self) -> None:
        """Close the NATS client, disconnecting from the server"""
        self.__logger.debug("Closing NATS client")
        self.__nats_protocol.close()
        self.__nats_protocol.join()

    def send(self, subject: str, payload: bytes, header: dict = None, reply_to: str = None) -> None:
        """Send a message to the given subject. Payload should already be of type `bytes`.

        `header` should be a dictionary of headers, which will be ignored if the server indicates that it doesn't
        support headers.
        """
        if not (
            isinstance(subject, str) and isinstance(payload, bytes) and (isinstance(reply_to, str) or reply_to is None)
        ):
            self.__logger.error("'subject' must be a string and 'payload' must be bytes")
            return False

        if header is not None and not self.__nats_protocol.info_options.headers:
            self.__logger.warning(
                "Headers were provided, but the server indicated that it doesn't want headers. Dropping headers and sending message"
            )
            header = None

        self.__nats_protocol.send(subject, payload, header, reply_to)
        return True

    def addCallback(self, callback: Callable, subject: str = "") -> Union[str, None]:
        """Add a callback, optionally specifying a subject to associate it with"""
        if not isinstance(callback, Callable):
            self.__logger.error("Provided callback is not a Callable")
            return None
        callback_id = self.__nats_protocol.addCB(callback, subject)
        return callback_id

    def removeCallback(self, callback_id: str, subject: str = "") -> bool:
        """Remove a callback, optionally from a subject, corresponding to the given callback ID"""
        return self.__nats_protocol.removeCB(callback_id, subject)

    def subscribe(self, subject: str, queue_group: str = None):
        """Subscribe to a subject"""
        self.__nats_protocol.sub(subject, queue_group)

    def unsubscribe(self, subject: str, messages_to_wait_for: int = 0):
        """Unsubscribe from a subject"""
        self.__nats_protocol.unsub(subject, messages_to_wait_for)
