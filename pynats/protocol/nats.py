"""Implementation of the logic side of the NATS protocol"""

import contextlib
import ssl
from dataclasses import dataclass, field
from queue import Empty
from threading import Event, Thread
from typing import Callable, Optional
from uuid import uuid4

import pynats.error as exceptions
import pynats.protocol.wire as wire
from pynats.transport import Transport


def createSubId() -> str:
    return str(uuid4()).split("-")[0]


@dataclass
class InfoOptions:
    server_id: str
    server_name: str
    version: str
    headers: bool
    max_payload: int
    proto: int
    client_id: str = ""
    auth_required: bool = False
    tls_required: bool = False
    tls_verify: bool = False
    connect_urls: list[str] = field(default_factory=list)
    ldm: bool = False
    jetstream: bool = False
    nonce: str = ""
    cluster: str = ""
    domain: str = ""

    @staticmethod
    def build(options_dict: dict):
        return InfoOptions(
            options_dict.get("server_id"),
            options_dict.get("server_name"),
            options_dict.get("version"),
            options_dict.get("headers"),
            options_dict.get("max_payload"),
            options_dict.get("proto"),
            options_dict.get("client_id", ""),
            options_dict.get("auth_required", False),
            options_dict.get("tls_required", False),
            options_dict.get("tls_verify", False),
            options_dict.get("connect_urls", []),
            options_dict.get("ldm", False),
            options_dict.get("jetstream", False),
            options_dict.get("nonce", ""),
            options_dict.get("cluster", ""),
            options_dict.get("domain", ""),
        )


class Protocol(Thread):
    def __init__(
        self,
        transport: Transport,
        user: Optional[str] = "",
        password: Optional[str] = "",
        auth_token: Optional[str] = "",
        tls: Optional[ssl.SSLContext] = None,
        connected: Optional[Event] = None,
    ) -> None:
        super().__init__()
        self.transport = transport
        self.user = user
        self.password = password
        self.auth_token = auth_token
        self.tls = tls
        self.got_connect = connected
        self.__close_event = Event()

        if not isinstance(tls, ssl.SSLContext):
            print(
                "'tls' argument should be an ssl.SSLContext to use to upgrade the socket. Setting 'tls' to None"
            )
            self.tls = None

        # Params from the server
        self.info_options: InfoOptions = None

        # Protocol handler for different message types
        self.protocol_handlers = {
            b"INFO": self.handleProtocolInfo,
            b"PING": self.handleProtocolPing,
            b"HMSG": self.handleProtocolHmsg,
            b"MSG": self.handleProtocolMsg,
            b"OK": self.handleProtocolOk,
            b"ERR": self.handleProtocolErr,
        }

        # Map of subject to sub id
        self.subscriptions: dict[str, str] = {}

        self.callbacks: list = []

    def close(self):
        self.__close_event.set()

    def run(self):
        self.transport.start()

        exit_loop = self.__close_event.is_set
        getMsg = self.transport.recv_queue.get
        doneMsg = self.transport.recv_queue.task_done
        suppressEmpty = contextlib.suppress(Empty)
        while not exit_loop():
            with suppressEmpty:
                data: wire.Message = getMsg(timeout=0.01)
                doneMsg()

                # No matter what, there should be a handler
                if data._type not in self.protocol_handlers:
                    print(f"Unrecognized protocol message: {data._type}")
                    continue
                self.protocol_handlers[data._type](data)

        print("Ending NATS Protocol")
        self.transport.close()

    def send(self, subject: str, payload: bytes, headers: dict, reply_to: str) -> None:
        msg_b = (
            wire.buildPub(subject, payload, reply_to)
            if not headers
            else wire.buildHpub(subject, payload, headers, reply_to)
        )
        print("Queueing (H)PUB")
        self.transport.send_queue.put(msg_b)

    def sub(self, subject: str, queue_group: str = None) -> bool:
        if subject in self.subscriptions:
            return False

        sid = createSubId()
        sub_b = wire.buildSub(subject, sid, queue_group)
        print(f"Subbing to {subject} with sid {sid}")
        self.transport.send_queue.put(sub_b, timeout=0.1)
        self.subscriptions[subject] = sid
        return True

    def unsub(self, subject: str, max_msgs: int = None) -> bool:
        sid = self.subscriptions.pop(subject, None)
        if sid is None:
            return False

        unsub_b = wire.buildUnsub(sid, max_msgs)
        self.transport.send_queue.put(unsub_b)
        return True

    def addCB(self, callback: Callable) -> None:
        self.callbacks.append(callback)

    # ---------------------------
    # Protocol type handlers
    # ---------------------------
    def handleProtocolInfo(self, msg: wire.InfoMessage) -> None:
        self.info_options = InfoOptions.build(msg.options)
        print(self.info_options)
        # Verify some info
        if self.info_options.auth_required and (
            any((not self.user, not self.password, not self.auth_token))
        ):
            raise exceptions.AuthException(
                "Server indicated authentication is required but no authentication parameters were provided."
            )

        connect_options = {
            "lang": "py",
            "version": self.info_options.version,
            "verbose": True,
            "pedantic": False,
            "tls_required": self.tls is not None,
            "headers": True,
        }

        if self.info_options.auth_required:
            connect_options["user"] = self.user
            connect_options["pass"] = self.password
            connect_options["auth_token"] = self.auth_token

        if self.info_options.tls_required:
            if self.tls is None:
                raise RuntimeError(
                    "Server indicated TLS is required and not SSL Context was provided"
                )
            self.transport.wrap_socket(self.tls)

        connect_wire: bytes = wire.build_connect(connect_options)
        print("Sending connect")
        self.transport.send_queue.put(connect_wire)
        self.got_connect.set()

    def handleProtocolPing(self, _) -> None:
        print("Received PING, sending PONG")
        pong_msg = wire.build_pong()
        self.transport.send_queue.put(pong_msg)

    def handleProtocolMsg(self, msg: wire.MsgMessage) -> None:
        print("Received MSG")
        for callback in self.callbacks:
            callback(msg)

    def handleProtocolHmsg(self, msg: wire.HmsgMessage) -> None:
        print("Received HMSG")
        for callback in self.callbacks:
            callback(msg)

    def handleProtocolOk(self, msg: wire.Message) -> None:
        print("Got +OK")

    def handleProtocolErr(self, msg: wire.ErrMessage) -> None:
        print(f"Got -ERR: {msg.error_message}")
