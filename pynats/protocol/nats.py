"""Implementation of the logic side of the NATS protocol
"""

from queue import Empty
from threading import Condition, Event, Thread
import contextlib
import time

from pynats.transport import Transport
import pynats.protocol.wire as wire
class Protocol(Thread):

    def __init__(self, transport: Transport) -> None:
        super().__init__()
        self.transport = transport
        self.__close_event = Event()
    
        # Params from the server
        self.server_id: str = None
        self.server_name: str = None
        self.version: str = None
        self.proto: int = None
        self.supports_headers: bool = None
        self.max_payload: int = None
        self.client_id: int = None
        self.xkey: str = None

    def close(self):
        self.__close_event.set()
    
    def run(self):

        self.transport.start()

        exit_loop = self.__close_event.is_set
        getMsg = self.transport.recv_queue.get_nowait
        doneMsg = self.transport.recv_queue.task_done
        suppressEmpty = contextlib.suppress(Empty)
        while not exit_loop():
            with suppressEmpty:
                data = getMsg()
                doneMsg()
                a = wire.get_message_type(data)
                print(a)

        print("Ending NATS Protocol")
        self.transport.close()