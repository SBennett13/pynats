"""Connection wrapper for NATS protocol client
"""

from queue import Queue
 
import pynats.protocol.nats as nats_protocol
import pynats.protocol.wire
import pynats.transport as transport

class NATSClient:

    def __init__(self, host: str, port: int) -> None:
        recv_queue = Queue(50)
        self.__transport = transport.Transport(host, port, recv_queue)
        self.__nats_protocol = nats_protocol.Protocol(self.__transport)

    def start(self) -> None:
        self.__nats_protocol.start()

    def close(self) -> None:
        self.__nats_protocol.close()
        self.__nats_protocol.join()
