"""All transport related things for the NATS Protocol
"""
import contextlib
import os
import select
import socket
import threading
from queue import Empty, Full, Queue


class Transport:
    def __init__(self, host: str, port: int, queue: Queue, send_queue: Queue) -> None:
        self.__socket = None
        self.__host = host
        self.__port = port
        self.__rcv_thread = None
        self.__close_pipe = os.pipe()
        self.__exit_event = threading.Event()

        self.recv_queue = queue
        self.send_queue = send_queue

    def start(self) -> None:
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.settimeout(0.01)
        self.__socket.connect((self.__host, self.__port))
        self.__socket.setblocking(0)
        self.__rcv_thread = threading.Thread(
            target=self.__thread_socketread
        )
        self.__rcv_thread.start()

    def close(self):
        self.__exit_event.set()
        os.write(self.__close_pipe[1], b"x")
        self.__rcv_thread.join()
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()

    def __thread_socketread(self):

        read = self.__socket.recv
        write = self.__socket.send
        sock = self.__socket
        pipe = self.__close_pipe[0]
        ex = self.__exit_event
        put_queue = self.recv_queue.put
        suppress_empty = contextlib.suppress(Empty)
        getSend = self.send_queue.get_nowait
        doneSend = self.send_queue.task_done

        while not ex.is_set():
            try:
                r, w, e = select.select([sock, pipe], [sock], [], 30)
                if w:
                    with suppress_empty:
                        msg = getSend()
                        write(msg)
                        doneSend()

                if not r:
                    continue
                if pipe in r:
                    continue

                data = read(4096)
                print(f"RECEIVED {data}")
                put_queue(data)
            except socket.error as e:
                print(f"SOCKET ERROR: {e}")
            except Full:
                print("Queue was full, wtf?")

        print("Ending socket read thread")