"""All transport related things for the NATS Protocol
"""
import contextlib
import os
import select
import socket
import threading
from queue import Empty, Full, Queue

import pynats.protocol.wire as wire

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

        self.recv_buffer = bytearray()
        self.send_buf = bytearray()

    def start(self) -> None:
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.settimeout(0.01)
        self.__socket.connect((self.__host, self.__port))
        self.__socket.setblocking(0)
        self.__rcv_thread = threading.Thread(
            target=self.__thread_socketread
        )
        self.__rcv_thread.start()

        self._send_buf_thread = threading.Thread(target=self.__thread_sendbuf)
        self._send_buf_thread.start()

    def close(self):
        self.__exit_event.set()
        os.write(self.__close_pipe[1], b"x")
        self.__rcv_thread.join()
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()

    def __thread_sendbuf(self):
        getSend = self.send_queue.get_nowait
        getDone = self.send_queue.task_done
        ex_event = self.__exit_event.is_set
        extend = self.send_buf.extend
        write = self.__socket.send
        suppress = contextlib.suppress(Empty)

        while not ex_event():
            with suppress:
                msg = getSend()
                extend(msg)
                getDone()
            if self.send_buf:
                _, w, _ = select.select([], [self.__socket], [], 3)
                if w:
                    print(self.send_buf)
                    num_sent = write(self.send_buf)
                    print(f"Wrote {num_sent} bytes")
                    self.send_buf = self.send_buf[num_sent:]

        print("Exiting send thread")

    def __thread_socketread(self):
        read = self.__socket.recv
        sock = self.__socket
        pipe = self.__close_pipe[0]
        ex = self.__exit_event
        put_queue = self.recv_queue.put
        recv_buf = self.recv_buffer

        while not ex.is_set():
            try:
                r, _, e = select.select([sock, pipe], [], [], 10)

                if e:
                    print("THIS IS BAD")

                if pipe in r:
                    continue
                if sock in r:
                    data = read(1024)
                    recv_buf.extend(data)
                    print(f"Received {data}")
                if recv_buf:
                    wire.parse_stream(data, put_queue)

            except socket.error as e:
                print(f"SOCKET ERROR: {e}")
            except Full:
                print("Queue was full, wtf?")

        print("Ending socket read thread")