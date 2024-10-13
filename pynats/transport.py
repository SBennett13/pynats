"""All transport related things for the NATS Protocol"""

import contextlib
import os
import select
import socket
import threading
from queue import Empty, Full, Queue
from ssl import SSLContext, SSLError

import pynats.protocol.wire as wire

import time

class Transport:
    def __init__(self, host: str, port: int, queue: Queue, send_queue: Queue) -> None:
        self.__socket = None
        self.__host = host
        self.__port = port
        self.__rcv_thread = None
        self._send_buf_thread = None
        self.__close_pipe_r = os.pipe()
        self.__close_pipe_w = os.pipe()
        self.__exit_event = threading.Event()

        self._send_buf_thread = None
        self.__rcv_thread = None

        self.recv_queue = queue
        self.send_queue = send_queue

    def start(self) -> None:
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.settimeout(0.01)
        self.__socket.connect((self.__host, self.__port))
        self.__socket.setblocking(0)

        self.__start_readwrite_threads()

    def close(self):
        self.__exit_event.set()
        os.write(self.__close_pipe_r[1], b"x")
        os.write(self.__close_pipe_w[1], b"x")
        self.__rcv_thread.join()
        self._send_buf_thread.join()
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()

    def __start_readwrite_threads(self) -> None:
        self.__rcv_thread = threading.Thread(target=self.__thread_socketread)
        self.__rcv_thread.start()

        self._send_buf_thread = threading.Thread(target=self.__thread_sendbuf)
        self._send_buf_thread.start()

    def wrap_socket(self, ssl_context: SSLContext):
        print("Closing read and write threads to upgrade socket")
        os.write(self.__close_pipe_r[1], b"x")
        os.write(self.__close_pipe_w[1], b"x")
        self.__rcv_thread.join()
        self._send_buf_thread.join()
        self.__socket = ssl_context.wrap_socket(
            self.__socket, server_hostname=self.__host, do_handshake_on_connect=False
        )
        try:
            self.__socket.do_handshake(True)
        except SSLError as e:
            print(f"SSL ERROR: {e}")

        self.__close_pipe_r = os.pipe()
        self.__close_pipe_w = os.pipe()
        print("Restarting read and write threads post SSL upgrade")
        self.__start_readwrite_threads()

    def __thread_sendbuf(self):
        getSend = self.send_queue.get
        getDone = self.send_queue.task_done
        ex_event = self.__exit_event.is_set
        send_buf = bytearray()
        pipe = self.__close_pipe_w[0]
        suppress = contextlib.suppress(Empty)

        while not ex_event():
            with suppress:
                msg = getSend(timeout=0.01)
                send_buf.extend(msg)
                getDone()

            r, w, _ = select.select([pipe], [self.__socket], [], 10)
            if r:
                with os.fdopen(pipe) as fd:
                    fd.read(1)
                break
            if w and send_buf:
                num_sent = self.__socket.send(bytes(send_buf))
                send_buf = send_buf[num_sent:]

        print("Exiting send thread")

    def __thread_socketread(self):
        sock = self.__socket
        pipe = self.__close_pipe_r[0]
        ex = self.__exit_event
        put_queue = self.recv_queue.put
        recv_buf = bytearray()

        while not ex.is_set():
            try:
                r, _, e = select.select([self.__socket, pipe], [], [], 10)

                if e:
                    print("THIS IS BAD")

                if pipe in r:
                    with os.fdopen(pipe) as fd:
                        fd.read(1)
                    break
                if sock in r:
                    data = self.__socket.recv(1024)
                    recv_buf.extend(data)
                while recv_buf:
                    bytes_processed = wire.parse_stream(recv_buf, put_queue)
                    recv_buf = recv_buf[bytes_processed:]
                    if bytes_processed == 0 or not recv_buf:
                        break

            except socket.error as e:
                print(f"SOCKET ERROR: {e}")
            except Full:
                print("Queue was full, wtf?")

        print("Exiting socket read thread")
