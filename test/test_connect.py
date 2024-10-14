#!/usr/bin/env python3

import os
import ssl
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


import pynats

SSL_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config")


def printMsg(msg):
    print(f"GOT MESSAGE: {msg}")


def main():
    ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    # ssl_ctx.minimum_version = ssl.PROTOCOL_TLSv1_2
    ssl_ctx.load_cert_chain(
        os.path.join(SSL_DIR, "client-cert.pem"),
        os.path.join(SSL_DIR, "client-key.pem"),
    )
    rootCA = os.getenv("ROOT_CA")
    if rootCA:
        ssl_ctx.load_verify_locations(rootCA)
    ssl_ctx.check_hostname = True
    a = pynats.NATSClient("localhost", 4222, user="a", password="b", tls=ssl_ctx)
    a.start()
    time.sleep(2)
    a.addCallback(printMsg)
    a.subscibe("FOO.BAR")
    a.send("FOO.BAR", b"Hello NATS!", {"Bar": "Baz", "a": "b"})
    time.sleep(10)
    a.unsubscribe("TEST")
    time.sleep(2)
    a.close()

if __name__ == "__main__":
    main()