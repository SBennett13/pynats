#!/usr/bin/env python3

import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


import pynats

def printMsg(msg):
    print(f"GOT MESSAGE: {msg}")


def main():
    a = pynats.NATSClient("localhost", 4222)    
    a.start()
    a.addCallback(printMsg)
    a.subscibe("FOO.BAR")
    a.send("FOO.BAR", b"Hello NATS!", {"Bar": "Baz", "a": "b"})
    time.sleep(10)
    a.unsubscribe("TEST")
    time.sleep(2)
    a.close()

if __name__ == "__main__":
    main()