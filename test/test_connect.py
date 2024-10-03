#!/usr/bin/env python3

import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


import pynats

def main():
    a = pynats.NATSClient("localhost", 4222)    
    a.start()
    time.sleep(5)
    a.close()

if __name__ == "__main__":
    main()