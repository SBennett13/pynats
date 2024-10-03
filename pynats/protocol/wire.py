"""Manage all things NATS wire protocol related
"""

import json
import re

MSG_TYPES = (
    b"INFO",
    b"CONNECT",
    b"PUB",
    b"HPUB",
    b"SUB",
    b"UNSUB",
    b"MSG",
    b"HMSG",
    b"PING",
    b"PONG",
    b"+OK",
    b"-ERR",
)

B_NEWLINE = b"\r\n"
B_MSG_DELIM = rb"[ \t]{1,}"
B_MSG_JSON = rb"\{\"[a-zA-Z0-9\"'-_: ]{0,}\}"

RE_MESSAGE_TYPE = re.compile(rb"^(?P<cmd>[A-Za-z]{3,})[ \t]{1,}")

def get_message_type(raw_msg) -> tuple[bytes, bytes]|None:
    match = RE_MESSAGE_TYPE.match(raw_msg)
    if match is None:
        return None
    cmd = match.group("cmd")
    if cmd not in MSG_TYPES:
        return None
    return (cmd, raw_msg[match.span()[1]:])


def build_connect(options: dict):
    """Version and lang aren't included"""
    if not isinstance(options, dict):
        raise TypeError("Options for connect must be a dictionary")
    
    return f"CONNECT {json.dumps(options)}".encode()
    
def parse_connect(options: bytes) -> dict:
    return json.loads(options)