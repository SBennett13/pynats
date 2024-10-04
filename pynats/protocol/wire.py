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

NEWLINE = "\r\n"
B_MSG_DELIM = rb"[ \t]{1,}"
B_MSG_JSON = rb"\{\"[a-zA-Z0-9\"'-_: ]{0,}\}"

RE_MESSAGE_TYPE = re.compile(rb"^(?P<cmd>[A-Za-z+-]{3,})[ \t\r\n]{1,}")

RE_INFO_OPTIONS = re.compile(rb"(?P<options>\{[a-zA-Z0-9\"'-_: ]{1,}\})[ \t]{1,}\r\n")

RE_MSG_BODY = re.compile(
    rb"(?P<subject>[a-zA-Z0-9_\.]{1,})[ \t]{1,}(?P<sid>\w){1,}[ \t]{1,}((?P<reply>[a-zA-Z0-9\._]{1,})[ \t]{1,}){0,1}(?P<numbytes>[0-9]{1,})\r\n(?P<payload>.{0,}){0,1}\r\n",
)
RE_HMSG_BODY = re.compile(
    rb"(?P<subject>[a-zA-Z0-9_\.]{1,})[ \t]{1,}(?P<sid>\w){1,}[ \t]{1,}((?P<reply>[a-zA-Z0-9\._]{1,})[ \t]{1,}){0,1}(?P<numhdrbytes>[0-9]{1,})[ \t]{1,}(?P<numbytes>[0-9]{1,})\r\nNATS/1\.0\r\n(?P<hdr>.{0,})\r\n\r\n(?P<payload>.{0,}){0,1}\r\n",
)


def get_message_type(raw_msg) -> tuple[bytes, bytes]|None:
    match = RE_MESSAGE_TYPE.match(raw_msg)
    if match is None:
        return None
    cmd = match.group("cmd")
    if cmd not in MSG_TYPES:
        return None
    return (cmd, raw_msg[match.span()[1]:])


def build_connect(options: dict) -> bytes:
    """Version and lang aren't included"""
    if not isinstance(options, dict):
        raise TypeError("Options for connect must be a dictionary")

    return f"CONNECT {json.dumps(options)} {NEWLINE}".encode()


def build_pong() -> bytes:
    return f"PONG{NEWLINE}".encode()


def buildPub(subject: str, payload: bytes, reply: str) -> bytes:
    msg = f"PUB {subject}".encode()
    if reply:
        msg += f"{ reply}".encode()
    msg += f" {len(payload)}\r\n".encode()
    if payload:
        msg += payload
    msg += b"\r\n"

    return msg


def buildSub(subject: str, sid: str, queue_group: str = None) -> bytes:
    msg = f"SUB {subject}".encode()
    if queue_group:
        msg += f" {queue_group}".encode()
    msg += f" {sid}\r\n".encode()

    return msg


def buildUnsub(sid: str, max_msgs: int = None) -> bytes:
    msg = f"UNSUB {sid}".encode()
    if max_msgs:
        msg += f" {max_msgs}".encode()
    msg += b"\r\n"

    return msg


def parse_info_options(options: bytes) -> dict:
    options_text = RE_INFO_OPTIONS.match(options)
    if options_text is None:
        return {}

    return json.loads(options_text.group("options"))


def parseMsg(body: bytes) -> dict:
    parsed = RE_MSG_BODY.match(body)
    if parsed is None:
        return None

    msg_content = {
        "subject": parsed.group("subject").decode(),
        "sid": parsed.group("sid").decode(),
        "num_bytes": int(parsed.group("numbytes").decode()),
        "payload": parsed.group("payload"),
    }
    reply_to = parsed.group("reply")
    if reply_to is not None:
        msg_content["reply-to"] = reply_to

    return msg_content


def parseHmsg(body: bytes) -> dict:
    parsed = RE_HMSG_BODY.match(body)
    if parsed is None:
        return None

    msg_content = {
        "subject": parsed.group("subject").decode(),
        "sid": parsed.group("sid").decode(),
        "num_header_bytes": int(parsed.group("numheaderbytes").decode()),
        "num_bytes": int(parsed.group("numbytes").decode()),
        "payload": parsed.group("payload"),
    }
    reply_to = parsed.group("reply")
    if reply_to is not None:
        msg_content["reply-to"] = reply_to

    # TODO Parse headers

    return msg_content