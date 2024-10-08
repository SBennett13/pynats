"""Manage all things NATS wire protocol related"""

import dataclasses
import json
import re
from typing import Union

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
B_NEWLINE = b"\r\n"
B_MSG_DELIM = rb"[ \t]{1,}"
B_MSG_JSON = rb"\{\"[a-zA-Z0-9\"'-_: ]{0,}\}"

RE_MESSAGE_TYPE = re.compile(rb"^(?P<cmd>[A-Za-z+-]{3,})[ \t\r\n]{1,}")

RE_INFO_MSG = re.compile(
    rb"INFO[ \t]{1,}(?P<options>\{[a-zA-Z0-9\"'-_: ]{1,}\})[ \t]{1,}\r\n"
)

RE_MSG_BODY = re.compile(
    rb"MSG[ \t]{1,}(?P<subject>[\w\.]{1,})[ \t]{1,}(?P<sid>[\w\.]{1,})[ \t]{1,}((?P<reply>[\w\.]{1,})[ \t]{1,}){0,1}(?P<numbytes>[0-9]{1,})\r\n(?P<payload>.{0,}){0,1}\r\n",
    re.ASCII,
)
RE_HMSG_BODY = re.compile(
    rb"HMSG[ \t]{1,}(?P<subject>[\w\.]{1,})[ \t]{1,}(?P<sid>[\w\.]{1,})[ \t]{1,}((?P<reply>[\w\.]{1,})[ \t]{1,}){0,1}(?P<numhdrbytes>[0-9]{1,})[ \t]{1,}(?P<numbytes>[0-9]{1,})\r\nNATS/1\.0\r\n(?P<hdr>([\w.]{1,}: [\w.]{1,}\r\n){1,})\r\n(?P<payload>.{0,}){0,1}\r\n",
    re.ASCII,
)


@dataclasses.dataclass
class Message:
    _type: bytes


@dataclasses.dataclass
class InfoMessage(Message):
    options: dict


@dataclasses.dataclass
class MsgMessage(Message):
    subject: str
    sid: str
    payload: bytes
    reply_to: str = ""


@dataclasses.dataclass
class HmsgMessage(Message):
    subject: str
    sid: str
    header: dict
    payload: bytes
    reply_to: str = ""


def parse_stream(buf: bytearray, putMsg):
    msg_type_match = RE_MESSAGE_TYPE.match(buf)
    if msg_type_match is None:
        # If we found nothing, find the next \r\n and trim the front
        # first_end = buf.find(b"\r\n")
        print("HELP ME PARSE")
        return 0

    msg_type = msg_type_match.group("cmd")
    if msg_type == b"INFO":
        info_msg, byte_len = parse_info(buf)
        putMsg(info_msg)
        return byte_len if byte_len is not None else 0
    elif msg_type == b"+OK":
        putMsg(Message(b"OK"))
        return msg_type_match.end()
    elif msg_type == b"PING":
        msg = Message(msg_type)
        putMsg(msg)
        return msg_type_match.end()
    elif msg_type == b"MSG":
        msg, byte_len = parseMsg(buf)
        putMsg(msg)
        return byte_len
    elif msg_type == b"HMSG":
        msg, byte_len = parseHmsg(buf)
        putMsg(msg)
        return byte_len


def parse_info(buf: bytearray) -> Union[Message, None]:
    # Look after the info for options
    info_opts = RE_INFO_MSG.match(buf, 0)
    if info_opts is not None:
        return InfoMessage(
            b"INFO", json.loads(info_opts.group("options"))
        ), info_opts.end()
    else:
        print(f"HELP: {buf}")
        return None, None


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
        msg += f" {reply}".encode()
    msg += f" {len(payload)}\r\n".encode()
    if payload:
        msg += payload
    msg += b"\r\n"

    return msg


def buildHpub(subject: str, payload: bytes, headers: dict, reply: str) -> bytes:
    hdrs = b"NATS/1.0\r\n"
    for k, v in headers.items():
        hdrs += f"{k}: {v}{NEWLINE}".encode()
    msg = f"HPUB {subject}".encode()
    if reply:
        msg += f" {reply}".encode()
    msg += f" {len(hdrs) + 2} {len(hdrs) + len(payload) + 2}{NEWLINE}".encode()
    msg += hdrs + B_NEWLINE
    if payload:
        msg += payload
    msg += B_NEWLINE
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
    options_text = RE_INFO_MSG.match(options)
    if options_text is None:
        return {}

    return json.loads(options_text.group("options"))


def parseMsg(buf: bytearray) -> dict:
    parsed = RE_MSG_BODY.match(buf)
    if parsed is None:
        return None

    msg = MsgMessage(
        b"MSG",
        parsed.group("subject").decode(),
        parsed.group("sid").decode(),
        parsed.group("payload"),
    )
    _ = int(parsed.group("numbytes").decode())
    reply_to = parsed.group("reply")
    if reply_to is not None:
        msg.reply_to = reply_to
    return msg, parsed.end()


def parseHmsg(body: bytes) -> dict:
    parsed = RE_HMSG_BODY.match(body)
    if parsed is None:
        return None

    msg = HmsgMessage(
        b"HMSG",
        parsed.group("subject").decode(),
        parsed.group("sid").decode(),
        {},
        parsed.group("payload"),
    )
    reply_to = parsed.group("reply")
    if reply_to is not None:
        msg.reply_to = reply_to

    hdrs = parsed.group("hdr")
    for headers in hdrs.decode().split("\r\n"):
        if not headers:
            continue
        k, v = headers.split(":")
        msg.header.update({k.strip(): v.strip()})
    return msg, parsed.end()
