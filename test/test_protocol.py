#!/usr/bin/env python3
"""Test protocol related things"""

import os
import sys

try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "pynats"))
    import protocol.wire as protocol
except ImportError:
    print("Error importing protocol")
    sys.exit(1)


def test_delimiters() -> None:
    tests = (b"    ", b"\t\t  \t", b"INFO   {option things}\t\r\n")
    fails = []
    import re

    regexp: re.Pattern[bytes] = re.compile(protocol.B_MSG_DELIM)
    for test in tests:
        found = regexp.search(test)
        if found is None:
            fails.append(test)

    print(f"DELIMITER: Passed {len(tests) - len(fails)} tests out of {len(tests)}")

def test_json() -> None:
    test_dict = {"a": 1, "b word": {"b2": "asda asd\t "}}
    import json
    import re
    good = json.dumps(test_dict).encode()
    bad = str(test_dict).encode()
    regexp: re.Pattern[bytes] = re.compile(protocol.B_MSG_JSON)
    fails = []
    for test, outcome in zip((good, bad), (True, False)):
        a = regexp.search(test)
        if (a is not None and outcome is False) or (a is None and outcome is True):
            fails.append(test)
    
    for fail in fails:
        print(f"Failed : {fail}")


def test_msg():
    msgs = [
        "FOO.BAR 9 11\r\nHello World\r\n".encode(),
        "FOO.BAR 9 GREETING.34 11\r\nHello World\r\n".encode(),
    ]
    fails = []
    for msg in msgs:
        a = protocol.RE_MSG_BODY.match(msg)
        if a is None:
            fails.append(msg)
            continue
    if not fails:
        print("Passed all MSG tests")
        return


def test_hmsg():
    msgs = [
        "FOO.BAR 9 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n".encode(),
        "FOO.BAR 9 BAZ.69 34 45\r\nNATS/1.0\r\nFoodGroup: vegetable\r\n\r\nHello World\r\n".encode(),
    ]
    fails = []

    for msg in msgs:
        a = protocol.RE_HMSG_BODY.match(msg)
        if a is None:
            fails.append(msg)

    if not fails:
        print("Passed all HMSG tests")
    else:
        print(f"Failed HMSG tests: {fails}")


if __name__ == "__main__":
    test_delimiters()
    test_json()
    test_msg()
    test_hmsg()