#!/usr/bin/env python3
"""Test protocol related things"""

import os
import sys

try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "pynats"))
    import protocol
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
if __name__ == "__main__":
    test_delimiters()
    test_json()