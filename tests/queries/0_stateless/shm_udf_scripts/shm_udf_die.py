#!/usr/bin/python3

# Misbehaving UDF: reads a request and then exits without answering, simulating a crashed
# command. The server must surface an error rather than hang or return wrong results.

import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import read_varint  # noqa: E402


def main():
    stdin = sys.stdin.buffer

    version = read_varint(stdin)
    if version is None:
        return
    read_varint(stdin)  # request id; this command never answers

    path_length = read_varint(stdin)
    stdin.read(path_length)  # path
    read_varint(stdin)  # input offset
    read_varint(stdin)  # input size

    # Die without writing a response.
    sys.exit(1)


if __name__ == "__main__":
    main()
