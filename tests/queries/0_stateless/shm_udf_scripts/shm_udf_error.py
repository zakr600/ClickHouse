#!/usr/bin/python3

# Misbehaving UDF: reads a request and always answers through the protocol's error channel
# (a non-zero status followed by a length-prefixed message). The server must fail the query
# with that message.

import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import (  # noqa: E402
    STATUS_ERROR,
    read_varint,
    write_string_binary,
    write_varint,
)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break
        request_id = read_varint(stdin)

        path_length = read_varint(stdin)
        stdin.read(path_length)
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_ERROR)
        write_string_binary(stdout, "the command cannot process this request")
        stdout.flush()


if __name__ == "__main__":
    main()
