#!/usr/bin/python3

# Misbehaving UDF: reads a request and reports success with an out-of-bounds output region.
# The server must reject the response instead of reading past the shared-memory region.

import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import read_varint, write_varint  # noqa: E402


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break
        request_id = read_varint(stdin)

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        region_size = os.path.getsize(path)

        # Success status, but the output claims to live past the end of the region.
        write_varint(stdout, request_id)
        write_varint(stdout, 0)
        write_varint(stdout, region_size + 1024)  # bogus offset
        write_varint(stdout, 16)  # bogus size
        stdout.flush()


if __name__ == "__main__":
    main()
