#!/usr/bin/python3

# Misbehaving executable UDF: for each input row it writes the row TWICE, so it produces more
# output rows than requested. The server must detect the overproduction, fail the query with a
# "wrong result" error, and (for executable_pool) invalidate the worker instead of returning it
# to the pool as valid.

import mmap
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import (  # noqa: E402
    PROTOCOL_VERSION,
    STATUS_ERROR,
    STATUS_OK,
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
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        try:
            if version != PROTOCOL_VERSION:
                raise ValueError(f"unsupported protocol version {version}")

            fd = os.open(path, os.O_RDWR)
            try:
                region = mmap.mmap(fd, 0)
            finally:
                os.close(fd)

            try:
                input_data = bytes(region[input_offset : input_offset + input_size])
                output = bytearray()
                for line in input_data.split(b"\n"):
                    if line == b"":
                        continue
                    output += line + b"\n"  # original row
                    output += line + b"\n"  # duplicate -> too many rows
                region[0 : len(output)] = bytes(output)
                region.flush()
            finally:
                region.close()

            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_OK)
            write_varint(stdout, 0)
            write_varint(stdout, len(output))
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
