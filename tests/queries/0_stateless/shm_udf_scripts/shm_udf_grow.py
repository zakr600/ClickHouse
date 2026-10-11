#!/usr/bin/python3

# Executable UDF used to exercise adaptive growth of the shared-memory region.
#
# It echoes the input back unchanged, writing the result at offset 0 (over the already-consumed
# input). Because the output is exactly as large as the input, it always fits into a region that
# the server has just grown to hold the input -- which lets a test drive region growth without
# also having to reserve room for a larger output. The control protocol is identical to shm_udf.py.

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
            break  # stdin closed -> exit
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
                # Copy the input out, then write it back at offset 0. `input_data` is an independent
                # bytes object, so overwriting the region afterwards is safe.
                input_data = bytes(region[input_offset : input_offset + input_size])
                region[0:input_size] = input_data
                region.flush()
                output_offset, output_size = 0, input_size
            finally:
                region.close()

            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_OK)
            write_varint(stdout, output_offset)
            write_varint(stdout, output_size)
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
