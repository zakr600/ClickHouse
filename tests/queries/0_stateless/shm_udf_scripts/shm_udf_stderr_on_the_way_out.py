#!/usr/bin/python3

# Answers correctly, closes its `stdout`, waits long enough for the server to give up draining, and
# only then writes its diagnostic to `stderr` before exiting.
#
# The pause is the whole point. The drain that runs when `stdout` ends stops as soon as `stderr`
# goes quiet for a moment, so a line written well after that is not found there - it is found by
# the bounded wait that reaps the command, which is the last stretch in which a command can write
# anything at all. Those bytes are still output the command produced, and `stderr_reaction` `throw`
# promises that output fails the query. Read and dropped on the floor, the query would succeed and
# the setting would quietly mean nothing on the way out.

import mmap
import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import (  # noqa: E402
    PROTOCOL_VERSION,
    STATUS_OK,
    read_varint,
    write_varint,
)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    stderr = sys.stderr.buffer

    version = read_varint(stdin)
    request_id = read_varint(stdin)
    if version != PROTOCOL_VERSION:
        raise RuntimeError(f"unsupported protocol version {version}")

    path_length = read_varint(stdin)
    path = stdin.read(path_length).decode("utf-8")
    input_offset = read_varint(stdin)
    input_size = read_varint(stdin)

    fd = os.open(path, os.O_RDWR)
    try:
        region = mmap.mmap(fd, 0)
    finally:
        os.close(fd)

    try:
        output = bytearray()
        for line in region[input_offset : input_offset + input_size].split(b"\n"):
            if line != b"":
                output += b"Key " + line + b"\n"

        output_offset = input_size
        region[output_offset : output_offset + len(output)] = bytes(output)
        region.flush()
    finally:
        region.close()

    write_varint(stdout, request_id)
    write_varint(stdout, STATUS_OK)
    write_varint(stdout, output_offset)
    write_varint(stdout, len(output))
    stdout.flush()

    # End the conversation, then outlast the drain's idle window (100 ms) by a wide margin.
    os.close(1)
    time.sleep(1)

    stderr.write(b"complaining on the way out\n")
    stderr.flush()

    sys.exit(0)


if __name__ == "__main__":
    main()
