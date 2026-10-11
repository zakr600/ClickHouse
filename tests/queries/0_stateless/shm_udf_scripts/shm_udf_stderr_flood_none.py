#!/usr/bin/python3

# A pooled worker that answers correctly and then writes far more to `stderr` than a pipe can hold,
# before going back to read the next request.
#
# Configured with `stderr_reaction` `none`, which documents that a chatty command never blocks on a
# full `stderr` pipe. That promise is easy to keep while a query is running - the read loop drains
# both pipes - and easy to lose at the moment the worker is handed back to the pool: nothing is
# reading it any more, `none` means the bytes are nobody's, and a worker returned with a full pipe
# is a worker blocked in `write` that will never read the next request. The borrow after it then
# waits out `command_read_timeout` for a process that is stuck.

import mmap
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import (  # noqa: E402
    PROTOCOL_VERSION,
    STATUS_OK,
    read_varint,
    write_varint,
)

# Twice the 64 KiB a Linux pipe holds by default, so the command is provably blocked partway.
CHATTER = b"e" * (128 * 1024)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    stderr = sys.stderr.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")
        request_id = read_varint(stdin)

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
                    output += str(os.getpid()).encode("ascii") + b"\n"

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

        # Answer first, then talk. Blocks partway unless somebody keeps reading.
        stderr.write(CHATTER)
        stderr.flush()


if __name__ == "__main__":
    main()
