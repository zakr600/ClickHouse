#!/usr/bin/python3
# A pooled shared-memory UDF that answers, waits until the query it answered is over (`--go`, see
# `go_signal.py`), writes a diagnostic to stderr and exits in the pool. See
# `pipe_pool_stderr_then_late_exit.py` for the pipe counterpart.
import mmap
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from go_signal import wait_for_go  # noqa: E402
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
                # The pid, so that the test can tell a replacement from a reused worker.
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

    # Once the query just served is over, complain and go: the worker dies in the pool with its
    # last words unread, and the next borrow has to report them.
    if not wait_for_go():
        return
    stderr.write(b"last words of the worker\n")
    stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
