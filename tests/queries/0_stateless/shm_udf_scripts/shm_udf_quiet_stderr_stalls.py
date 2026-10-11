#!/usr/bin/python3

# A UDF that closes its own `stderr` and then never answers.
#
# Closing `stderr` is legal and ordinary (see `shm_udf_quiet_stderr.py`); what this script is about
# is what the server does with that pipe afterwards. Once the only writer is gone, `poll` reports
# `POLLHUP` on it immediately and forever, and a read of it returns zero bytes. A server that keeps
# that descriptor in the set it waits on therefore never waits at all: it spins, burning a core, and
# `command_read_timeout` - which is measured by the poll it is no longer doing - never fires. The
# query then hangs for good on a command that has merely stopped talking.
#
# So the descriptor has to be dropped from the poll set at EOF, and this command has to fail with a
# read timeout like any other command that does not answer.

import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import read_varint  # noqa: E402


def main():
    stdin = sys.stdin.buffer

    # Before anything else, so the pipe is already hung up when the server starts waiting.
    os.close(2)

    # Read the request in full and then simply stop, without writing a single byte to stdout.
    read_varint(stdin)
    read_varint(stdin)  # request id; this command never answers
    path_length = read_varint(stdin)
    if path_length is not None:
        stdin.read(path_length)
        read_varint(stdin)
        read_varint(stdin)

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
