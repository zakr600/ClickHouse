#!/usr/bin/python3

# A pooled command that answers its first key, closes its stdout and then stays alive far longer
# than the `command_termination_timeout` it is configured with. A worker that hung up its stdout
# cannot serve anyone else, so it is discarded - and whether its exit code is wanted decides
# whether the request fails or the process is simply signalled once the budget is spent.

import os
import sys
import time

if __name__ == "__main__":
    line = sys.stdin.readline().replace("\n", "")
    # The answer has no trailing newline: its last field ends at EOF, so the server cannot finish
    # reading it before the descriptor below is closed, and by the time it decides whether to keep
    # the worker the hangup is already there to be seen. With the newline, the answer would be
    # complete before the close, and a server that checked in between would put the worker back
    # in the pool as healthy - and the request would not wait for it at all.
    sys.stdout.write(line + "\t" + "Key " + line)
    sys.stdout.flush()
    # `sys.stdout.close()` leaves the descriptor open (the standard streams are opened with
    # `closefd=False`); the server only sees EOF once the descriptor itself is closed.
    os.close(sys.stdout.fileno())

    time.sleep(60)
