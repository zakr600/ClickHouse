#!/usr/bin/python3

# A pooled command that answers the first row only, closes its stdout, and then neither reads its
# stdin nor exits: it stays until it is killed. It answered short, so it cannot go back to the pool,
# and nothing it does ends it. Its pid is written to the file named by `--pid-file`, for the test to
# check that the server did not leave it behind as a zombie.

import os
import sys
import time

if __name__ == "__main__":
    pid_file = sys.argv[sys.argv.index("--pid-file") + 1]
    line = sys.stdin.readline()
    if line:
        os.write(1, ("Key " + line.strip() + "\n").encode())
    os.close(1)
    with open(pid_file, "w") as f:
        f.write(f"{os.getpid()}\n")
    while True:
        time.sleep(1)
