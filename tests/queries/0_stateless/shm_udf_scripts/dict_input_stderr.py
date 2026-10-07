#!/usr/bin/python3

# A pooled command that answers each key and writes a diagnostic to `stderr` - written before the
# row, so it is already on the pipe whenever the server has its row. Before the row is written at
# all, not merely before it is flushed: CI runs Python with `PYTHONUNBUFFERED`, where `write` is the
# flush.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        updated_line = line.replace("\n", "")

        sys.stderr.write("the command complains\n")
        sys.stderr.flush()

        sys.stdout.write(updated_line + "\t" + "Key " + updated_line + "\n")
        sys.stdout.flush()
