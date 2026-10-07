#!/usr/bin/python3

# A source that produces its rows and writes a diagnostic to `stderr`. The diagnostic is written
# before the rows - not merely before they are flushed, since CI runs Python with
# `PYTHONUNBUFFERED`, where every write goes straight to the pipe - so it is already waiting
# whenever the server has the rows and what the server does about it does not depend on scheduling.

import sys

if __name__ == "__main__":
    sys.stderr.write("the source complains\n")
    sys.stderr.flush()

    print("1" + "\t" + "Value 1", end="\n")
    print("2" + "\t" + "Value 2", end="\n")
    print("3" + "\t" + "Value 3", end="\n")

    sys.stdout.flush()
