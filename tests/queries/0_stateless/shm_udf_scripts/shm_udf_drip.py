#!/usr/bin/python3

# Executable UDF over the shared-memory protocol (see `shm_udf.py`) that answers every request with
# an error frame, written one byte at a time with a pause before each: every pause is shorter than
# `command_read_timeout`, the whole frame takes longer. When stdin reaches EOF the process exits.
#
# Arguments: the pause in seconds, the length of the error message.

import sys
import time

STATUS_ERROR = 1


def read_varint(stream):
    result = 0
    shift = 0
    while True:
        chunk = stream.read(1)
        if not chunk:
            return None
        byte = chunk[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7


def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    pause = float(sys.argv[1])
    message = b"x" * int(sys.argv[2])

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        request_id = read_varint(stdin)
        stdin.read(read_varint(stdin))  # path
        read_varint(stdin)  # input offset
        read_varint(stdin)  # input size

        frame = varint(request_id) + varint(STATUS_ERROR) + varint(len(message)) + message
        for i in range(len(frame)):
            time.sleep(pause)
            stdout.write(frame[i : i + 1])
            stdout.flush()


if __name__ == "__main__":
    main()
