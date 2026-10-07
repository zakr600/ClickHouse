#!/usr/bin/python3

# A pooled shared-memory UDF that outlives its own discarding. It answers its request with a stray
# byte after the response frame - in the same write, so the server's hand-back probe is sure to see
# it and discard the worker - and when its stdin reaches EOF it does not exit: it ignores `SIGTERM`
# and, for a few seconds, writes how many bytes of pages its own copy of the region's descriptor
# still has behind it to the file named by its argument. A process the server has given up on keeps
# the descriptor it inherited; what the test looks at is whether it keeps the pages too.
#
# Protocol (all control values use the ClickHouse native binary encoding):
#   server -> stdin : varint version, varint request id, varint path length + path bytes,
#                     varint input offset, varint input size
#   stdout <- server: the request id echoed back, varint status (0 = ok), then on success
#                     varint output offset + varint output size; status 2 asks the server for a
#                     larger region and is followed by the varint total size needed; any other
#                     status is followed by a length-prefixed error message
# The bulk data lives in the shared-memory file at the given path; the pipes carry only
# these small control commands. When stdin reaches EOF the process exits.

import mmap
import signal
import time
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1
STATUS_NEED_MORE_SPACE = 2


class NeedMoreSpace(Exception):
    def __init__(self, required_size):
        super().__init__(
            f"the shared-memory region must be at least {required_size} bytes"
        )
        self.required_size = required_size


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


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def write_varint(stream, value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    stream.write(bytes(out))


def write_string_binary(stream, text):
    encoded = text.encode("utf-8")
    write_varint(stream, len(encoded))
    stream.write(encoded)


def process(input_data, region, region_size):
    # A function without arguments: the request carries no input at all (offset 0, size 0) and the
    # answer is one row that depends on none. The server still has to make that request - there is
    # nothing else that would make this command produce a row.
    if "--zero-argument" in sys.argv:
        output = b"42\n"
        output_offset = len(input_data)
        if output_offset + len(output) > region_size:
            raise NeedMoreSpace(output_offset + len(output))
        region[output_offset : output_offset + len(output)] = output
        region.flush()
        return output_offset, len(output)

    # Input format is TabSeparated: one UInt64 per line. With `--echo` each row is answered with
    # itself, so the query decides whether the answer parses as the return type.
    output = bytearray()
    for line in input_data.split(b"\n"):
        if line == b"":
            continue
        if "--echo" in sys.argv:
            output += line + b"\n"
        elif "--report-pid" in sys.argv:
            output += str(os.getpid()).encode("ascii") + b"\n"
        else:
            output += b"Key " + line + b"\n"

    output_offset = len(input_data)  # write the result right after the input
    if output_offset + len(output) > region_size:
        # Only the server can resize the region: ask it for one that fits and it re-sends the
        # same request over the larger mapping.
        raise NeedMoreSpace(output_offset + len(output))

    region[output_offset : output_offset + len(output)] = bytes(output)
    region.flush()
    return output_offset, len(output)


REGION_FD = 3


def linger():
    # Discarded: the server closed stdin. Stay, deaf to `SIGTERM`, and report the pages behind the
    # inherited descriptor of the region - `st_blocks` is in 512-byte units.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    report = sys.argv[1]
    for _ in range(100):
        committed = os.fstat(REGION_FD).st_blocks * 512
        with open(report + ".tmp", "w") as out:
            out.write(f"{committed}\n")
        os.rename(report + ".tmp", report)
        time.sleep(0.1)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            linger()
            return
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
                region_size = len(region)
                input_data = region[input_offset : input_offset + input_size]
                output_offset, output_size = process(input_data, region, region_size)
            finally:
                region.close()

            # The response frame and the stray byte in one write: written field by field, they
            # would go out in separate writes wherever `stdout` is unbuffered (`PYTHONUNBUFFERED`,
            # set in CI), and the server could pronounce the worker clean before the byte arrived.
            stdout.write(
                encode_varint(request_id)
                + encode_varint(STATUS_OK)
                + encode_varint(output_offset)
                + encode_varint(output_size)
                + b"\n"
            )
        except NeedMoreSpace as need_more_space:
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_NEED_MORE_SPACE)
            write_varint(stdout, need_more_space.required_size)
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
