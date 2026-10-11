#!/usr/bin/python3

# A pooled shared-memory UDF that reports whether the region past its input is clean, and then
# dirties it. Otherwise identical to shm_udf.py. `--peek-at OFFSET` probes a fixed offset instead,
# and `--extend-to BYTES` first extends the region's file past the server's mapping, so that the
# probe can look at a tail the server never mapped.
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
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import (  # noqa: E402
    PROTOCOL_VERSION,
    STATUS_ERROR,
    STATUS_NEED_MORE_SPACE,
    STATUS_OK,
    read_varint,
    write_string_binary,
    write_varint,
)


class NeedMoreSpace(Exception):
    def __init__(self, required_size):
        super().__init__(
            f"the shared-memory region must be at least {required_size} bytes"
        )
        self.required_size = required_size


def process(input_data, region, region_size):
    # Reports, for every input row, whether the probed stretch of the region still holds anything:
    # "clean" if the 4 KiB right after the input (or at `--peek-at`) are all zero, "dirty" otherwise. A
    # previous request's larger input or output leaves its bytes there unless the server scrubbed
    # them; this is how the test sees whether it did. Then overwrites that stretch, so that the next
    # request finds it dirty unless scrubbed again.
    probe_from = (
        int(sys.argv[sys.argv.index("--peek-at") + 1])
        if "--peek-at" in sys.argv
        else len(input_data)
    )
    probe = bytes(region[probe_from : probe_from + 4096])
    verdict = b"clean" if probe.strip(b"\x00") == b"" else b"dirty"
    output = bytearray()
    for line in input_data.split(b"\n"):
        if line == b"":
            continue
        output += verdict + b"\n"
    region[probe_from : probe_from + 4096] = b"x" * 4096

    output_offset = len(input_data)  # write the result right after the input
    if output_offset + len(output) > region_size:
        # Only the server can resize the region: ask it for one that fits and it re-sends the
        # same request over the larger mapping.
        raise NeedMoreSpace(output_offset + len(output))

    region[output_offset : output_offset + len(output)] = bytes(output)
    region.flush()
    return output_offset, len(output)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
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
                # Extend the file past the server's mapping once (`--extend-to`): the tail the
                # server never mapped is where a scrub that only covered the mapping would miss.
                if "--extend-to" in sys.argv:
                    extend_to = int(sys.argv[sys.argv.index("--extend-to") + 1])
                    if os.fstat(fd).st_size < extend_to:
                        os.ftruncate(fd, extend_to)
                region = mmap.mmap(fd, 0)
            finally:
                os.close(fd)

            try:
                region_size = len(region)
                input_data = region[input_offset : input_offset + input_size]
                output_offset, output_size = process(input_data, region, region_size)
            finally:
                region.close()

            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_OK)
            write_varint(stdout, output_offset)
            write_varint(stdout, output_size)
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
