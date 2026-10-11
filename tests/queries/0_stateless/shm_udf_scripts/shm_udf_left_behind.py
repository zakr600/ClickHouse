#!/usr/bin/python3

# A shared-memory UDF that leaves processes behind when it exits. At startup it forks two
# descendants that keep the inherited descriptor of the region (and nothing else): one stays in the
# command's process group and sleeps; the other leaves the group (`setsid`), waits for a file `go` to
# appear in the directory named by the argument, then writes the number of blocks the region's file
# holds into `left_blocks` there and exits. The command itself answers normally and exits when its
# stdin reaches EOF. The pids go into the same directory, as `worker`, `in_group` and `left_group`.
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
import time
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


def write_file(directory, name, text):
    with open(os.path.join(directory, name + ".tmp"), "w") as out:
        out.write(text)
    os.rename(os.path.join(directory, name + ".tmp"), os.path.join(directory, name))


def write_pid(directory, name):
    write_file(directory, name, f"{os.getpid()}\n")


def keep_only_the_region():
    os.closerange(0, REGION_FD)
    os.closerange(REGION_FD + 1, 1024)


def start_in_group(directory):
    if os.fork() != 0:
        return
    keep_only_the_region()
    write_pid(directory, "in_group")
    time.sleep(60)
    os._exit(0)


def start_left_group(directory):
    if os.fork() != 0:
        return
    os.setsid()
    keep_only_the_region()
    write_pid(directory, "left_group")
    deadline = time.monotonic() + 60
    while not os.path.exists(os.path.join(directory, "go")) and time.monotonic() < deadline:
        time.sleep(0.05)
    write_file(directory, "left_blocks", f"{os.fstat(REGION_FD).st_blocks}\n")
    os._exit(0)


def wait_for(directory, name):
    while not os.path.exists(os.path.join(directory, name)):
        time.sleep(0.01)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    directory = sys.argv[1]
    start_in_group(directory)
    start_left_group(directory)
    # Both descendants are where they belong (`left_group` out of the group) before the first answer.
    wait_for(directory, "in_group")
    wait_for(directory, "left_group")
    write_pid(directory, "worker")

    while True:
        version = read_varint(stdin)
        if version is None:
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
