#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: the commands are waited for through `/proc`

# A pooled worker of the pipe transport that a borrow finds unfit - here, one that wrote a row to its
# stdout once it was back in the pool - is discarded before anything is built on it. Nobody is
# interested in how it exits, so it is not given `command_termination_timeout` (set far beyond the
# time limit of the test, so that waiting it out would be a hang): it is killed and reaped at once, and
# does not stay behind as a zombie - even though it does not exit on stdin EOF.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

GO="$SHM_UDF_WORK/go"
MARKER="$SHM_UDF_WORK/marker"

{
    echo "<function><type>executable_pool</type><name>pipe_late_stdout_lingers</name><return_type>UInt64</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<command_termination_timeout>86400</command_termination_timeout>"
    echo "<command>pipe_pool_late_stdout.py --go $GO --marker $MARKER --linger</command></function>"
} | shm_functions

shm_local "
    CREATE TABLE pids (n UInt8, pid UInt64) ENGINE = Memory;
    INSERT INTO pids SELECT 1, pipe_late_stdout_lingers(0);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh file $MARKER', TSV, 'ok UInt8', (SELECT 0));
    INSERT INTO pids SELECT 2, pipe_late_stdout_lingers(1);
    SELECT uniqExact(pid) FROM pids;
    SELECT * FROM executable('shm_wait.sh reaped', TSV, 'ok UInt8', (SELECT pid FROM pids WHERE n = 1));
"
