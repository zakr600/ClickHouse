#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A discarded worker that does not go away. The command answers with a stray byte after its
# response, so the hand-back discards it; it ignores `SIGTERM`, so it is still running - and still
# holds the descriptor of the region it inherited - when the teardown's grace period
# (`command_termination_timeout`) is spent, the server gives up on it and drops the region together
# with its charge. The pages must go with the charge: the server frees every page of the file when it
# drops the region, whoever else still holds it. The command reports what is left behind its own
# descriptor, and that is nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

REPORT="$SHM_UDF_WORK/committed"

{
    echo "<function><type>executable_pool</type><name>shm_lingering_holder</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>"
    echo "<command_termination_timeout>1</command_termination_timeout><check_exit_code>0</check_exit_code>"
    echo "<command>shm_udf_lingering_holder.py $REPORT</command></function>"
} | shm_functions

shm_local "
    SELECT shm_lingering_holder(1);
    SELECT sum(value) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"

# The command reports every 100 ms for 10 seconds; wait for a report of nothing, or take the last one.
committed=""
for _ in $(seq 1 100); do
    committed=$(cat "$REPORT" 2>/dev/null)
    [[ "$committed" == 0 ]] && break
    sleep 0.1
done
echo "${committed:-no report}"
