#include <Processors/Sources/ShellCommandSharedMemorySource.h>

#include <climits>
#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/UDFProcessSubtreeSampler.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ErrnoException.h>
#include <Common/scope_guard_safe.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>

#include <Common/SharedMemoryRegion.h>
#include <Common/FailPoint.h>
#include <Formats/formatBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Core/Block.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Field.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>

#include <boost/circular_buffer.hpp>
#include <fmt/ranges.h>

#include <csignal>
#include <ranges>

#include <Processors/Sources/ShellCommandHolder.h>
#include <Processors/Sources/ShellCommandSourceHelpers.h>
#include <Processors/Sources/TimeoutPipeBuffers.h>

namespace CurrentMetrics
{
    extern const Metric MemoryTrackingUnmeasured;
}

namespace ProfileEvents
{
    extern const Event ExecutableUDFSharedMemoryCalls;
    extern const Event ExecutableUDFSharedMemoryInputBytes;
    extern const Event ExecutableUDFSharedMemoryOutputBytes;
    extern const Event ExecutableUDFSharedMemoryRegionGrowths;
    extern const Event ExecutableUDFSharedMemoryAllocatedBytes;
    extern const Event ExecutableUDFSharedMemoryDirtyChannelDiscards;
    extern const Event ExecutableUDFSharedMemoryScrubbedBytes;
}

namespace DB
{

namespace FailPoints
{
    extern const char executable_udf_fail_handback_measurement[];
}

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int LOGICAL_ERROR;
    extern const int UDF_EXECUTION_FAILED;
    extern const int UNSUPPORTED_METHOD;
}

/// How much a shared-memory region is enlarged at once while the query's memory limit cannot be
/// enforced (see `ensureRegionFits`). Small enough that going over the limit stays bounded, large
/// enough that flushing a buffer's worth of trailing bytes does not remap the region per byte.
static constexpr size_t UNENFORCED_GROWTH_STEP = 1024 * 1024;

/// Version of the shared-memory control protocol between the server and an executable UDF.
/// Sent as the first varint of every request so the child can detect an incompatible protocol
/// version and answer with an error status.
static constexpr UInt64 SHARED_MEMORY_PROTOCOL_VERSION = 1;


/// Identifies one request, and has to be unguessable rather than merely unique.
///
/// A counter would be enough to catch a command that writes junk: the junk lands where the id
/// belongs and does not match. It is not enough to catch a command that is wrong in a more
/// systematic way - one that, having answered request N, writes a whole plausible frame for N+1
/// before it is asked. That frame would match, and the next query would be handed an answer
/// computed for somebody else's input. Drawing the id at random takes that away: a command cannot
/// write the answer to a question it has not been asked yet.
static UInt64 generateRequestId()
{
    static thread_local pcg64_fast rng(randomSeed());
    return rng();
}

/// Status codes returned by the child in the first varint of every response.
static constexpr UInt64 SHARED_MEMORY_STATUS_OK = 0;

/// The child cannot fit its result into the region and asks for a bigger one. The status is
/// followed by a varint with the total region size (in bytes) the child needs. The server enlarges
/// the region (up to `shared_memory_max_size`) and re-sends the same request; enlarging preserves
/// the file contents, so the serialized input is still in place. Any other non-zero status is an
/// error followed by a message.
static constexpr UInt64 SHARED_MEMORY_STATUS_NEED_MORE_SPACE = 2;

/// Upper bound on the length of an error message read from the child on the failure path.
static constexpr size_t SHARED_MEMORY_MAX_ERROR_MESSAGE_SIZE = 64 * 1024;

namespace
{

    /** Serializes straight into a shared-memory region, enlarging it when it fills up.
      *
      * Going through a heap buffer first would make the serialized chunk exist twice at the same
      * time - once on the heap and once in the region - doubling the peak memory of a call, and it
      * would copy every byte, which is what this transport exists to avoid. Writing through the
      * mapping is safe because the region is sealed against shrinking (see `SharedMemoryRegion`):
      * the command cannot take a page out from under this buffer.
      *
      * `grow` must make the region hold at least `required` bytes or throw. Enlarging a region
      * replaces its mapping, so `data` is re-read after every growth; the bytes written so far
      * survive it, because the file behind the mapping keeps its contents.
      */
    class WriteBufferToSharedMemoryRegion : public WriteBuffer
    {
    public:
        using GrowFn = std::function<void(size_t required)>;

        WriteBufferToSharedMemoryRegion(SharedMemoryRegion & region_, GrowFn grow_)
            : WriteBuffer(region_.data(), region_.size())
            , region(region_)
            , grow(std::move(grow_))
        {
        }

    private:
        /// Called once the working buffer is used up - which also happens on the flush that ends the
        /// serialization, so a region filled to its last byte does not by itself mean that the region
        /// is too small. One spare byte outside the region tells the two cases apart: a writer that
        /// still has something to say lands in that byte and comes back here, and only then is the
        /// region really enlarged. Without it, input that ends exactly at the end of the region would
        /// demand one byte more than it needs - a needless growth, or a failed query when the region
        /// is not allowed to grow at all (`shared_memory_max_size` defaults to `shared_memory_size`).
        void nextImpl() override
        {
            /// `bytes` is only updated after this returns, so this is exactly what has been written,
            /// wherever it went.
            size_t written = count();

            if (written > region.size())
                moveOverflowIntoRegion(written);

            if (written == region.size())
            {
                set(overflow.data(), overflow.size());
                return;
            }

            set(region.data() + written, region.size() - written);
        }

        /// The serialization is over and everything is already in the region, so there is nothing
        /// to flush here. In particular the region must not grow from this method: `finalize`
        /// blocks memory-limit exceptions for its whole body, so a growth started here would commit
        /// its pages without `max_memory_usage` ever being enforced. Draining the spare byte is the
        /// writer's job (any `next` does it, under the memory limit) - see serializeInput, which is
        /// also why this buffer is never finalized implicitly from a destructor.
        ///
        /// This only covers growth this buffer starts itself. A format that wraps this one
        /// (`WriteBufferValidUTF8`, `PeekableWriteBuffer`) flushes into it from inside its own
        /// `finalize`, under the same blocked limit, and a growth from there cannot be checked
        /// against `max_memory_usage` either - the bytes are still charged to the query, only the
        /// limit is not enforced for them. `ensureRegionFits` keeps that bounded by growing in a
        /// small step instead of doubling whenever it sees the limit blocked, and serializeInput
        /// enforces the limit once the format is done.
        void finalizeImpl() override
        {
            if (offset() != 0 && working_buffer.begin() == overflow.data())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Shared-memory region buffer is finalized with a byte past the end of the region; "
                    "it has to be flushed with `next` first");

            bytes += offset();
            set(nullptr, 0);
        }

        /// Enlarges the region to hold everything written so far (this throws when it cannot) and
        /// copies the spare byte into the space that opened up.
        void moveOverflowIntoRegion(size_t written)
        {
            size_t old_size = region.size();
            grow(written);
            memcpy(region.data() + old_size, overflow.data(), written - old_size);
        }

        SharedMemoryRegion & region;
        GrowFn grow;
        /// Catches a writer stepping past the end of the region; see nextImpl.
        std::array<char, 1> overflow{};
    };

    /** What one borrow has charged its query's memory tracker for the mmap'd shared-memory region.
      *
      * These are synthetic charges: the region is a mapped `memfd`, not a heap allocation at a known
      * address. We therefore intentionally do NOT emit allocation-profiler samples
      * (AllocationTrace::onAlloc / onFree) for them — a sample carrying a fake pointer would only
      * pollute allocation profiles. The memory-tracker counter (used for the memory limit) is
      * still updated by `alloc` / `free` regardless, and so is `MemoryTrackingUnmeasured`: the
      * pages are not in the measurement `MemoryWorker` corrects the global tracker with, and
      * without it the share of this charge that reaches the global tracker would be gone on its
      * next tick.
      *
      * Charged and released on the query thread. Nothing is released implicitly: the source gives
      * the whole charge back in its `cleanup` (`releaseAll`), after the worker and the region it
      * covers are gone.
      */
    class QueryMemoryCharge
    {
    public:
        /// May throw MEMORY_LIMIT_EXCEEDED before it records the charge, leaving `amount` unchanged.
        void charge(size_t bytes)
        {
            [[maybe_unused]] auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(bytes));
            try
            {
                /// These synthetic charges are large enough to matter on their own. Do not let them
                /// sit below `max_untracked_memory`, because the region's pages are committed right
                /// after this method returns.
                CurrentThread::flushUntrackedMemory();
                CurrentMemoryTracker::check();
            }
            catch (...)
            {
                [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
                CurrentThread::flushUntrackedMemory();
                throw;
            }

            charged_bytes += bytes;
            CurrentMetrics::add(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));
        }

        /// Same, for a path that is already unwinding. The bytes are committed whatever happens to
        /// this growth, so the query has to be charged for them; but a `MEMORY_LIMIT_EXCEEDED`
        /// raised over them here would replace the exception on its way out - the one that says
        /// what actually went wrong with the growth.
        ///
        /// Blocked rather than caught, the way `~SharedMemoryRegion` blocks them: a caught
        /// `MEMORY_LIMIT_EXCEEDED` is a charge that was rolled back, which would leave committed
        /// pages counted nowhere at all. With the exception blocked the tracker takes the bytes -
        /// over the limit, which is what actually happened - and `cleanup` gives them back with
        /// the rest of the borrow's charge.
        void chargeNoThrow(size_t bytes) noexcept
        {
            LockMemoryExceptionInThread block_exceptions(VariableContext::Global);
            try
            {
                charge(bytes);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot charge the query for the pages a failed growth committed");
            }
        }

        void uncharge(size_t bytes)
        {
            [[maybe_unused]] auto trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
            CurrentThread::flushUntrackedMemory();
            charged_bytes -= bytes;
            CurrentMetrics::sub(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));
        }

        /// Brings the query's charge from `charged` - what was charged ahead of an operation that
        /// commits pages - to `settled`, what the re-read after it found: gives back what the
        /// operation turned out not to need, or charges what it committed beyond the bound (may
        /// throw MEMORY_LIMIT_EXCEEDED).
        void settle(size_t charged, size_t settled)
        {
            if (settled < charged)
                uncharge(charged - settled);
            else if (settled > charged)
                charge(settled - charged);
        }

        /// Gives the whole charge back.
        void releaseAll()
        {
            if (charged_bytes)
                uncharge(charged_bytes);
        }

        size_t amount() const { return charged_bytes; }

    private:
        size_t charged_bytes = 0;
    };

    /** Exchanges data with the child process through a shared-memory region instead of the pipes.
      *
      * The protocol is strictly lock-step and driven synchronously from `generate` (see the full
      * specification in docs/reference/functions/regular-functions/udf.mdx, "Shared memory mode"):
      *   1. pull the next input chunk and serialize it into the shared-memory region;
      *   2. write a request to the child's stdin:
      *        varint version, varint request id, string path, varint input offset, varint input size;
      *   3. read the response from the child's stdout: the request id echoed back, a varint status,
      *        then, on success, varint output offset + varint output size; on failure, a
      *        length-prefixed error message;
      *   4. deserialize the output from the region.
      * Each request invalidates the previous contents of the region. The region is created once per
      * process (reused across pool borrows), so the loop keeps a single mapping for the whole session.
      */
    class ShellCommandSharedMemorySource final : public ISource
    {
    public:
        ShellCommandSharedMemorySource(
            ContextPtr context_,
            const ShellCommandSourceCoordinator::Configuration & coordinator_configuration,
            SharedHeader sample_block_,
            ShellCommandHolder::ShellCommandBuilderFunc build_command_,
            Pipe input_pipe_,
            const ShellCommandSourceConfiguration & configuration_,
            BorrowedShellCommandHolder && command_holder_)
            : ISource(sample_block_)
            , context(context_)
            , format(coordinator_configuration.format)
            , sample_block(sample_block_)
            , configuration(configuration_)
            , is_pooled(static_cast<bool>(command_holder_))
            , stderr_throws(coordinator_configuration.stderr_reaction == ExternalCommandStderrReaction::THROW)
            , check_exit_code(coordinator_configuration.check_exit_code)
            , shared_memory_max_size(coordinator_configuration.shared_memory_max_size)
            , shared_memory_max_footprint(SharedMemoryRegion::roundUpToPages(coordinator_configuration.shared_memory_max_size))
            , command_holder(std::move(command_holder_))
        {
            try
            {
                /// Create the region here (not in the caller) so that any failure — creating the
                /// `memfd`, reserving its storage, sealing or mapping it — is cleaned up by this
                /// constructor, which returns the borrowed process holder to the pool. On the
                /// pool path a region is created once and reused across borrows. The region is
                /// charged to this query for the whole borrow (see `acquireRegion`).

                /// Before anything is built on a reused worker, and before its region is taken
                /// over: what these probes find decides whether the region survives with it.
                inspectPooledWorkerBeforeTheBorrow();
                discardPooledWorkerOverTheCap();

                acquireRegion(coordinator_configuration.shared_memory_size);

                if (command_holder)
                    scrubRegionForBorrower();

                acquireCommand(build_command_);

                timeout_command_out = std::make_unique<TimeoutReadBufferFromFileDescriptor>(
                    command->out.getFD(), command->err.getFD(), coordinator_configuration.command_read_timeout_milliseconds, coordinator_configuration.stderr_reaction,
                    configuration_.sampler.get(), control_channel_buffer_size);

                context = makeContextForReadingCommandOutput(context, configuration.read_fixed_number_of_rows);

                timeout_command_in = std::make_unique<TimeoutWriteBufferFromFileDescriptor>(
                    command->in.getFD(), coordinator_configuration.command_write_timeout_milliseconds, configuration_.sampler.get(), control_channel_buffer_size);

                /// Before the first request, so that anything an earlier borrow's command wrote
                /// after its response is cleared off the pipe and reported against nobody, rather
                /// than read during this query and charged to it.
                discardStderrLeftByAPreviousBorrow();

                setUpInputPipeline(std::move(input_pipe_));

                borrow.constructor_finished = true;
            }
            catch (...)
            {
                /// Construction cannot send a protocol request, so a pooled command is still at a
                /// clean boundary and can be returned together with the holder - provided its
                /// pipes were wrapped, which `controlChannelIsClean` checks; a worker whose reader
                /// never came to be is discarded. A failure of the teardown itself must not replace
                /// the failure that got us here.
                borrow.command_can_be_reused = true;
                try
                {
                    cleanup();
                }
                catch (...)
                {
                    tryLogCurrentException("ShellCommandSharedMemorySource");
                }
                throw;
            }
        }

        ~ShellCommandSharedMemorySource() override
        {
            /// Destructors are noexcept, so nothing may escape - and `cleanup` is not allocation
            /// free (assigning an empty `QueryPipeline` allocates its processor list, handing the
            /// holder back to the pool grows a vector), which under a memory limit is exactly where
            /// an exception comes from.
            try
            {
                cleanup();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        String getName() const override { return "ShellCommandSharedMemorySource"; }

    protected:
        Chunk generate() override
        {
            try
            {
                while (true)
                {
                    if (output_executor)
                    {
                        Chunk chunk;
                        if (output_executor->pull(chunk))
                        {
                            /// `pull` can report success and still return an empty chunk (it makes
                            /// one execution step, which does not necessarily produce data). Such a
                            /// chunk would finish the source in `ISource::tryGenerate` and skip the
                            /// row-count checks below, so keep pulling instead of returning it.
                            if (!chunk.hasRows())
                                continue;

                            /// A command that produces more rows than requested violates the UDF
                            /// protocol. Detect it here — before the oversized chunk leaves the
                            /// source — so the exception below marks the command invalid and a
                            /// pooled worker is discarded instead of being reused as valid.
                            if (configuration.read_fixed_number_of_rows
                                && current_read_rows + chunk.getNumRows() > configuration.number_of_rows_to_read)
                                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                                    "Executable UDF wrong result, expected {} row(s), but the command produced more (at least {})",
                                    configuration.number_of_rows_to_read,
                                    current_read_rows + chunk.getNumRows());

                            current_read_rows += chunk.getNumRows();
                            return chunk;
                        }

                        destroyOutputPipeline();
                    }

                    /// On the pool path we cannot rely on stdin EOF; stop once enough rows were produced.
                    /// On the non-pooled path the child exits on stdin EOF, so close it before `wait`.
                    if (configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read)
                    {
                        closeStdinIfNeeded(is_pooled);
                        return {};
                    }

                    if (!sendNextRequest())
                    {
                        assertEnoughRowsRead();
                        return {};
                    }
                }
            }
            catch (...)
            {
                /// A failure while the next input is being prepared - the input pipeline itself, a
                /// region that cannot grow to hold the serialized block, an exceeded memory limit -
                /// never reached the child: no request was sent, so a pooled worker is still at a
                /// clean protocol boundary and it, together with its region, can be reused by the
                /// next borrow. Any other failure either leaves the child's state unknown (a
                /// partially written request, an unread response) or proves that it misbehaves, so
                /// the worker has to be discarded.
                if (borrow.preparing_input)
                    borrow.command_can_be_reused = true;
                else if (!borrow.command_can_be_reused)
                    borrow.command_is_invalid = true;
                throw;
            }
        }

        Status prepare() override
        {
            auto status = ISource::prepare();

            if (status == Status::Finished)
            {
                /// Decided once and used twice below: the answer includes a probe of the child's
                /// stdout, so asking again could give a different one, and closing stdin for a
                /// worker that is then not reaped - or reaping one whose stdin was left open, which
                /// makes the blocking wait sit out the whole `command_termination_timeout` - is
                /// exactly the mismatch the two uses have to avoid.
                const bool keep_command = commandIsReused();

                /// Reported here, while the command's pipes are still open. The wait below reaps a
                /// discarded child, and reaping closes `out` and `err` - after which their descriptor
                /// numbers may already belong to something else, so there is nothing left to read the
                /// leftover output from. `cleanup` reports instead on the paths that never reach this
                /// one (cancellation, an exception downstream); whichever runs first clears the flags,
                /// so a discard is reported once.
                reportDirtyChannelDiscard();

                /// Everything below this line takes the worker's `/proc` entries away: closing its
                /// stdin makes a discarded child exit, and a zombie has no `mm`, so its `VmHWM` is
                /// gone the moment it does; the wait after that reaps the pid altogether. So the
                /// borrow's CPU and peak resident set are read here, while there is still something
                /// to read - otherwise a discarded worker, which is exactly the case this teardown
                /// exists for, would report zeros. `cleanup` calls the same thing for the paths
                /// that never reach `prepare`; it is idempotent.
                if (!keep_command)
                    recordPooledResourceUsageNoThrow();

                /// The source can finish without `generate` reaching the end of the input — most
                /// notably on cancellation. A child that is not going back to the pool only exits
                /// once it sees EOF on its stdin, so close it before the blocking wait below, which
                /// reaps the child before closing any pipe itself.
                closeStdinNoThrow(keep_command);

                if (timeout_command_out->hasStderr())
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Executable generates stderr: {}", timeout_command_out->getStderr());

                /// Two independent reasons to wait, and either one on its own is enough. One is
                /// `check_exit_code`: the worker's exit status has to be read. The other is
                /// `stderr_reaction`: this wait is the last stretch in which the command can still
                /// write, and what it writes there has to go through the reaction like anything it
                /// wrote earlier. Tying the second to the first is what left a command with
                /// `stderr_reaction = throw` and `check_exit_code = 0` able to complain on its way
                /// out and still have the query succeed.
                const bool wait_for_command = command != nullptr && !keep_command;
                if (wait_for_command && (check_exit_code || timeout_command_out->stderrIsObserved()))
                {
                    /// The same decision as the stdin close above: a worker that goes back to
                    /// the pool must not be reaped here, and one that does not had its stdin
                    /// closed, so this wait can actually finish. `commandIsReused` reads a
                    /// missing command as "not reused", so check it before the wait.
                    ///
                    /// `waitDrainingOutput` rather than `wait`, because the very reason a
                    /// worker is discarded here can be that it wrote past its response frame:
                    /// once that output fills the pipe the child sits in `write`, and a plain
                    /// `wait` - which reaps before it closes anything - would never return.
                    /// Nothing reads those pipes any more, so this wait takes the bytes off
                    /// them and lets the child reach its own exit - without a bound for a
                    /// non-pooled command whose exit status is checked, as on the pipe path,
                    /// and within `command_termination_timeout` for a pooled worker, which was
                    /// never waited for before. What it finds on stderr still goes
                    /// through `stderr_reaction`: this is the last stretch in which a command
                    /// can write, and under `throw` that output fails the query like any other.
                    ///
                    /// A pooled worker that does not exit within that budget fails the query rather
                    /// than being waved through: `check_exit_code` says the exit status is
                    /// checked, and a status that cannot be read is not a passing one. The
                    /// query's rows are already correct, but so are the rows of any command
                    /// whose exit code turns out to be non-zero - which is exactly what the
                    /// setting exists to reject. `check_exit_code = 0` is how a command that is
                    /// not expected to exit promptly is configured - and such a command is
                    /// still waited for here when stderr is observed, for what it may say on
                    /// its way out; that spends the budget `~ShellCommand` would otherwise
                    /// spend before signalling it, not a second one (the two waits share one
                    /// deadline), so nothing is stalled that was not stalled before.
                    /// A worker whose answer was abandoned mid-protocol (a downstream `LIMIT`) is
                    /// waited for within `command_termination_timeout`: a command written for this
                    /// transport exits on stdin EOF, and its stdout carries only control frames,
                    /// so closing it after some amount of output - what ends an abandoned producer
                    /// on the pipe path - would never come.
                    const bool output_abandoned = !finished
                        && (!configuration.read_fixed_number_of_rows || current_read_rows < configuration.number_of_rows_to_read);
                    waitForCommandExit(*command, *timeout_command_out, /*close_inputs_first=*/ false,
                        {
                            .stderr_sink = {},
                            .check_exit_status = check_exit_code,
                            .unbounded_status_wait = !is_pooled && !output_abandoned,
                            .limit_stdout_drain = output_abandoned,
                            .check_cancelled = queryKilledCheck(context),
                        },
                        {.subject = "The process of an executable UDF", .after = " after its stdin was closed"});
                }
            }

            return status;
        }

    private:
        /// A worker whose region has outgrown `shared_memory_max_size` is not built on.
        /// The cap is what an administrator sized the pool by - `pool_size` regions of at
        /// most that - and the server's own growth never exceeds it, but the seals do not
        /// stop the command from extending the file (`ftruncate` past the end is not
        /// shrinking), and a file it stretched to a terabyte would be charged to this
        /// query, mapped, and - for a borrow by another user - zeroed page by page. So the
        /// file's length is read before anything else, and a worker over the cap goes,
        /// with its region; this borrow starts a fresh one. The same check runs where the
        /// worker is handed back (`commandIsReused`), so this is the second line, for a
        /// hand-back that could not run it.
        ///
        /// A check and an action on a file the command can extend at any moment are two
        /// different things, and the command is alive between them. So this check is the
        /// graceful path, not the guarantee: the guarantee is that nothing after it is ever
        /// sized by the file's length without that very length having been compared with
        /// the cap first - the mapping and the reservation (`takeOverReusedRegion`) re-read
        /// the file and refuse to go on with a figure over the cap, and a
        /// command that extends the file inside that window costs its worker the borrow.
        void discardPooledWorkerOverTheCap()
        {
            if (!command_holder)
                return;

            if (const auto * over = command_holder->sharedMemoryRegionOverTheCap(shared_memory_max_size))
            {
                /// Discarded even if the log line throws: see `discardPooledWorkerBeforeTheBorrow`.
                SCOPE_EXIT({ command_holder->discardWorkerAndRegion(); });
                LOG_WARNING(
                    getLogger("ShellCommandSharedMemorySource"),
                    "The process of an executable UDF has grown its shared-memory region to {} bytes "
                    "(its length, the pages it committed, or what it would hold once mapped whole), past "
                    "shared_memory_max_size ({} bytes); the process and its region are discarded and this "
                    "borrow starts a fresh one",
                    std::max(over->backingSize(), over->costOnceMappedWhole()), shared_memory_max_size);
            }
        }

        /// Charges this query for the region and takes the region on: the pooled worker's, or a new
        /// one of `shared_memory_size` bytes. May throw MEMORY_LIMIT_EXCEEDED.
        ///
        /// The region is charged to this query's memory tracker for the whole borrow, so it counts
        /// against its memory limit; the charge is released on the same (query) thread in
        /// `cleanup`, including when creating the region throws. A pooled region outlives the
        /// borrow, so the charge for it is handed over from the holder here and handed back in
        /// `cleanup`; the holder accounts it globally while the worker sits idle in the pool. It is
        /// never held by both trackers at once, and by neither only for the moment of a hand-over -
        /// see `ShellCommandHolder::releaseChargeToBorrower` and `cleanup`. The hand-over happens
        /// only right before the query is charged: the checks before this one
        /// (`inspectPooledWorkerBeforeTheBorrow`, `discardPooledWorkerOverTheCap`) may discard the
        /// worker, its region stays resident until the worker has been killed and reaped, so it
        /// stays charged globally until `discardWorkerAndRegion` drops it.
        void acquireRegion(size_t shared_memory_size)
        {
            if (command_holder)
            {
                /// Charge before the region is created: creating it commits its pages, so
                /// a query that is already at its memory limit has to be rejected first
                /// (the non-pooled branch below does the same). A region that survived a
                /// previous borrow may have grown, so charge what it actually holds - its
                /// committed size; a missing one is created at exactly `shared_memory_size`.
                /// As read by the cap check before this: `takeOverReusedRegion` reads the file
                /// again before anything is sized by it, and settles the charge with that.
                size_t existing_size = command_holder->lastSeenSharedMemorySize();
                if (existing_size > shared_memory_max_footprint)
                    failBorrowOnRegionOverTheCap(existing_size);
                command_holder->releaseChargeToBorrower();
                /// Whole pages: what a fresh region of `shared_memory_size` bytes holds.
                query_memory_charge.charge(existing_size ? existing_size : SharedMemoryRegion::roundUpToPages(shared_memory_size));
                shared_memory_region = command_holder->getOrCreateSharedMemory(shared_memory_size, borrow.region_created_by_this_borrow);

                if (!borrow.region_created_by_this_borrow)
                    takeOverReusedRegion(existing_size);
            }
            else
            {
                query_memory_charge.charge(SharedMemoryRegion::roundUpToPages(shared_memory_size));
                shared_memory_region = std::make_shared<SharedMemoryRegion>(shared_memory_size);
                borrow.region_created_by_this_borrow = true;
            }

            /// In whole pages, like the charge above and every growth (`ensureRegionFits`):
            /// a region is committed in pages, and a counter that mixed bytes here with
            /// pages there would not add up to what the memory trackers report.
            if (borrow.region_created_by_this_borrow)
                ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, shared_memory_region->footprint());
        }

        /// Takes on the process - started for this borrow, or the pooled worker - only once the
        /// region exists. A `memfd` has no name a process could open later, so the command
        /// reaches its region by having inherited its descriptor at `exec` - which is why the
        /// region (`acquireRegion`) had to come first. A pooled worker that served an earlier
        /// borrow inherited it when it was started; `buildCommand` hands it back as it is.
        /// `command` is the last thing the constructor takes on for the reason given at its
        /// declaration.
        void acquireCommand(const ShellCommandHolder::ShellCommandBuilderFunc & build_command)
        {
            if (command_holder)
            {
                /// Whether this is a worker that has already served a borrow is decided by
                /// what the holder still has after the probes before this: one they discarded is
                /// gone, and `buildCommand` starts a fresh process on the region the holder
                /// owns now.
                worker_is_reused = command_holder->hasReturnedCommand();
                command = command_holder->buildCommand();

                /// Borrow acquired: capture the pid for procfs sampling. Best-effort, and it
                /// allocates, so a failure must not fail a query that is otherwise ready.
                if (configuration.sampler)
                {
                    try
                    {
                        configuration.sampler->recordPidAcquired(command->getPid());
                    }
                    catch (...)
                    {
                        tryLogCurrentException("ShellCommandSharedMemorySource");
                    }
                }
            }
            else
            {
                command = build_command({{SHARED_MEMORY_CHILD_FD, shared_memory_region->fd()}});

                if (configuration.sampler)
                    configuration.sampler->recordExecutablePid(command->getPid());
            }
        }

        void setUpInputPipeline(Pipe && input_pipe)
        {
            input_header = materializeBlock(input_pipe.getHeader());
            input_pipe.resize(1);
            input_pipeline = QueryPipeline(std::move(input_pipe));
            input_executor = std::make_unique<PullingPipelineExecutor>(input_pipeline);
        }

        void assertEnoughRowsRead()
        {
            if (configuration.read_fixed_number_of_rows && current_read_rows < configuration.number_of_rows_to_read)
            {
                borrow.command_is_invalid = true;
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF wrong result, expected {} row(s), actual {}",
                    configuration.number_of_rows_to_read,
                    current_read_rows);
            }
        }

        /// Pulls the next non-empty input block and serializes it into the region (growing the
        /// region on demand). Returns the serialized size, or std::nullopt when input is exhausted.
        std::optional<size_t> serializeInput()
        {
            /// A function without arguments has nothing to serialize: its input block has no
            /// columns, so it has no rows either, whatever the query asked for, and the pipeline
            /// carries no chunk at all. It is still a call - the command is asked to produce rows,
            /// exactly as it is over the pipes, where this function writes an empty payload and the
            /// command answers it - so the request is made by hand, once, with nothing in the
            /// region, and the input is exhausted after it. Leaving it out would call the command
            /// not at all and fail the query for the rows it never produced.
            if (input_header.columns() == 0)
            {
                if (zero_argument_request_sent)
                    return std::nullopt;

                zero_argument_request_sent = true;
                return 0;
            }

            Block input_block;
            bool have_input = false;
            while (input_executor->pull(input_block))
            {
                if (input_block.rows() != 0)
                {
                    have_input = true;
                    break;
                }
            }

            if (!have_input)
                return std::nullopt;

            /// Serialize into the region itself, growing it on demand (up to `shared_memory_max_size`)
            /// whenever it fills up. The command asks for more room later if its result does not fit
            /// next to the input.
            /// Deliberately not auto-finalized: on the exception path the buffer is destroyed while
            /// the stack unwinds, which holds nothing back (everything written is already in the
            /// region), and a `finalize` from a destructor could not report a problem anyway.
            WriteBufferToSharedMemoryRegion write_buffer(
                *shared_memory_region,
                /// The input is serialized straight into the region, so its total size is known only
                /// once it is over: every request for room is a lower bound on what it needs.
                [this](size_t required)
                { ensureRegionFits(required, "The serialized input", /*required_is_lower_bound=*/ true); });

            auto output_format = context->getOutputFormat(format, write_buffer, input_header);
            formatBlock(output_format, input_block);

            /// `formatBlock` flushes the format into the buffer, which also moves a byte that ended
            /// up in its spare slot into the region. Do it explicitly all the same: it is what grows
            /// the region under this query's memory limit, and `finalize` may not do it (see
            /// WriteBufferToSharedMemoryRegion::finalizeImpl).
            write_buffer.next();
            write_buffer.finalize();

            /// A format that wraps this buffer (`WriteBufferValidUTF8`, `PeekableWriteBuffer`)
            /// flushes what it still holds from its own `finalize` - inside `formatBlock`, when the
            /// format finalizes its buffers - where memory-limit exceptions are blocked. A growth
            /// from there is charged, but `max_memory_usage` is not enforced for it (see
            /// `ensureRegionFits`), so it is enforced now that exceptions are allowed again.
            if (memory_limit_check_pending)
            {
                memory_limit_check_pending = false;
                CurrentMemoryTracker::check();
            }

            return write_buffer.count();
        }

        /// Request to the child: protocol version, request id, file path, input offset, input size.
        void sendRequest(size_t input_size, UInt64 request_id)
        {
            borrow.request_sent = true;
            writeVarUInt(SHARED_MEMORY_PROTOCOL_VERSION, *timeout_command_in);
            writeVarUInt(request_id, *timeout_command_in);
            writeStringBinary(SharedMemoryRegion::pathForChildFd(SHARED_MEMORY_CHILD_FD), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(0), *timeout_command_in);
            writeVarUInt(static_cast<UInt64>(input_size), *timeout_command_in);
            timeout_command_in->next();
        }

        /// Sends the request to the child and sets up output_executor over the response. The region
        /// must already hold `input_size` bytes of serialized input at offset 0.
        void exchange(size_t input_size)
        {
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryCalls);
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryInputBytes, input_size);
            /// The pipe buffers account for control frames; account for the payload once,
            /// even if the command requests more space and the control request is retried.
            if (configuration.sampler)
                configuration.sampler->recordInputBytes(input_size);

            UInt64 output_offset = 0;
            UInt64 output_size = 0;

            /// The whole response frame - not each read of it - has `command_read_timeout` to arrive.
            /// The frame is a handful of varints and at most a capped error message, so a command
            /// that answers at all answers it in one go; one that trickles it out a byte at a time
            /// would otherwise hold the query for that many timeouts.
            SCOPE_EXIT({ timeout_command_out->disarmFrameDeadline(); });

            while (true)
            {
                const UInt64 request_id = generateRequestId();
                sendRequest(input_size, request_id);
                timeout_command_out->armFrameDeadline();

                const UInt64 status = readResponseStatus(request_id);

                if (status == SHARED_MEMORY_STATUS_NEED_MORE_SPACE)
                {
                    growRegionAsRequested();
                    continue;
                }

                if (status != SHARED_MEMORY_STATUS_OK)
                    throwReportedError(status);

                readVarUInt(output_offset, *timeout_command_out);
                readVarUInt(output_size, *timeout_command_out);
                break;
            }

            setUpOutputPipeline(output_offset, output_size);
        }

        /// Reads the beginning of the response from the child - the id of the request it is
        /// answering, and the status - and returns the status. What follows it is either the output
        /// location (on success), the size it needs (when the region is too small) or an error
        /// message.
        ///
        /// The id is what makes the answer provably an answer to *this* request. Without
        /// it, anything the process left on its stdout - a stray byte after an earlier
        /// response, a line of debug output - is read as the status of this one, and the
        /// rest of the frame shifts along with it: the real status becomes the offset and
        /// the real offset becomes the size. That is not a failure, it is a plausible
        /// frame, and with a matching row count the query returns the region's *input*
        /// bytes as its result. Silently wrong answers are the one outcome the checks
        /// around here exist to prevent, and only an echoed id rules them out.
        UInt64 readResponseStatus(UInt64 request_id)
        {
            UInt64 answered_request_id = 0;
            readVarUInt(answered_request_id, *timeout_command_out);

            if (answered_request_id != request_id)
            {
                borrow.command_is_invalid = true;
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF answered request {} with a frame for request {}. The command must echo "
                    "the request id and must write nothing but the response frame; a process that has written "
                    "anything else is out of step with the protocol and its answers cannot be trusted",
                    request_id, answered_request_id);
            }

            UInt64 status = 0;
            readVarUInt(status, *timeout_command_out);
            return status;
        }

        /// Handles `SHARED_MEMORY_STATUS_NEED_MORE_SPACE`: reads the size the command needs and
        /// grows the region to it, for `exchange` to re-send the same request.
        ///
        /// The result does not fit next to the input; the server is the only side that
        /// can enlarge the region, so it does that and re-sends the same request. The
        /// serialized input survives the growth (`posix_fallocate` keeps the file contents),
        /// so it does not have to be written again. This terminates: every iteration
        /// strictly increases the region size, which is capped by `shared_memory_max_size`.
        void growRegionAsRequested()
        {
            UInt64 requested_size = 0;
            readVarUInt(requested_size, *timeout_command_out);

            /// The frame was read in full, so the worker is at a clean protocol boundary, as
            /// after the other answers that fail the query without discarding it.
            if (requested_size <= shared_memory_region->size())
            {
                borrow.command_can_be_reused = true;
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF asked for a shared-memory region of {} bytes, which is not larger "
                    "than the current one ({} bytes)",
                    requested_size, shared_memory_region->size());
            }

            try
            {
                ensureRegionFits(requested_size, "The region size requested by the command");
            }
            catch (...)
            {
                /// The child answered this request in full and is waiting for the next one,
                /// so a region that cannot grow that far (a memory limit, the configured
                /// cap) leaves it at a clean protocol boundary, like a failure while the
                /// input was being prepared: the pooled worker stays reusable.
                borrow.command_can_be_reused = true;
                throw;
            }
        }

        /// Handles an error status: reads the message that follows it and fails the query with it.
        [[noreturn]] void throwReportedError(UInt64 status)
        {
            String message;
            /// Cap the error message so a buggy or malicious command cannot force a huge
            /// allocation on the failure path.
            readStringBinary(message, *timeout_command_out, SHARED_MEMORY_MAX_ERROR_MESSAGE_SIZE);

            /// The response was read in full, so the command is back to waiting for the next
            /// request: this is the command reporting that it cannot process this input, not
            /// the command misbehaving, and a pooled worker survives it. (A truncated
            /// message would have thrown out of the read above and discarded the worker.)
            borrow.command_can_be_reused = true;
            throw Exception(ErrorCodes::UDF_EXECUTION_FAILED,
                "Executable UDF reported an error (status {}): {}", status, message);
        }

        /// Sets up `output_executor` over the output the command reported at `output_offset`,
        /// `output_size` in the region.
        void setUpOutputPipeline(UInt64 output_offset, UInt64 output_size)
        {
            auto & region = *shared_memory_region;

            if (output_offset > region.size() || output_size > region.size() - output_offset)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable UDF returned an out-of-bounds region: offset {}, size {}, region size {}",
                    output_offset, output_size, region.size());

            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryOutputBytes, output_size);
            if (configuration.sampler)
                configuration.sampler->recordOutputBytes(output_size);

            /// Parsed where it lies. The region is sealed against shrinking, so the bytes the
            /// command just reported are backed by pages that cannot go away under the parser -
            /// there is no copy to make and nothing to check before reading. `output_read_buffer`
            /// points into the mapping, and a growth replaces the mapping, so every caller drops
            /// the buffer before asking for the next chunk.
            output_read_buffer = std::make_unique<ReadBufferFromMemory>(region.data() + output_offset, output_size);
            output_pipeline = QueryPipeline(Pipe(context->getInputFormat(format, *output_read_buffer, *sample_block, configuration.max_block_size)));
            /// Like in pipe mode: the rows the command returns are not rows read by this query, so
            /// they must not be added to SelectedRows/SelectedBytes by the read progress callback.
            output_pipeline.disableProfileEventUpdate();
            output_executor = std::make_unique<PullingPipelineExecutor>(output_pipeline);
        }

        /// Serializes the next chunk into the region and exchanges it.
        bool sendNextRequest()
        {
            borrow.preparing_input = true;
            auto input_size = serializeInput();
            borrow.preparing_input = false;

            if (!input_size)
            {
                closeStdinIfNeeded(is_pooled);
                return false;
            }
            exchange(*input_size);
            return true;
        }

        /// Looks over the worker this borrow would be built on, before anything is built on it -
        /// and, above all, before its region is taken over.
        ///
        /// Two states disqualify it: a process that hung up its stdout while it sat in the pool,
        /// and one that has written to its stdout since its last answer. The second one's bytes
        /// are an earlier borrow's - this one has sent nothing yet - and read as the beginning of
        /// *this* answer they are a plausible response frame, so the worker has to go. Under
        /// `stderr_reaction` `throw`, so does one that has written to its stderr (see below).
        ///
        /// Either way it goes together with its region, and this borrow starts on a fresh one. A
        /// hung-up stdout says the process closed it, not that the process is gone - it may have
        /// closed it and carried on - and even a process that has exited may have left a
        /// descendant holding the descriptor to the region it inherited. Handing that region
        /// to a replacement would leave this query reading a mapping something else can still
        /// write into: the destructor gives the process the termination timeout and then a signal
        /// it may ignore, and a descendant nothing at all. So a process and its region live and
        /// die together, as everywhere else that drops a worker. What that costs is the region a
        /// worker that died in the pool had grown, which the replacement grows again if it needs
        /// it.
        ///
        /// Both run here rather than after `buildCommand` for the same reason: once the region
        /// has been taken over and charged to this query, dropping it is no longer a matter of
        /// letting them go.
        void inspectPooledWorkerBeforeTheBorrow()
        {
            if (!command_holder)
                return;

            ShellCommand * worker = command_holder->returnedCommand();
            if (!worker)
                return;

            const auto reason = inspectReusedWorker(*worker, stderr_throws);
            if (reason == UnfitReusedWorker::NONE)
                return;

            if (reason != UnfitReusedWorker::EXITED)
                ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryDirtyChannelDiscards);

            discardPooledWorkerBeforeTheBorrow([&]
            {
                reportUnfitReusedWorker(
                    *worker, reason, "ShellCommandSharedMemorySource", "The process of an executable UDF", ", with its region,");
            });
        }

        /// Drops a worker `inspectPooledWorkerBeforeTheBorrow` found unfit, together with its region,
        /// after `report` - which reads what the worker left on its pipes and logs it, so it needs the
        /// worker alive. The worker is then killed and reaped at once, without the grace period
        /// (`ShellCommand::discardWithoutGrace`): nobody is interested in how it exits. The decision
        /// is already made, and the worker has to go whatever happens on the way: the report can
        /// throw (reading, formatting and logging allocate - `MEMORY_LIMIT_EXCEEDED`), and a holder
        /// still holding the process would hand it back to the pool when the constructor unwinds, for
        /// the next borrow to be built on the worker that was found unfit. So the discard runs on
        /// every path, and the exception goes on after it.
        template <typename Report>
        void discardPooledWorkerBeforeTheBorrow(Report && report)
        {
            SCOPE_EXIT({ command_holder->discardWorkerAndRegion(); });
            report();
        }

        /// Ensures the region can hold `required` bytes, growing it (up to `shared_memory_max_size`)
        /// if needed. Growth doubles the size to amortize repeated growths. `what` names whose
        /// requirement this is, for the exception raised when the region cannot grow that far. The
        /// added bytes are charged to the query memory tracker like the rest of the region; for a
        /// pooled region `cleanup` hands that charge over to the holder together with the region.
        void ensureRegionFits(size_t required, std::string_view what, bool required_is_lower_bound = false)
        {
            auto & region = *shared_memory_region;
            if (required <= region.size())
                return;

            if (required > shared_memory_max_size)
                throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER,
                    "{} ({}{} bytes) does not fit into the shared-memory region "
                    "({} bytes, maximum {} bytes): increase shared_memory_max_size",
                    what, required_is_lower_bound ? "at least " : "", required, region.size(), shared_memory_max_size);

            const size_t new_size = chooseGrownSize(required, required_is_lower_bound);

            /// What this growth commits, at most: the pages between the end of the file and
            /// `new_size`. `posix_fallocate` commits every page of the file that is not committed,
            /// and the footprint says how many pages the file holds but not where: pages the
            /// command committed past the end of the file (`refreshFootprint`) may lie inside
            /// that range - then this growth commits less than this, and those pages are already
            /// charged - or far beyond it, at an offset the growth never reaches - then it commits
            /// all of this on top of them. Telling the two apart beforehand would take a
            /// page-by-page walk the file does not offer (`mincore` and `SEEK_HOLE` do not see
            /// reserved pages), and the growth cannot be charged after it has committed its pages:
            /// a query at its memory limit has to be refused before, not told afterwards. So the
            /// most it can commit is charged and checked first, the footprint is re-read after,
            /// and what the growth turned out not to need is given back - a moment of double
            /// counting for the pages the command committed just past the end, and only those.
            /// The length the server itself committed the file up to (`reservedSize`) rather than
            /// the mapped size or the length of the file: a growth that committed its pages and
            /// could not map them has left the file longer than the mapping, and those pages are
            /// committed and charged; and a file the command extended is longer than what was
            /// committed, and the growth commits the difference. Whole pages, like the footprint.
            ///
            /// The one thing this bound does not cover is a page the command freed inside the file
            /// (`FALLOC_FL_PUNCH_HOLE`): the growth commits it again, and the re-read finds the
            /// footprint higher than it charged for. That is charged then, after the fact - the
            /// command that punched the hole is the command that pays for it, and it pays with its
            /// own query's limit.
            ///
            /// Re-read now, not taken from the last hand-over: the command is alive, and a request
            /// that comes back asking for a larger region (`NEED_MORE_SPACE`) comes back from a
            /// command that had the region to itself in between - pages it committed past the end
            /// during that request are what this growth would commit on top of, and a bound that
            /// did not know of them would let the growth take the footprint past the cap and find
            /// out afterwards. One `fstat` per growth, and growths are amortized. A region whose
            /// footprint cannot be read is not handed to the next borrow (see below).
            size_t footprint_before = 0;
            try
            {
                footprint_before = region.refreshFootprint();
            }
            catch (...)
            {
                borrow.command_is_invalid = true;
                throw;
            }
            const size_t expected = region.fillCostUpTo(new_size);

            /// Never past the cap, in pages like the footprint: the server's own growth is what
            /// the cap is a promise about (the command's own commits are checked where the worker
            /// changes hands - see `regionIsWithinTheCap`). A region the command has filled with
            /// pages far past the end has no room left for the growth, and a worker in that state
            /// is not one to keep: the next chunk would fail the same way.
            if (footprint_before + expected > shared_memory_max_footprint)
            {
                borrow.command_is_invalid = true;
                throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER,
                    "{} ({}{} bytes) does not fit into the shared-memory region: growing the region to {} bytes "
                    "would take its footprint from {} to {} bytes, past shared_memory_max_size ({} bytes); "
                    "the process of the executable UDF has committed pages of its own into the region "
                    "(past the end of its file) and is discarded",
                    what, required_is_lower_bound ? "at least " : "", required, new_size,
                    footprint_before, footprint_before + expected, shared_memory_max_size);
            }

            /// Charge first (may throw MEMORY_LIMIT_EXCEEDED), then grow. If the growth fails, roll
            /// back exactly the part that was not committed: `posix_fallocate` undoes itself, but
            /// a remap that fails after it leaves the pages committed and the file - sealed against
            /// shrinking - permanently larger. Those pages stay charged, here and on every later
            /// borrow, because they are what the region costs from now on. Re-read rather than
            /// taken from the cached figure, on both paths: the cached one is raised to the length
            /// of the file, and the pages of the command's own that this growth committed on top
            /// (see above) show only in `st_blocks`.
            ///
            /// The charge is brought up to the footprint just re-read before the growth is added on
            /// top, and settled as a whole against the footprint after it - not by the difference
            /// the growth made. The command had the region to itself since the charge was last
            /// settled, and pages it committed in between (`fallocate` past the end, an extended
            /// file) are in `footprint_before` but not in the charge; settling by the difference
            /// alone would carry that shortfall past a growth that has measured it. Never below what
            /// the borrow was charged before: that charge was made for the region as it was handed
            /// over, and a page the command punched out of it since can come back on the next write.
            const size_t charged_before = query_memory_charge.amount();
            const size_t charged = std::max(charged_before, footprint_before) + expected;
            query_memory_charge.charge(charged - charged_before);

            try
            {
                /// The holder's region and this borrow's are one object (`getOrCreateSharedMemory`).
                region.grow(new_size);
            }
            catch (...)
            {
                settleFailedGrowth(footprint_before, charged_before, charged);
                throw;
            }

            /// Counted before anything after it can throw, not after: the growth has happened and
            /// its pages are committed, and both the re-read and the settlement (`settleGrowth`) can
            /// throw - the few pages the bound does not cover (a hole the command punched, refilled
            /// by this `posix_fallocate`) are charged there, and a query at its limit is failed for
            /// them. That is the charge doing its job, but it must not also make a growth that really
            /// happened invisible in the counters. The failure path (`settleFailedGrowth`) counts
            /// them for the same reason.
            ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryRegionGrowths);

            settleGrowth(footprint_before, charged_before, charged);
        }

        /// The size `ensureRegionFits` grows the region to, for a requirement over its current size
        /// and within the cap.
        size_t chooseGrownSize(size_t required, bool required_is_lower_bound)
        {
            const auto & region = *shared_memory_region;

            /// A caller that only knows a lower bound - the input is serialized straight into the
            /// region and asks for one byte at a time (`moveOverflowIntoRegion`) - gets the region
            /// doubled, so that repeated growth stays amortized, but never past the cap. When
            /// doubling overshoots the cap the region grows to the cap at once: that commits the
            /// whole configured maximum for a chunk that needs a little more room, which is the
            /// price of not growing byte by byte for the rest of that chunk - a growth is a
            /// `posix_fallocate` and a remap, and one of them beats a thousand. A caller that knows
            /// its exact requirement - the command's `NEED_MORE_SPACE` names the total it needs -
            /// gets exactly that: doubling it would charge the query for room nobody asked for, and
            /// fail a query whose memory limit fits what was asked.
            size_t new_size = required_is_lower_bound ? std::max(required, std::min(region.size() * 2, shared_memory_max_size)) : required;

            /// Unless memory-limit exceptions are blocked right now. That happens when the growth is
            /// driven from inside someone else's `finalize` - a format's wrapping buffer
            /// (`WriteBufferValidUTF8`, `PeekableWriteBuffer`) flushing what it still holds into this
            /// region - because `WriteBuffer::finalize` blocks them for its whole body. The charge
            /// for the growth then cannot fail, so `max_memory_usage` is not enforced for these bytes, and a
            /// doubling would put a whole region size past the limit. Take a modest step instead:
            /// what escapes the limit is then bounded by what the writer is actually flushing.
            if (LockMemoryExceptionInThread::isBlocked(VariableContext::Process, /*fault_injection=*/ false))
            {
                new_size = std::max(required, std::min(region.size() + UNENFORCED_GROWTH_STEP, shared_memory_max_size));
                memory_limit_check_pending = true;
            }


            return new_size;
        }

        /// Settles the charge for a growth whose `region.grow` threw, from inside the handler of
        /// that exception; the caller rethrows it. Rolls back exactly the part that was not
        /// committed (see `ensureRegionFits`).
        void settleFailedGrowth(size_t footprint_before, size_t charged_before, size_t charged)
        {
            /// If the footprint cannot be refreshed, retain the conservative charge and
            /// preserve the growth failure that brought us here.
            const auto growth_exception = std::current_exception();
            size_t footprint_after = 0;
            try
            {
                footprint_after = shared_memory_region->refreshFootprint();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSource", "Cannot refresh the shared-memory footprint after a failed growth");
                borrow.command_is_invalid = true;
                std::rethrow_exception(growth_exception);
            }
            const size_t settled = std::max(charged_before, footprint_after);
            if (settled < charged)
                query_memory_charge.uncharge(charged - settled);
            /// More than the bound, for the one reason the bound does not cover: a page the
            /// command punched out of the file that `posix_fallocate` committed again before
            /// the remap failed. The growth failed, but those pages are in the file and the
            /// file cannot shrink, so the query pays for them exactly as it does on the path
            /// where the growth succeeds.
            else if (settled > charged)
                query_memory_charge.chargeNoThrow(settled - charged);
            const size_t added = footprint_after > footprint_before ? footprint_after - footprint_before : 0;
            if (added)
                ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, added);
        }

        /// Settles the charge for a growth that succeeded, against the footprint re-read after it
        /// (see `ensureRegionFits`).
        ///
        /// A region whose footprint cannot be read after it grew, or whose pages the query could
        /// not be charged for in full, is not one to hand to the next borrow: the worker goes
        /// with it, and the query keeps the charge it has until then.
        void settleGrowth(size_t footprint_before, size_t charged_before, size_t charged)
        {
            try
            {
                const size_t footprint_after = shared_memory_region->refreshFootprint();
                const size_t added = footprint_after > footprint_before ? footprint_after - footprint_before : 0;
                ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryAllocatedBytes, added);

                query_memory_charge.settle(charged, std::max(charged_before, footprint_after));
            }
            catch (...)
            {
                borrow.command_is_invalid = true;
                throw;
            }
        }

        /// Brings a region that served an earlier borrow into the state this borrow relies on: the
        /// whole file mapped.
        ///
        /// The file can be longer than what the server has mapped - a command that extended it,
        /// or a growth that committed its pages and could not map them - and the command maps the
        /// whole file on every request, so the tail beyond the mapping is as readable to it as the
        /// rest, and an offset it answers with may lie there. The query is already charged for the
        /// file's footprint (`lastSeenSharedMemorySize`), a region only ever grows, and the file is under
        /// the cap (checked before this), so the mapping is brought up to the file here; a region
        /// that cannot be mapped whole is no use to this or any later borrow, and the worker goes
        /// with it rather than being handed on with the same defect.
        ///
        /// Pages the previous command freed inside the file (the seals stop it from shrinking the
        /// file, not from punching holes in it) are not committed again here: nothing about that
        /// would hold - the command keeps its descriptor and can punch again at any instant - and
        /// `SharedMemoryRegion` explains why no check can even tell. A hole costs the server a page
        /// allocation on its next access, which can raise `SIGBUS` if the kernel cannot reserve memory.
        void takeOverReusedRegion(size_t charged_size)
        {
            auto & region = *shared_memory_region;

            /// Re-read, and compared with the cap again, because this is the figure the growth
            /// below commits with `posix_fallocate` and maps: the constructor's check was a moment
            /// ago, and the command can have extended the file since. Sizing the growth by an
            /// unchecked length would let a command that races the borrow have the server commit
            /// whatever it made the file - which is exactly what the cap exists to prevent.
            const size_t footprint = region.refreshFootprint();
            const size_t backing = region.backingSize();
            if (region.isOverTheCap(shared_memory_max_size))
                failBorrowOnRegionOverTheCap(std::max(backing, region.costOnceMappedWhole()));

            /// The query was charged for the footprint read a moment earlier; a region that grew
            /// in between - within the cap - is charged for the rest before it is mapped, so that
            /// what this borrow holds is what it is charged for. May throw the memory limit, in
            /// which case nothing has been touched yet and the worker keeps its region.
            ///
            /// And for what mapping the file whole is about to commit, at most: the growth below
            /// `posix_fallocate`s the file up to its length, and the pages between the length the
            /// server itself last committed and that length may all be missing - a command can
            /// extend the file without committing a page - while the footprint may consist of pages
            /// the command committed past the end, which the fill adds to rather than uses (that
            /// is what `isOverTheCap` has just ruled out going past the cap). Charged before the
            /// fill, like every growth (`ensureRegionFits`), and settled against the footprint
            /// re-read after it: what the fill turned out not to need is given back.
            const size_t fill = region.fillCostUpTo(backing);
            if (footprint + fill > charged_size)
                query_memory_charge.charge(footprint + fill - charged_size);

            if (backing > region.size())
            {
                try
                {
                    region.grow(backing);
                }
                catch (...)
                {
                    dropRegionAndWorker();
                    throw;
                }

                /// Settled both ways, like a growth (`ensureRegionFits`): what the fill did not need
                /// is given back, and pages the command freed under the length and replaced past it
                /// - which the bound cannot see, the count being the same - are charged now that
                /// the re-read sees them.
                query_memory_charge.settle(footprint + fill, region.refreshFootprint());
            }
        }

        /// Drops this borrow's view of the region together with the holder's worker and region:
        /// what a borrow does when it finds the worker's region unusable after it has begun.
        void dropRegionAndWorker()
        {
            shared_memory_region.reset();
            borrow.region_created_by_this_borrow = false;
            command_holder->discardWorkerAndRegion();
        }

        /// Discards the worker this borrow has taken out of the holder, and its region with it. The
        /// process goes first, before the region and the accounting that goes with it: its stdin is
        /// closed, so a worker written to exit on EOF exits, and `~ShellCommand` waits for it -
        /// whatever is left of `command_termination_timeout` - and then sends `SIGKILL` to its whole
        /// process group and reaps it (`ShellCommand::Config::own_process_group`). Neither the worker
        /// nor a descendant still in its group runs again, so nothing can write into the region once
        /// its charge is dropped; the region frees every page of its file as it is destroyed
        /// (`~SharedMemoryRegion`). What is out of reach is a descendant that left the group (see the
        /// note on the cap in `docs/reference/functions/regular-functions/udf.mdx`).
        void discardTakenWorker() noexcept
        {
            closeStdinNoThrow(/*command_is_reused=*/ false);
            command = nullptr;
            dropRegionAndWorker();
        }

        /// The region's file found over `shared_memory_max_size` after the constructor's check let
        /// the borrow begin: the command extended it in between. Fail closed - the worker and its
        /// region go, and so does this query - rather than carry on with a figure the cap was
        /// meant to rule out. The next query starts a fresh worker. (`cleanup` sees no worker in
        /// the holder afterwards, so it does not mistake this for a borrow that never touched it.)
        [[noreturn]] void failBorrowOnRegionOverTheCap(size_t backing)
        {
            dropRegionAndWorker();
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "The process of an executable UDF extended its shared-memory region to {} bytes, past "
                "shared_memory_max_size ({} bytes), while the region was being borrowed; the process and "
                "its region are discarded",
                backing, shared_memory_max_size);
        }

        /// Clears the region when this borrow belongs to a different user, or to the same user
        /// under different roles, than the previous one (`ShellCommandHolder::BorrowerIdentity`).
        /// What a query wrote into a pooled region stays there until overwritten, and the command
        /// serving the next query can read it - over the pipes it only ever saw what it was sent.
        /// That boundary is where it matters, and the cost is paid only there: nothing when the
        /// borrower is the same. The whole file and not just what the server knows it used: the
        /// command may have written anywhere in it, and only clearing everything says anything
        /// about all of it - and by now the whole file is mapped (`takeOverReusedRegion`), so the
        /// region is the file.
        ///
        /// Cleared by freeing its pages and committing them again
        /// (`SharedMemoryRegion::releasePagesUpToLength`) rather than by writing zeros over them,
        /// because only that tells what the clearing costs before it commits anything. A write
        /// allocates every page the command freed under the length, and the footprint the borrow
        /// is charged for cannot see such holes when the command committed as many pages past the
        /// end (`SharedMemoryRegion::fillCostUpTo`): the charge before a write would have to be
        /// the whole region on top of the one the borrow already has, and a query that fits the
        /// region once would be refused for a borrow that commits nothing new. With the pages
        /// freed first, what is left is exactly what the command committed past the end, and the
        /// commit adds the length to it.
        void scrubRegionForBorrower()
        {
            ShellCommandHolder::BorrowerIdentity borrower{context->getUserID(), context->getCurrentRoles()};
            /// A region this borrow created is a fresh, zero-filled file with nobody's data in it:
            /// the discarded worker's region went with it (`discardWorkerAndRegion`), and
            /// clearing a new one would be wasted work of its size.
            if (command_holder->lastBorrower() && *command_holder->lastBorrower() != borrower
                && shared_memory_region && !borrow.region_created_by_this_borrow)
            {
                auto & region = *shared_memory_region;

                /// The region is clear from here on, whatever happens below. The charge is brought
                /// up to what the commit makes the file hold. Any failure discards the worker and
                /// its region while the query still owns the charge. The charge is settled against
                /// the footprint re-read after the commit, and
                /// a commit that took the footprint past the cap is a region the command made over
                /// the cap: the borrow fails closed, with the worker and its region.
                try
                {
                    const size_t charged_before = query_memory_charge.amount();
                    const size_t past_the_length = region.releasePagesUpToLength();

                    /// The length was re-read just now, and the command can have extended the file
                    /// since the borrow checked it: what the commit below would make the file hold
                    /// is compared with the cap before it is committed, not after - the server never
                    /// commits pages by a length that has not been checked first.
                    const size_t committed_after = SharedMemoryRegion::roundUpToPages(region.backingSize()) + past_the_length;
                    if (region.backingSize() > shared_memory_max_size || committed_after > shared_memory_max_footprint)
                        failBorrowOnRegionOverTheCap(std::max(region.backingSize(), committed_after));

                    const size_t charged = std::max(charged_before, committed_after);
                    if (charged > charged_before)
                        query_memory_charge.charge(charged - charged_before);

                    region.recommitUpToLength();
                    ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryScrubbedBytes, region.backingSize());

                    const size_t footprint_after = region.refreshFootprint();
                    if (region.isOverTheCap(shared_memory_max_size))
                        failBorrowOnRegionOverTheCap(std::max(region.backingSize(), region.costOnceMappedWhole()));

                    /// Never below what the borrow was charged before the scrub: that charge was made
                    /// for the region as it was handed over.
                    query_memory_charge.settle(charged, std::max(charged_before, footprint_after));
                }
                catch (...)
                {
                    /// Clearing has already changed the region, even if recommitting failed.
                    /// Do not let constructor cleanup mistake it for an untouched worker.
                    dropRegionAndWorker();
                    throw;
                }
            }
            command_holder->recordBorrower(std::move(borrower));
        }

        /// Whether the region's file is still within `shared_memory_max_size` - the one property
        /// of a worker's region that the worker cannot be handed back to the pool without. The
        /// server's own growth stops at the cap; the command's extension of the file does not
        /// (see the constructor), and a worker whose file has passed it is discarded here rather
        /// than charged to the server at that size and handed to the next query. Never throws: a
        /// file whose length cannot be read is not one to build the next borrow on either.
        bool regionIsWithinTheCap() noexcept
        {
            if (!shared_memory_region)
                return true;

            /// All of it inside the handler: re-reading the file, the page arithmetic of the cap
            /// (whose unit is read from `/sys` on first use) and the message all allocate or can
            /// fail, and either must cost the worker its place in the pool, not terminate the
            /// server from a `noexcept` function.
            try
            {
                shared_memory_region->refreshFootprint();
                if (!shared_memory_region->isOverTheCap(shared_memory_max_size))
                    return true;

                LOG_WARNING(
                    getLogger("ShellCommandSharedMemorySource"),
                    "The process of an executable UDF has grown its shared-memory region to {} bytes "
                    "(its length, the pages it committed, or what it would hold once mapped whole), past "
                    "shared_memory_max_size ({} bytes); the process will not be reused",
                    std::max(shared_memory_region->backingSize(), shared_memory_region->costOnceMappedWhole()), shared_memory_max_size);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot check the size of a shared-memory region; the process will not be reused");
            }
            return false;
        }

        /// Takes anything a previous borrow's command left on its stderr off the pipe, without
        /// putting it through `stderr_reaction`.
        ///
        /// The probe that decides whether a worker may be pooled is one instant, so a command that
        /// writes a moment after answering slips past it and its bytes are sitting there when the
        /// next query borrows the process. Those bytes belong to the query that caused them, and
        /// that query is over; running them through the reaction here would fail *this* query for
        /// something it did not do, which under `stderr_reaction` `throw` is the difference between
        /// a confusing failure and a wrong accusation. They are logged instead, so they are not
        /// lost, and the query that borrows the worker is left alone.
        void discardStderrLeftByAPreviousBorrow()
        {
            /// Only a worker that served an earlier borrow can have left anything: a process
            /// started for this borrow (or just now, as a replacement) has no previous invocation,
            /// and what it writes at startup is this query's - under `throw` it fails it, as on
            /// the pipe path.
            if (!is_pooled || !worker_is_reused)
                return;

            try
            {
                timeout_command_out->clearStderrOfAnEarlierBorrow("ShellCommandSharedMemorySource");
            }
            catch (...)
            {
                /// Not at a known boundary, as in `quarantineReusedWorker`: the worker is discarded
                /// rather than built on, and the query fails.
                borrow.command_is_invalid = true;
                throw;
            }
        }

        /// Whether the pooled worker process survives this borrow and goes back to the pool together
        /// with its holder. Only a process left at a known protocol boundary does: one that never
        /// saw a request from this borrow, or one that answered every request in full. A protocol
        /// failure, a dead child and an invocation cut short (query cancellation, an exception
        /// downstream) all leave its state unknown, so it is discarded instead - which also means
        /// its stdin must be closed and its shared-memory region released.
        ///
        /// Only meaningful once the source is being torn down: while it is still running, a pooled
        /// worker that has not produced all its rows yet is not being discarded.
        /// A pooled worker may go back only while its control channel is at a protocol boundary.
        /// `exchange` reads exactly the control frame of a response - the status, and then either
        /// the output location, the size the command wants, or an error message - and nothing makes
        /// the command stop writing to stdout afterwards. A stray byte left there costs this query
        /// nothing, because its answer has already been read out of the shared-memory region; the
        /// next borrow is the one that reads that byte as the status varint of the response to its
        /// own first request, and fails in a way that looks nothing like the command that caused it.
        ///
        /// Stderr carries the same hazard, quietly rather than fatally, and a hangup on stdout says
        /// the child is gone; `TimeoutReadBufferFromFileDescriptor::ChannelState` spells all three
        /// out. Any of them means the worker is not at a boundary and has to be reaped instead.
        ///
        /// The probe cannot be exhaustive - a command that writes its stray byte later still slips
        /// through - but it catches the case that actually happens, a command that emits it together
        /// with its response.
        /// Not `const`: under `stderr_reaction` `none` it also empties the worker's stderr pipe (see
        /// below), which is a change to the worker's state, not just a look at it.
        bool controlChannelIsClean() noexcept
        {
            /// Latched, because the answer is asked for more than once - `prepare` decides with it
            /// and `cleanup` decides again - and the evidence does not survive being looked at.
            /// Reporting the discard drains the leftover stderr, and closing the discarded child's
            /// stdin makes it exit; a second, unlatched probe would then find two clean pipes and
            /// hand back a worker whose stdin is already closed, and the next query to borrow it
            /// would fail writing its first request. Deciding twice is exactly what the two callers
            /// avoid internally - this is what makes them agree with each other as well.
            if (borrow.channel_was_dirty)
                return false;

            /// Once the child has been reaped its pipes are closed and the descriptor numbers may
            /// have been recycled, so there is nothing safe to poll here - and a reaped child is
            /// not going back to the pool anyway. The same holds for a stdout closed on its own:
            /// `waitDrainingOutput` closes it where a command floods it past what was asked, and it
            /// can return without reaping, so a worker can reach this with its stdout gone and
            /// `isWaitCalled` still false. Such a worker has no way to answer the next query in any
            /// case.
            if (!command || command->isWaitCalled() || command->isStdoutClosed())
                return false;

            /// The constructor builds the process before it wraps the process's pipes, and the
            /// wrapping can fail (an allocation past the memory limit, a descriptor that cannot be
            /// made non-blocking). The constructor's own cleanup then asks this question with no
            /// reader to ask it through. A worker whose pipes could not even be looked at is not
            /// handed on - the same rule as a probe that fails below.
            if (!timeout_command_out)
                return false;

            /// Pending stderr disqualifies the worker only under `stderr_reaction` `throw` (see
            /// the pipe-mode probe for the reasoning): under every other reaction the bytes are
            /// taken off the pipe here and put through the reaction - logged against this query,
            /// the one that caused them - and the worker stays at a clean boundary. Under `none`
            /// the drain is also what keeps a chatty command from filling the pipe across borrows
            /// until it blocks in `write` without ever reading the next request.
            /// The drain polls and reads; either can fail, and this function may not throw. A
            /// worker whose pipes could not even be probed is not one to hand on: it is treated as
            /// dirty and discarded.
            try
            {
                if (!timeout_command_out->stderrThrows())
                {
                    static constexpr size_t stderr_drain_budget_ms = 100;
                    timeout_command_out->drainStderrFully(stderr_drain_budget_ms);
                }
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot probe the pipes of a pooled command; discarding its process");
                borrow.channel_was_dirty = true;
                return false;
            }

            const auto state = timeout_command_out->channelState();
            if (state.isClean())
                return true;

            /// Remember what was wrong, so that the discard can be reported for what it is. It is
            /// otherwise completely invisible: the query it happens in succeeds, and the next one
            /// simply gets a new process - so a command that writes to stderr after answering would
            /// quietly turn its `executable_pool` into a process per call.
            borrow.channel_was_dirty = true;
            unreported_dirty_channel = state;
            return false;
        }

        /// Reports a discarded worker, once per borrow. Leftover output is the command's mistake and
        /// is reported as such, together with whatever it left on stderr - this is the only place
        /// that output is ever going to be seen, since nothing else reads a discarded worker's pipes
        /// before they are closed. A child that simply exited is not a mistake and must not be
        /// described as one: it gets its own line and is not counted as a protocol violation.
        void reportDirtyChannelDiscard() noexcept
        {
            /// Taken and cleared up front, so that nothing below can leave a discard to be reported
            /// a second time - and so that everything that follows is inside the handler. Every
            /// line here allocates, this runs on `prepare`'s path where the stack is not unwinding,
            /// and the function is `noexcept`: a `MEMORY_LIMIT_EXCEEDED` from formatting a log
            /// message would otherwise terminate the server over a diagnostic.
            const auto state = std::exchange(unreported_dirty_channel, std::nullopt);
            if (!state)
                return;

            try
            {
                if (state->stdout_has_unread_output || state->stderr_has_unread_output)
                    ProfileEvents::increment(ProfileEvents::ExecutableUDFSharedMemoryDirtyChannelDiscards);

                logDirtyChannelDiscard("ShellCommandSharedMemorySource", *timeout_command_out, *state,
                    "The command must not write anything past its response frame, and must write diagnostics "
                    "before the response rather than after it.");
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        /// Not `const` for the same reason as `controlChannelIsClean`, which it asks.
        /// Asked by `prepare` and again by `cleanup`. A "no" is final: `prepare` acts on it - closes
        /// the worker's stdin, waits for it - and a later "yes" from a probe that happens to pass
        /// would hand that worker back to the pool. A "yes" can still turn into a "no": discarding
        /// is the safe direction. A worker whose stdin is closed cannot take a request either way.
        bool commandIsReused()
        {
            if (borrow.reuse_ruled_out)
                return false;

            const bool reused = is_pooled
                && command != nullptr
                && !borrow.command_is_invalid
                && !borrow.stdin_closed
                /// A worker no request was sent to is at the boundary it was borrowed at - a query
                /// cancelled between the constructor and its first `generate`, say.
                && (!borrow.request_sent || borrow.command_can_be_reused
                    || (configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read))
                /// The region's size is not asked about here: `cleanup` checks it against the cap
                /// as late as it can, and discards the worker itself if it is over.
                && controlChannelIsClean();

            borrow.reuse_ruled_out = !reused;
            return reused;
        }


        /// Closes the command's stdin, so that the child exits when it sees EOF. `command_is_reused`
        /// tells that the process is going back to the pool for another borrow: only then does the
        /// descriptor have to stay open. Every other process - a non-pooled one, or a pooled one
        /// that is being discarded - has to be closed here, because the waits that follow are the
        /// ones that do NOT close it themselves: the sampler's `tryWaitWithoutStatusCheck` polls
        /// for the whole `command_termination_timeout`, and `prepare` may call the blocking `wait`,
        /// which reaps the child before closing any pipe and would never return for a child that
        /// is waiting for its next request. (`~ShellCommand` does close the pipes before waiting,
        /// so it is not what this protects against.)
        void closeStdinIfNeeded(bool command_is_reused)
        {
            if (command_is_reused || borrow.stdin_closed || !command)
                return;

            borrow.stdin_closed = true;

            /// The constructor can fail before the write buffer exists - while creating a region or
            /// charging its memory. The child is already running and blocked in read by then, so
            /// its stdin still has to be closed: otherwise the wait in `cleanup` and in
            /// `~ShellCommand` blocks for the whole `command_termination_timeout` before the child is
            /// signalled.
            ///
            /// A buffer that was canceled is in the same position: `cleanup` cancels it where
            /// finalizing it threw, and a worker can be decided against after that (the late cap
            /// re-check), which brings it back here. `WriteBuffer::finalize` refuses a canceled
            /// buffer with a `LOGICAL_ERROR`, which in a debug or sanitizer build aborts the server
            /// rather than being caught - and there is nothing to finalize anyway. The descriptor
            /// still has to be closed, which is all this is here for.
            if (!timeout_command_in || timeout_command_in->isCanceled())
            {
                command->in.close();
                return;
            }

            try
            {
                timeout_command_in->finalize();
                (*timeout_command_in).reset();
            }
            catch (...)
            {
                /// The child exits when it sees EOF on its stdin, so the descriptor has to be closed
                /// even when finalizing the buffer failed: otherwise the child stays blocked in
                /// read and the wait for it never returns.
                timeout_command_in->cancel();
                command->in.close();
                throw;
            }

            command->in.close();
        }

        /// Reads the borrow's CPU and peak resident set out of `/proc` while the worker is still
        /// there to be read. Idempotent (see `UDFProcessSubtreeSampler::recordReleased`), so both
        /// the teardown that discards a worker and the ordinary end of a borrow call it.
        ///
        /// Never throws: it reads procfs and builds containers, so a memory limit can refuse it,
        /// and it runs both from `cleanup` - which the destructor calls - and from `prepare`, where
        /// failing a query that has already produced its rows over a profiling read would be worse
        /// than losing the measurement.
        void recordPooledResourceUsageNoThrow() noexcept
        {
            recordPooledReleaseNoThrow(configuration.sampler.get(), is_pooled, "ShellCommandSharedMemorySource");
        }

        /// Same, for the teardown paths (cancellation, cleanup) where an exception must not escape.
        void closeStdinNoThrow(bool command_is_reused)
        {
            try
            {
                closeStdinIfNeeded(command_is_reused);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource");
            }
        }

        /// Tears the output pipeline down: the executor, then the pipeline, then
        /// `output_read_buffer`, which points into the region's mapping and is read by the pipeline.
        ///
        /// The pipeline is moved into a temporary rather than assigned an empty one: assignment
        /// would construct one, and that allocates - in `generate`, right after a perfectly good
        /// answer was read, that would fail the query and throw away a healthy worker over cleaning
        /// up the result; in `cleanup`, it is on a path that runs from a destructor.
        void destroyOutputPipeline()
        {
            output_executor.reset();
            {
                QueryPipeline discarded = std::move(output_pipeline);
            }
            output_read_buffer.reset();
        }

        /// The write buffer must be finalized (or canceled) before it is destroyed. On the pool
        /// path stdin stays open for reuse, so it was not finalized while sending requests.
        void finalizeStdinBufferNoThrow()
        {
            if (timeout_command_in && !timeout_command_in->isFinalized() && !timeout_command_in->isCanceled())
            {
                try
                {
                    timeout_command_in->finalize();
                    (*timeout_command_in).reset();
                }
                catch (...)
                {
                    timeout_command_in->cancel();
                    tryLogCurrentException("ShellCommandSharedMemorySource");
                }
            }
        }

        /// Settles what the holder keeps before it is handed back, and measures it: a worker that is
        /// not kept is discarded together with its region, and the region of one that is - or of
        /// one never taken out of the holder - is checked against the cap once more and measured
        /// for the charge the holder takes back (`acquireChargeFromBorrower`). Returns that size;
        /// zero without a holder, or when the measurement failed. A worker discarded on the way
        /// turns `keep_command` false.
        size_t settleWorkerForHandBack(bool & keep_command, bool worker_untouched)
        {
            if (!command_holder)
                return 0;

            if (!keep_command && !worker_untouched)
            {
                /// The worker process is being discarded (protocol failure, child death,
                /// overproduction, cancellation, etc.). Its pooled shared-memory region belongs
                /// to that process, so it goes too, even when an earlier borrow created it,
                /// instead of being left on the reused holder, with its charge, for a
                /// replacement process - which could not use it anyway, since it is the process
                /// that inherits a region's descriptor at `exec`, and a replacement gets its
                /// own. The charge for the region is this borrow's (`releaseChargeToBorrower`)
                /// and is released by `cleanup`, and a holder left without a region takes none back
                /// (`acquireChargeFromBorrower`).
                discardTakenWorker();
            }
            /// Otherwise the region stays with the worker (kept, or never taken out), at
            /// whatever size this borrow grew it to. It is sealed against shrinking, so there
            /// is no trimming it back to `shared_memory_size` for the idle time;
            /// `shared_memory_max_size` is what a pooled worker may hold, and the holder
            /// charges the server for exactly that.

            /// Checked once more, as late as it can be: `keep_command` was decided in `cleanup`, and
            /// the command is alive in between - a file it extended past the cap since then would be
            /// charged to the server and handed to the next query along with the worker. The window
            /// between this read and the charge after it cannot be closed (the command can extend the
            /// file at any instant), which is why the charge is capped as well. It cannot come after
            /// the borrow's charge is released: a worker found over the cap is destroyed
            /// here, which can take up to `command_termination_timeout`, and its region stays
            /// mapped until then - it is to be counted against the query for all of that time,
            /// not left uncounted by every tracker.
            if (keep_command && !regionIsWithinTheCap())
            {
                keep_command = false;
                discardTakenWorker();
            }

            /// The same for a worker that stays in the holder without having been taken out
            /// (`worker_untouched`): it goes back to the pool just as well, and a file its command
            /// stretched past the cap must not be handed on with it, charged at the cap and no
            /// higher, for as long as nobody borrows it again.
            /// A file whose size cannot be read is not one to hand on either.
            if (worker_untouched)
            {
                bool over_the_cap = true;
                try
                {
                    over_the_cap = command_holder->sharedMemoryRegionOverTheCap(shared_memory_max_size) != nullptr;
                }
                catch (...)
                {
                    tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot measure the pooled region; discarding its worker");
                }
                if (over_the_cap)
                    dropRegionAndWorker();
            }

            /// Measure before the query charge is released. An unknown footprint disqualifies
            /// both an active worker and one still held after a failed constructor.
            size_t persistent_bytes = 0;
            try
            {
                fiu_do_on(FailPoints::executable_udf_fail_handback_measurement,
                {
                    throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Injected shared-memory footprint measurement failure");
                });
                /// As the cap check just above read it - for a kept worker and for one never
                /// taken out alike; a discarded one has no region left.
                persistent_bytes = std::min(command_holder->lastSeenSharedMemorySize(), shared_memory_max_footprint);
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandSharedMemorySource", "Cannot measure the pooled region; discarding its worker");
                keep_command = false;
                discardTakenWorker();
            }
            return persistent_bytes;
        }

        /// Hands the holder back to the pool, with the worker if it is kept.
        void returnHolderToPool(bool keep_command)
        {
            if (keep_command)
                command_holder->returnCommand(std::move(command));

            /// A worker that is not going back to the pool has to die before its slot does: the
            /// query waiting for that slot starts a replacement at once, so leaving this process
            /// to be destroyed later - with a `command_termination_timeout` wait in front of it -
            /// lets the pool run over `pool_size` for as long as that takes. Its stdin was closed
            /// by `cleanup`, so the child is already on its way out.
            command = nullptr;

            command_holder.returnToPool();
        }

        void cleanup()
        {
            /// Tear down the output pipeline first. Its parsing threads (`input_format_parallel_parsing`)
            /// read straight out of the shared-memory region through output_read_buffer, so they must be
            /// joined before the child is reaped and before the region is unmapped below. `generate`
            /// does this in order on the normal path; here it also covers the destructor path (query
            /// cancellation, an exception downstream) where the pipeline is still alive.
            destroyOutputPipeline();

            /// The reuse decision, made once and used for everything below.
            bool keep_command = commandIsReused();

            /// A process and its region live and die together: the process reached it by
            /// inheriting its descriptor at `exec`, and a region created for it cannot be
            /// swapped for another one behind its back - it would go on opening the descriptor it
            /// was given. So a borrow that failed after creating the region does not keep the
            /// process either. It is a fresh one in that case (a returned process comes with its
            /// region already there), so nothing of value is lost.
            if (!borrow.constructor_finished && borrow.region_created_by_this_borrow)
                keep_command = false;

            /// The converse of the same rule. A borrow that failed before it took the worker out of
            /// the holder - the constructor charging the worker's region to a query that is at its
            /// memory limit, say - touched neither the process nor its region: no request reached
            /// it, and it is still sitting in the holder with the very descriptor it was started
            /// with. `keep_command` is false here only because there is no `command` in this
            /// object to keep. Its region has to stay with it all the same: dropping it and
            /// keeping the process would have the next borrow create a new region that the worker
            /// has never heard of, and read its answers out of memory the worker never writes to.
            const bool worker_untouched = command == nullptr
                && command_holder && command_holder->hasReturnedCommand()
                && !borrow.region_created_by_this_borrow;

            reportDirtyChannelDiscard();

            /// Before the stdin close below, for the same reason `prepare` samples before its own:
            /// a discarded child exits on that EOF, and a zombie has no `/proc` mm fields left to
            /// read. Idempotent, so a borrow that already went through `prepare` is unaffected.
            recordPooledResourceUsageNoThrow();

            /// A child that is not going back to the pool exits on stdin EOF, so its stdin must be
            /// closed here as well: `generate` closes it on the normal path, but not when the source
            /// is torn down before that (query cancellation, an exception downstream), and never for
            /// a pooled worker, which only turns out to be discarded at this point. A child left
            /// blocked in read(stdin) would make the sampler's wait below spin for the whole
            /// `command_termination_timeout`.
            closeStdinNoThrow(keep_command);

            finalizeStdinBufferNoThrow();

            /// As in `ShellCommandSource::cleanup`. The pool path was measured above, before the stdin close.
            if (configuration.sampler && !is_pooled && command)
                recordNonPooledUsage(*configuration.sampler, *command, "ShellCommandSharedMemorySource");

            if (borrow.command_is_invalid)
                command = nullptr;

            const size_t persistent_bytes = settleWorkerForHandBack(keep_command, worker_untouched);

            /// A non-pooled command and its region go before the charge for the region does, by the
            /// same rule as a discarded pooled worker (`settleWorkerForHandBack`): `~ShellCommand` can wait up to
            /// `command_termination_timeout` for a command that was not waited for (`check_exit_code`
            /// off, a cancelled query), and the region stays resident until the command is gone.
            if (!command_holder)
            {
                command = nullptr;
                shared_memory_region.reset();
            }

            /// Release the per-borrow memory charge on the query thread.
            query_memory_charge.releaseAll();

            /// A region the holder still owns outlives this borrow, so it is charged
            /// again - globally this time - now that the borrow's charge is gone. There is no way
            /// to move a charge between trackers atomically, so one of the two orders has to be
            /// picked: this one leaves the bytes uncounted for the moment in between, the other
            /// would count them twice. Undercounting for a moment can at most let a concurrent
            /// allocation through (memory limits are approximate anyway - see
            /// `max_untracked_memory`), while double counting could fail a query that fits and
            /// would inflate the peak the server reports. The borrow side of the hand-over
            /// (`releaseChargeToBorrower`) errs the same way, for the same reason.
            if (command_holder)
                command_holder->acquireChargeFromBorrower(persistent_bytes);

            if (command_holder)
                returnHolderToPool(keep_command);
        }

        /// The query context, replaced once in the constructor by the one for reading the
        /// command's output (`makeContextForReadingCommandOutput`).
        ContextPtr context;

        /// Configuration, fixed at construction.
        const std::string format;
        const SharedHeader sample_block;
        const ShellCommandSourceConfiguration configuration;
        const bool is_pooled;
        /// Whether the command's stderr fails the query (`stderr_reaction` `throw`). Known before
        /// the reader is built, for `inspectPooledWorkerBeforeTheBorrow`.
        const bool stderr_throws;
        const bool check_exit_code;
        const size_t shared_memory_max_size;
        /// The cap in the unit footprints come in - whole pages: a region of 16 bytes holds a page,
        /// and a cap of 16 bytes has to mean that page, not fail it on every borrow.
        const size_t shared_memory_max_footprint;

        /// The pipes carry only the control frames - a few dozen bytes each way - so the buffers
        /// are sized for them, not for data: the default would cost every borrow two allocations
        /// of a megabyte, charged to the query.
        static constexpr size_t control_channel_buffer_size = 4096;

        Block input_header;

        /// Whether the one request a function without arguments makes has been made.
        bool zero_argument_request_sent = false;
        /// Set by `ensureRegionFits` when it grew the region while memory-limit exceptions were
        /// blocked, so the charge went through without `max_memory_usage` being checked; cleared
        /// by `serializeInput` once it has checked it.
        bool memory_limit_check_pending = false;

        SharedMemoryRegionPtr shared_memory_region;
        /// What this borrow has charged its query for `shared_memory_region`.
        QueryMemoryCharge query_memory_charge;

        /// Whether the process served an earlier borrow (and may have left output on its pipes),
        /// as opposed to one started for this borrow or as a replacement during it.
        bool worker_is_reused = false;

        /** What this borrow has done to the worker so far, which together decides the worker's fate
          * when the source is torn down: whether it goes back to the pool with its region
          * (`commandIsReused`, `cleanup`) or is discarded. Only ever read and written on the query
          * thread.
          */
        struct BorrowState
        {
            /// Set at the end of the constructor. A borrow that failed after creating the region
            /// does not keep the process either (see `cleanup`).
            bool constructor_finished = false;
            /// Whether the region was created by this borrow, as opposed to taken over together
            /// with a worker that served an earlier one.
            bool region_created_by_this_borrow = false;

            /// Set while the input for the next request is being serialized, before that request
            /// is sent to the child - see the catch-all in `generate`.
            bool preparing_input = false;
            /// Whether a request has been sent to the worker by this borrow (`sendRequest`).
            bool request_sent = false;
            /// Set when input preparation fails before a request reaches a pooled child, or when
            /// the child answered in full and the query fails anyway. Unlike an incomplete or
            /// cancelled invocation, this leaves the child at a known protocol boundary.
            bool command_can_be_reused = false;
            /// The child is out of step with the protocol or its state is unknown: it is discarded.
            bool command_is_invalid = false;
            /// Whether the command's stdin has been closed (`closeStdinIfNeeded`).
            bool stdin_closed = false;

            /// Latched by `controlChannelIsClean` and never cleared - see there for why the answer
            /// must not be re-derived.
            bool channel_was_dirty = false;
            /// Latched by `commandIsReused`, see there.
            bool reuse_ruled_out = false;
        };

        BorrowState borrow;

        std::unique_ptr<TimeoutReadBufferFromFileDescriptor> timeout_command_out;
        std::unique_ptr<TimeoutWriteBufferFromFileDescriptor> timeout_command_in;

        size_t current_read_rows = 0;

        /// What `controlChannelIsClean` found - which is not a mere question: it drains stderr and
        /// latches its answer - until `reportDirtyChannelDiscard` reports and clears it.
        std::optional<TimeoutReadBufferFromFileDescriptor::ChannelState> unreported_dirty_channel;

        QueryPipeline input_pipeline;
        std::unique_ptr<PullingPipelineExecutor> input_executor;

        /// output_read_buffer points into the region's mapping and is read by the output pipeline
        /// (including its parallel-parsing threads), so it must outlive the pipeline:
        /// declared first, therefore destroyed last. `cleanup` tears all three down in order.
        std::unique_ptr<ReadBufferFromMemory> output_read_buffer;
        QueryPipeline output_pipeline;
        std::unique_ptr<PullingPipelineExecutor> output_executor;

        /// The worker process and its pool holder are taken over after EVERY other member, because
        /// every other member has to be able to throw without costing a healthy pooled worker: until
        /// this object owns the holder, together with the worker it may hold, it is still the
        /// caller's. The pool's slot does not depend on this order - `BorrowedShellCommandHolder`
        /// returns the holder to the pool from wherever it is - only the worker does.
        /// `timeout_command_out` allocates its buffer, and both `QueryPipeline` members allocate in
        /// their default constructor, so this is not a theoretical ordering.
        ///
        /// Being last also makes them the first members destroyed, which is safe: `cleanup` runs
        /// before any of that and has already torn the pipelines down and
        /// handed the command back, and ~TimeoutReadBufferFromFileDescriptor deliberately does not
        /// touch the descriptors the command owns.
        ///
        /// The holder is declared before the command, so the command is destroyed before it: on a
        /// path on which `cleanup` did not get to return the holder, its destructor does, and a
        /// worker that is not going back with it has to die before its slot is released (see
        /// `returnHolderToPool`).
        BorrowedShellCommandHolder command_holder;
        std::unique_ptr<ShellCommand> command;
    };

}

Pipe createShellCommandSharedMemoryPipe(
    ContextPtr context,
    const ShellCommandSourceCoordinator::Configuration & coordinator_configuration,
    SharedHeader sample_block,
    ShellCommandHolder::ShellCommandBuilderFunc build_command,
    Pipe input_pipe,
    const ShellCommandSourceConfiguration & source_configuration,
    BorrowedShellCommandHolder command_holder)
{
    return Pipe(std::make_unique<ShellCommandSharedMemorySource>(
        std::move(context),
        coordinator_configuration,
        std::move(sample_block),
        std::move(build_command),
        std::move(input_pipe),
        source_configuration,
        std::move(command_holder)));
}

}
