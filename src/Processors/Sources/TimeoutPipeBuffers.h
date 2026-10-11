#pragma once

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/ShellCommandSettings.h>
#include <Common/Stopwatch.h>
#include <Common/UDFProcessSubtreeSampler.h>
#include <Common/logger_useful.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <boost/circular_buffer.hpp>

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>

/// The pipes of an external command with timeouts: reading its stdout while keeping its stderr
/// drained and put through `stderr_reaction`, and writing its stdin. Shared by the pipe and the
/// shared-memory transports of `ShellCommandSourceCoordinator`.

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int TIMEOUT_EXCEEDED;
}

bool tryMakeFdNonBlocking(int fd);

void makeFdNonBlocking(int fd);

bool tryMakeFdBlocking(int fd);

void makeFdBlocking(int fd);

int pollWithTimeout(pollfd * pfds, size_t num, size_t timeout_milliseconds);

/// What the pipe `fd` holds at this moment, up to `max_size` bytes: exactly the bytes that are there
/// (`FIONREAD`), so the read never waits, whatever the mode of the descriptor, and a writer that
/// keeps the pipe busy - a descendant of a dead command that inherited its write end - cannot hold
/// the reader. Nothing for a closed descriptor (-1). Throws if the pipe cannot be measured.
String readWhatThePipeHolds(int fd, size_t max_size);

bool pollFd(int fd, size_t timeout_milliseconds, int events);

class TimeoutReadBufferFromFileDescriptor : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit TimeoutReadBufferFromFileDescriptor(
        int stdout_fd_,
        int stderr_fd_,
        size_t timeout_milliseconds_,
        ExternalCommandStderrReaction stderr_reaction_,
        UDFProcessSubtreeSampler * sampler_,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(buffer_size)
        , stdout_fd(stdout_fd_)
        , stderr_fd(stderr_fd_)
        , timeout_milliseconds(timeout_milliseconds_)
        , stderr_reaction(stderr_reaction_)
        , sampler(sampler_)
        /// Allocated up front rather than on first use: the first use may be from a `noexcept`
        /// probe on a cleanup path (`controlChannelIsClean`, `pipeWorkerIsAtACleanBoundary`),
        /// where an allocation refused by the memory limit would terminate the server.
        , stderr_read_buf(new char[BUFFER_SIZE])
    {
        makeFdNonBlocking(stdout_fd);
        makeFdNonBlocking(stderr_fd);

        pfds[0].fd = stdout_fd;
        pfds[0].events = POLLIN;
        pfds[1].fd = stderr_fd;
        pfds[1].events = POLLIN;

        /// Both descriptors are polled even under `ExternalCommandStderrReaction::NONE`. "None"
        /// says what to do with the command's stderr - nothing - not that the pipe may be left
        /// unread: a pipe nobody reads fills up, and the command then blocks in `write` with its
        /// answer unfinished. That is a hang for a single-shot command and worse for a pooled one,
        /// where the bytes accumulate across borrows until some later query is the one that stalls.
        /// So the bytes are read and thrown away here (`nextImpl` matches no reaction for `NONE`),
        /// which is what "ignore this output" has to mean for a pipe.
    }

    bool nextImpl() override
    {
        if (stdout_is_done)
            return false;

        size_t bytes_read = 0;

        /// One budget for the whole call rather than one per wake-up. `command_read_timeout` says
        /// how long this read may wait for the command to say something on its stdout, and this
        /// loop is woken by things that are not that: stderr the command drips out, a stderr pipe
        /// that has hung up, an interrupted read. Restarting the timeout at each of them would let
        /// a command that never answers hold the query for as long as it keeps making noise on the
        /// other pipe - which is the same unbounded wait, only reached the long way round.
        ///
        /// And not even one per call while a frame deadline is armed (`armFrameDeadline`): each
        /// call returns as soon as anything arrives, so a command that drips its answer out a
        /// byte at a time would otherwise get a fresh budget for every byte.
        const UInt64 deadline_ns = frame_deadline_ns ? frame_deadline_ns : readDeadlineNs();

        while (!bytes_read)
        {
            pfds[0].revents = 0;
            pfds[1].revents = 0;
            int num_events = pollWithTimeout(pfds, num_pfds, remainingMs(deadline_ns));
            if (num_events <= 0)
                throwReadTimeout();

            bool has_stdout = pfds[0].revents > 0;
            bool has_stderr = pfds[1].revents > 0;

            if (has_stderr)
                readStderrOnce();

            if (has_stdout)
            {
                ssize_t res = ::read(stdout_fd, internal_buffer.begin(), internal_buffer.size());

                if (-1 == res && errno != EINTR)
                    throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from pipe");

                if (res == 0)
                {
                    /// Late diagnostics are handled by `waitDrainingOutput` with the command's
                    /// termination budget. Repeated EOF probes must not wait for future stderr.
                    stdout_is_done = true;
                    break;
                }

                if (res > 0)
                {
                    bytes_read += res;
                    if (sampler)
                    {
                        sampler->recordOutputBytes(static_cast<size_t>(res));
                        /// The child produced this output, so it was running; sample its subtree VmHWM.
                        /// It may have already exited (short-lived UDF) — then the read finds no VmHWM
                        /// and this is a harmless no-op. Also a no-op on the pool path (executable_root_pid <= 0).
                        sampler->sampleExecutablePeak();
                    }
                }
            }

            /// Checked after the wake-up, not before it, so that a `command_read_timeout` of zero
            /// keeps meaning "probe once" rather than "fail without looking".
            ///
            /// It has to be checked at all because an exhausted budget turns the poll above into a
            /// readiness probe rather than a wait, and a probe is satisfied by anything pending -
            /// including stderr the command is flooding out while never answering on stdout. Left
            /// to the poll alone, that loop would spin at full speed for as long as the command
            /// kept writing, and `command_read_timeout` would never be reached.
            if (!bytes_read && remainingMs(deadline_ns) == 0)
                throwReadTimeout();
        }

        if (bytes_read > 0)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
        }
        else
        {
            /// Concluding best-effort tail sample. The function has closed stdout, so
            /// this is the last point it is typically still alive; take one final
            /// subtree sample (bypassing the throttle) to catch a peak reached after
            /// the last IO sample but before EOF. Fired once; a no-op on the pool path
            /// and harmless if the child has already exited. This is a single tail
            /// attempt, not continuous sampling during the post-output reap.
            /// This concluding sample is best-effort and is intentionally NOT covered
            /// by a deterministic test — whether the child is still resident when EOF
            /// is detected is timing-dependent, so any assertion on it would be
            /// flaky; the deterministic guarantees (output-phase capture, max-not-sum,
            /// parent-independence) are covered by the integration tests.
            if (sampler && !final_sample_taken)
            {
                final_sample_taken = true;
                sampler->sampleExecutablePeak(/*is_final=*/true);
            }
            return false;
        }

        return true;
    }

    ~TimeoutReadBufferFromFileDescriptor() override
    {
        /// Do not touch stdout_fd/stderr_fd here: they are owned by the ShellCommand, which may
        /// already have closed them (`ShellCommand::wait` closes the streams), and the numbers may
        /// be recycled by another thread. An fcntl on them would corrupt an unrelated descriptor.

        // Handle LOG_FIRST and LOG_LAST cases with circular buffer
        if (!stderr_result_buf.empty())
        {
            String stderr_result;
            stderr_result.reserve(stderr_result_buf.size());
            stderr_result.append(stderr_result_buf.begin(), stderr_result_buf.end());

            if (stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST || stderr_reaction == ExternalCommandStderrReaction::LOG_LAST)
            {
                LOG_WARNING(
                    getLogger("ShellCommandSource"),
                    "Executable generates stderr at the {}: {}",
                    stderr_reaction == ExternalCommandStderrReaction::LOG_FIRST ? "beginning" : "end",
                    stderr_result);
            }
        }
    }

    /// Check if stderr was accumulated (for THROW mode)
    bool hasStderr() const { return stderr_full_output.has_value(); }

    /// Get accumulated stderr content (for THROW mode)
    const String & getStderr() const { return *stderr_full_output; }

    /// Get buffered stderr content from circular buffer (for LOG_FIRST/LOG_LAST modes)
    /// Clears the buffer to prevent duplicate logging in destructor
    String consumeBufferedStderr()
    {
        if (stderr_result_buf.empty())
            return {};
        String result;
        result.reserve(stderr_result_buf.size());
        result.append(stderr_result_buf.begin(), stderr_result_buf.end());
        stderr_result_buf.clear();
        return result;
    }

    /// What the command's pipes say about it right now. Three separate answers rather than one
    /// verdict, because they are three different things and the caller reports them differently:
    /// output left on a pipe is a protocol violation by the command, a hangup on stdout is a child
    /// that is simply gone, and neither should be described as the other.
    struct ChannelState
    {
        /// The command wrote past its response frame. `exchange` reads exactly that frame, so the
        /// leftover is read by whichever query borrows this worker next, as the status varint of a
        /// response to its own request.
        bool stdout_has_unread_output = false;

        /// The write end of stdout is gone: the child exited, or closed its stdout, which ends the
        /// protocol either way. Not a violation and not something to blame the command for - but
        /// still a worker that must not be handed on.
        bool stdout_hung_up = false;

        /// Same hazard as unread stdout, only quieter: `nextImpl` drains stderr on every read, so
        /// what is left here is picked up by the next query and reported as its output - and under
        /// `stderr_reaction = throw`, fails it.
        bool stderr_has_unread_output = false;

        bool isClean() const { return !stdout_has_unread_output && !stdout_hung_up && !stderr_has_unread_output; }
    };

    /// Whether a pipe holds bytes nobody has read: what a pooled process wrote after its last answer.
    static bool pipeHasPendingOutput(int fd) noexcept { return (ShellCommand::pendingEvents(fd) & POLLIN) != 0; }

    /// Whether the command's stderr holds bytes nobody has read, whatever the reaction.
    bool stderrHasPendingOutput() const noexcept { return pipeHasPendingOutput(stderr_fd); }

    /// `consider_buffered_output` says whether bytes this buffer has read but not handed on count
    /// as unread output. They do for the shared-memory transport, where every byte of the response
    /// frame is accounted for and a leftover is a protocol violation. They do not for the pipe
    /// transport, where a format reader may legitimately hold buffered bytes it did not parse, and
    /// only what is still in the kernel pipe is evidence that the command spoke out of turn.
    ChannelState channelState(bool consider_buffered_output = true) const noexcept
    {
        const Int16 stdout_events = ShellCommand::pendingEvents(stdout_fd);

        ChannelState state;
        state.stdout_has_unread_output
            = (consider_buffered_output && available() > 0) || (stdout_events & POLLIN) != 0;
        state.stdout_hung_up = (stdout_events & (POLLHUP | POLLERR | POLLNVAL)) != 0;

        /// A hangup on stderr, unlike on stdout, is not asked about: a command may close its own
        /// stderr, and it stays hung up for the rest of its life, so reading that as leftover output
        /// would discard a healthy worker on every borrow and turn the pool into a process per call.
        /// A child that has actually exited hangs up stdout too, which is where that is caught.
        ///
        /// Asked only under `ExternalCommandStderrReaction::THROW`, the one reaction under which
        /// stderr that reaches the wrong query is a wrong verdict rather than a log line in the
        /// wrong place. Under every other one the next borrow takes what is left off the pipe
        /// without the reaction and logs it against the worker (`quarantineReusedWorker`,
        /// `discardStderrLeftByAPreviousBorrow`), and a command that keeps logging is not worth
        /// a process per call.
        if (stderr_reaction == ExternalCommandStderrReaction::THROW)
            state.stderr_has_unread_output = (ShellCommand::pendingEvents(stderr_fd) & (POLLIN | POLLERR | POLLNVAL)) != 0;

        return state;
    }

    /// The same read as `consumePendingStderr`, without putting what it finds through the reaction.
    /// For a caller that has to clear the pipe of somebody else's output - see
    /// `discardStderrLeftByAPreviousBorrow`.
    String consumePendingStderrWithoutReaction() const
    {
        return readPendingStderr();
    }

    /// Reads whatever is sitting unread on stderr right now, so a caller that is about to throw the
    /// command away can report it. Best-effort and non-blocking - the descriptor is in non-blocking
    /// mode and nothing here waits for more - and capped, because the amount a broken command can
    /// have left there is not bounded by anything else.
    ///
    /// What it reads also goes through the configured reaction, which matters for
    /// `ExternalCommandStderrReaction::THROW`: output the command produced after its response is
    /// still output it produced, and `throw` promises the query fails for it. Reported only as a
    /// log line, it would leave the query succeeding against the contract the setting states. The
    /// other reactions are served by the discard report the caller writes from the returned string.
    ///
    /// Deliberately not `noexcept`: it builds a string, and an allocation on this teardown path can
    /// be refused by the memory tracker. That has to reach the caller's handler, which gives up on
    /// the diagnostic, rather than terminate the server over it.
    String consumePendingStderr()
    {
        String result = readPendingStderr();

        if (stderr_reaction == ExternalCommandStderrReaction::THROW)
            accumulateStderrForThrow(result);

        return result;
    }

    String readPendingStderr() const
    {
        return readWhatThePipeHolds(stderr_fd, MAX_PENDING_STDERR_SIZE);
    }

    /// Whether anything is done with the command's stderr beyond taking it off the pipe. `NONE`
    /// drops what it reads, so a caller that would only read in order to drop has nothing to do.
    bool stderrIsObserved() const { return stderr_reaction != ExternalCommandStderrReaction::NONE; }

    /// Whether stderr the command writes fails the query. The one reaction under which a byte of
    /// stderr that was not attributed to the query that caused it is a wrong verdict, and not just
    /// a log line in the wrong place.
    bool stderrThrows() const { return stderr_reaction == ExternalCommandStderrReaction::THROW; }

    /// Reads stderr until the pipe is empty or `budget_milliseconds` runs out.
    ///
    /// For the moment a pooled worker is handed on under `ExternalCommandStderrReaction::NONE`.
    /// Nothing is done with those bytes - that is what `none` means - but they cannot be left on
    /// the pipe either: they accumulate across borrows, and the command blocks in `write` once the
    /// pipe fills, so the borrow after that finds a worker that never reads its request. `none`
    /// promises a chatty command does not block, and this is where that promise is kept for a
    /// worker that goes back into the pool.
    ///
    /// With `with_reaction` false the bytes are dropped whatever the reaction is. That is for a
    /// borrow that clears the pipe of a *previous* borrow's output before sending its own request
    /// (`quarantineReusedWorker`, `discardStderrLeftByAPreviousBorrow`): those bytes are the
    /// earlier query's, and putting them through this query's `stderr_reaction` would fail this
    /// query, under `throw`, for a diagnostic it did not cause. The caller has already read and
    /// reported what it could of them (`consumePendingStderrWithoutReaction`, which is capped);
    /// this takes the rest, and whatever the command keeps writing during the drain, the same way.
    void drainStderrFully(size_t budget_milliseconds, bool with_reaction = true, size_t bytes_already_read = 0)
    {
        const UInt64 deadline_ns = monotonicDeadlineNs(budget_milliseconds);

        /// An empty pipe is answered at once: nothing pending, nothing to wait for. But a pipe
        /// that was full may belong to a command in the middle of a burst - blocked in `write`,
        /// and only now, with room made, being woken to write the rest. That wake-up takes a
        /// moment, longer on a loaded machine, and a poll with no timeout would find the pipe
        /// momentarily empty and declare the burst over, leaving its tail to land in the middle of
        /// the next request. So once enough has been read that the writer may have been blocked,
        /// each further poll waits a little for it, up to the budget.
        ///
        /// Only then: a command that wrote a log line and went back to waiting for its next
        /// request is not blocked on anything, and waiting for it would put an idle window on
        /// every pooled call that logs, which is the hot path of `executable_pool`. A writer can
        /// be blocked only on a pipe without room for its write - less than `PIPE_BUF` free - so
        /// that is what is looked for before every read, until it is seen once. The bytes consumed
        /// for the borrow's diagnostic count as well: the pipe held them too.
        static constexpr size_t wait_for_writer_ms = 20;
        bool writer_may_be_blocked = false;
        size_t read_before = bytes_already_read;

        while (!stderr_is_done)
        {
            const size_t remaining_ms = remainingMs(deadline_ns);
            if (remaining_ms == 0)
                return;

            pfds[1].revents = 0;
            const size_t wait_ms = writer_may_be_blocked ? std::min(remaining_ms, wait_for_writer_ms) : 0;
            if (pollWithTimeout(&pfds[1], 1, wait_ms) <= 0 || pfds[1].revents == 0)
                return;

            if (!writer_may_be_blocked)
                writer_may_be_blocked = stderrPipeIsNearlyFull(std::exchange(read_before, 0));
            readStderrOnce(with_reaction);
        }
    }

    /// Whether the stderr pipe has - counting `bytes_already_read` that were just taken off it - less
    /// than `PIPE_BUF` bytes free: the only state in which the command can be blocked writing to it.
    /// Answered "yes" where the pipe cannot be measured, which only costs the wait.
    bool stderrPipeIsNearlyFull(size_t bytes_already_read) const
    {
#if defined(OS_LINUX)
        int queued = 0;
        const int capacity = ::fcntl(stderr_fd, F_GETPIPE_SZ);
        if (capacity <= 0 || 0 != ::ioctl(stderr_fd, FIONREAD, &queued) || queued < 0)
            return true;
        return static_cast<size_t>(queued) + bytes_already_read + PIPE_BUF > static_cast<size_t>(capacity);
#else
        UNUSED(bytes_already_read);
        return true;
#endif
    }

    /// Clears what a pooled worker left on its stderr after the response of an earlier borrow, before
    /// this borrow sends its first request, and logs it against nobody: those bytes are the earlier
    /// query's, and putting them through this query's reaction would fail it, under `throw`, for a
    /// diagnostic it did not cause. The report is capped; the pipe is not - a worker that filled it
    /// is blocked in `write` and will not read the request until there is room - so what is left
    /// beyond the cap, and what the command writes while this drains, is dropped the same way.
    void clearStderrOfAnEarlierBorrow(const char * log_name)
    {
        /// The usual case, a quiet pipe, answered with one `poll`.
        if (!stderrHasPendingOutput())
            return;

        static constexpr size_t stderr_drain_budget_ms = 100;
        const String leftover = consumePendingStderrWithoutReaction();
        drainStderrFully(stderr_drain_budget_ms, /*with_reaction=*/ false, leftover.size());
        if (!leftover.empty())
            LOG_WARNING(
                getLogger(log_name),
                "The process of a pooled command had unread output on its stderr when it was borrowed, so it was "
                "written after the response of an earlier invocation. It is reported here rather than against "
                "this query, which did not cause it. Stderr: {}",
                leftover);
    }

    /// For a caller that reads the command's stderr itself and needs those bytes to go through the
    /// configured reaction all the same - the bounded wait that reaps a command drains both pipes,
    /// and that is the last stretch in which a command can still write.
    void consumeStderrBytes(std::string_view str) { consumeStderrChunk(str); }

    /// Makes every read until `disarmFrameDeadline` share one deadline, `timeout_milliseconds`
    /// from now, rather than each read getting its own: what bounds the time a whole response
    /// may take to arrive, not just the gap between two of its pieces.
    void armFrameDeadline() noexcept { frame_deadline_ns = readDeadlineNs(); }
    void disarmFrameDeadline() noexcept { frame_deadline_ns = 0; }

private:
    /// One chunk of the command's stderr, put through the configured reaction. `NONE` matches
    /// nothing and the bytes are dropped - which is exactly what it is for: they still have to be
    /// taken off the pipe, or the command blocks in `write` once the pipe fills up.
    void consumeStderrChunk(std::string_view str)
    {
        switch (stderr_reaction)
        {
            case ExternalCommandStderrReaction::NONE:
                break;
            case ExternalCommandStderrReaction::THROW:
                accumulateStderrForThrow(str);
                break;
            case ExternalCommandStderrReaction::LOG:
                LOG_WARNING(getLogger("TimeoutReadBufferFromFileDescriptor"), "Executable generates stderr: {}", str);
                break;
            case ExternalCommandStderrReaction::LOG_FIRST:
            {
                const size_t to_insert = std::min(stderr_result_buf.reserve(), str.size());
                if (to_insert > 0)
                    stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.begin() + to_insert);
                break;
            }
            case ExternalCommandStderrReaction::LOG_LAST:
                stderr_result_buf.insert(stderr_result_buf.end(), str.begin(), str.end());
                break;
        }
    }

    /// Accumulating stops at `MAX_STDERR_SIZE`, but reading never does: the point of reading is to
    /// keep the command from blocking, and that is true whether or not the bytes are still wanted.
    void accumulateStderrForThrow(std::string_view str)
    {
        /// Nothing read is not something written. `hasStderr` is "the command has produced output
        /// on its stderr", and under `throw` that is a verdict on the query - so a read that came
        /// back empty must leave it alone. The probes call this with whatever the pipe held, and
        /// for a worker discarded over its *stdout* the pipe usually holds nothing at all: a
        /// value emplaced here would fail a query whose rows were already correct, with an empty
        /// message to explain it.
        if (str.empty())
            return;

        const size_t current_size = stderr_full_output ? stderr_full_output->size() : 0;
        if (current_size >= MAX_STDERR_SIZE)
            return;

        if (!stderr_full_output)
            stderr_full_output.emplace();

        stderr_full_output->append(str.substr(0, MAX_STDERR_SIZE - current_size));
    }

    /// Reads what is pending on stderr once and puts it through the reaction. A pipe that is done -
    /// EOF, or an error that reading again will not fix - is dropped out of the poll set, because
    /// `poll` reports a hung-up descriptor immediately and forever: left in, it would turn the wait
    /// for the command's next answer into a busy loop that spins a core and never reaches
    /// `command_read_timeout`. A command closing its own stderr is ordinary (see
    /// `shm_udf_quiet_stderr.py`), so this is not an error path.
    ///
    /// Returns the number of bytes read.
    size_t readStderrOnce(bool with_reaction = true)
    {
        const ssize_t res = ::read(stderr_fd, stderr_read_buf.get(), BUFFER_SIZE);
        if (res > 0)
        {
            if (with_reaction)
                consumeStderrChunk(std::string_view(stderr_read_buf.get(), static_cast<size_t>(res)));
            return static_cast<size_t>(res);
        }

        if (res < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            return 0;

        stopPollingStderr();
        return 0;
    }

    void stopPollingStderr() noexcept
    {
        stderr_is_done = true;
        /// `poll` ignores a negative descriptor and leaves its `revents` zero.
        pfds[1].fd = -1;
        pfds[1].revents = 0;
    }

    [[noreturn]] void throwReadTimeout() const
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);
    }

    /// A monotonic deadline `timeout_milliseconds` from now. Clamped, because
    /// `command_read_timeout` is not bounded anywhere: a huge value would wrap the multiplication
    /// or the addition and put the deadline in the past, turning every wait into a probe.
    UInt64 readDeadlineNs() const noexcept { return monotonicDeadlineNs(timeout_milliseconds); }

    static size_t remainingMs(UInt64 deadline_ns) noexcept { return millisecondsUntil(deadline_ns); }

    int stdout_fd;
    int stderr_fd;
    size_t timeout_milliseconds;
    /// Zero when no frame deadline is armed.
    UInt64 frame_deadline_ns = 0;
    ExternalCommandStderrReaction stderr_reaction;
    UDFProcessSubtreeSampler * sampler;
    bool final_sample_taken = false;
    bool stdout_is_done = false;

    static constexpr size_t BUFFER_SIZE = 4_KiB;
    static constexpr size_t MAX_STDERR_SIZE = 1_MiB;  /// Safety limit for stderr accumulation
    /// A discarded worker's leftover stderr goes into a log line, so keep it to a readable size.
    static constexpr size_t MAX_PENDING_STDERR_SIZE = 4_KiB;
    pollfd pfds[2]{};
    static constexpr size_t num_pfds = 2;
    /// Set once stderr has reached EOF or failed for good; `pfds[1]` is then out of the poll set.
    bool stderr_is_done = false;
    std::unique_ptr<char[]> stderr_read_buf;
    boost::circular_buffer_space_optimized<char> stderr_result_buf{BUFFER_SIZE};
    std::optional<String> stderr_full_output;  /// For THROW mode: accumulate stderr up to MAX_STDERR_SIZE
};

class TimeoutWriteBufferFromFileDescriptor : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit TimeoutWriteBufferFromFileDescriptor(
        int fd_, size_t timeout_milliseconds_, UDFProcessSubtreeSampler * sampler_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<WriteBuffer>(buffer_size), fd(fd_), timeout_milliseconds(timeout_milliseconds_), sampler(sampler_)
    {
        makeFdNonBlocking(fd);
    }

    void nextImpl() override
    {
        if (!offset())
            return;

        size_t bytes_written = 0;

        while (bytes_written != offset())
        {
            if (!pollFd(fd, timeout_milliseconds, POLLOUT))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe write timeout exceeded {} milliseconds", timeout_milliseconds);

            ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

            if ((-1 == res || 0 == res) && errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write into pipe");

            if (res > 0)
            {
                bytes_written += res;
                if (sampler)
                {
                    sampler->recordInputBytes(static_cast<size_t>(res));
                    /// The child's stdin is still open (this write succeeded), so it was
                    /// running; sample its subtree VmHWM. It may exit before we sample — a
                    /// harmless no-op. Also a no-op on the pool path (executable_root_pid <= 0).
                    sampler->sampleExecutablePeak();
                }
            }
        }
    }

    /// Restore blocking mode before the command is returned to the process pool.
    /// Safe only while the fd is provably open (the send-data task calls this right
    /// before closing/returning); the destructor must not do it, see
    /// ~TimeoutReadBufferFromFileDescriptor.
    void reset() const
    {
        makeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
    UDFProcessSubtreeSampler * sampler;
};

}
