#include <Processors/Sources/TimeoutPipeBuffers.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>

#include <ranges>
#include <span>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_POLL;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

bool tryMakeFdNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;
    /// A pooled worker's pipes keep the mode across borrows: no second syscall for each of them.
    if ((flags & O_NONBLOCK) != 0)
        return true;
    if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK))
        return false;

    return true;
}

void makeFdNonBlocking(int fd)
{
    bool result = tryMakeFdNonBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set non-blocking mode of pipe");
}

bool tryMakeFdBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;

    if ((flags & O_NONBLOCK) == 0)
        return true;
    if (-1 == fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)))
        return false;

    return true;
}

void makeFdBlocking(int fd)
{
    bool result = tryMakeFdBlocking(fd);
    if (!result)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set blocking mode of pipe");
}

int pollWithTimeout(pollfd * pfds, size_t num, size_t timeout_milliseconds)
{
    /// Once: it is called several times per query, and obtaining a logger takes a global lock.
    static LoggerPtr logger = getLogger("TimeoutReadBufferFromFileDescriptor");
    /// A negative descriptor is one `poll` skips (a closed stderr); `fcntl` on it would only fail.
    auto describe_fd = [](const auto & pollfd)
    {
        if (pollfd.fd < 0)
            return fmt::format("(fd={})", pollfd.fd);
        return fmt::format("(fd={}, flags={})", pollfd.fd, fcntl(pollfd.fd, F_GETFL));
    };

    int res = 0;

    /// Account against one anchor in microseconds: the per-iteration millisecond stopwatch this
    /// replaces truncated a sub-millisecond interruption to 0, so a signal arriving faster than once
    /// per millisecond - the query profiler under load - left the budget untouched and the poll never
    /// expired. Same accounting as `ReadBufferFromFileDescriptor::poll` and `Epoll::getManyReady`.
    /// Clamp before scaling: `timeout_milliseconds` comes from the unrestricted `command_read_timeout` /
    /// `command_write_timeout` settings, so a huge value would wrap in the multiplication and could then
    /// round a non-zero remainder down to zero.
    const UInt64 timeout_microseconds
        = std::min<UInt64>(timeout_milliseconds, std::numeric_limits<UInt64>::max() / 1000) * 1000;
    UInt64 remaining_microseconds = timeout_microseconds;
    Stopwatch watch;

    while (true)
    {
        LOG_TEST(logger, "Polling descriptors: {}", fmt::join(std::span(pfds, pfds + num) | std::views::transform(describe_fd), ", "));

        res = poll(
            pfds,
            static_cast<nfds_t>(num),
            static_cast<int>(std::min<UInt64>(
                (remaining_microseconds + 999) / 1000, static_cast<UInt64>(std::numeric_limits<int>::max()))));

        if (res < 0)
        {
            if (errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot poll");

            /// A zero timeout is a non-blocking readiness probe, so there is no deadline to exhaust:
            /// retry it rather than letting a signal report the descriptor as not ready.
            if (timeout_microseconds == 0)
                continue;

            const UInt64 elapsed_microseconds = watch.elapsedMicroseconds();
            if (elapsed_microseconds >= timeout_microseconds)
            {
                LOG_TEST(logger, "Timeout exceeded: elapsed={}us, timeout={}us", elapsed_microseconds, timeout_microseconds);
                res = 0;
                break;
            }
            remaining_microseconds = timeout_microseconds - elapsed_microseconds;
        }
        else
        {
            break;
        }
    }

    LOG_TEST(
        logger,
        "Poll for descriptors: {} returned {}",
        fmt::join(std::span(pfds, pfds + num) | std::views::transform(describe_fd), ", "),
        res);

    return res;
}

String readWhatThePipeHolds(int fd, size_t max_size)
{
    String result;
    if (fd < 0)
        return result;

    int available = 0;
    if (0 != ::ioctl(fd, FIONREAD, &available))
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot query the pipe {} for buffered bytes", fd);

    result.resize(std::min(static_cast<size_t>(std::max(available, 0)), max_size));
    size_t done = 0;
    while (done < result.size())
    {
        const ssize_t res = ::read(fd, result.data() + done, result.size() - done);
        if (res > 0)
            done += static_cast<size_t>(res);
        else if (res == -1 && errno == EINTR)
            continue;
        else
            break;
    }
    result.resize(done);
    return result;
}

bool pollFd(int fd, size_t timeout_milliseconds, int events)
{
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = static_cast<int16_t>(events);
    pfd.revents = 0;

    return pollWithTimeout(&pfd, 1, timeout_milliseconds) > 0;
}

}
