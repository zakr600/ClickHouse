#include <Common/waitForPid.h>
#include <Common/VersionNumber.h>
#include <Poco/Environment.h>
#include <Common/Stopwatch.h>
/// for abortOnFailedAssertion() via chassert() (dependency chain looks odd)
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <fcntl.h>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-statement-expression"
#define HANDLE_EINTR(x) ({ \
    decltype(x) eintr_wrapper_result; \
    do { \
        eintr_wrapper_result = (x); \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result; \
})

namespace DB
{

enum PollPidResult
{
    RESTART,
    TIMED_OUT,
    FAILED
};

}

#if defined(OS_LINUX)

#include <poll.h>

#if !defined(__NR_pidfd_open)
    #if defined(__x86_64__)
        #define SYS_pidfd_open 434
    #elif defined(__aarch64__)
        #define SYS_pidfd_open 434
    #elif defined(__powerpc64__)
        #define SYS_pidfd_open 434
    #elif defined(__riscv)
        #define SYS_pidfd_open 434
    #elif defined(__s390x__)
        #define SYS_pidfd_open 434
    #elif defined(__loongarch64)
        #define SYS_pidfd_open 434
    #elif defined(__e2k__)
        #define SYS_pidfd_open 206
    #else
        #error "Unsupported architecture"
    #endif
#else
    #define SYS_pidfd_open __NR_pidfd_open
#endif

#if !defined(__NR_pidfd_send_signal)
    #define SYS_pidfd_send_signal 424
#else
    #define SYS_pidfd_send_signal __NR_pidfd_send_signal
#endif

namespace DB
{

int syscall_pidfd_open(pid_t pid)
{
    return static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
}

int syscall_pidfd_send_signal(int pidfd, int sig)
{
    return static_cast<int>(syscall(SYS_pidfd_send_signal, pidfd, sig, nullptr, 0));
}

static bool supportsPidFdOpen()
{
    VersionNumber pidfd_open_minimal_version(5, 3, 0);
    VersionNumber linux_version(Poco::Environment::osVersion());
    return linux_version >= pidfd_open_minimal_version;
}

/// Without a `pidfd` there is nothing to wait on: a `/proc/<pid>` directory is always ready for
/// `poll`, so polling it waits for nothing, and whether the process is there at all the caller has
/// just found out (`checkPidExited`). Wait a short step, and let it look again.
static PollPidResult waitStepWithoutPidFd(int timeout_in_ms)
{
    static constexpr int no_pidfd_step_ms = 5;
    if (poll(nullptr, 0, std::min(timeout_in_ms, no_pidfd_step_ms)) < 0 && errno != EINTR)
        return PollPidResult::FAILED;
    return PollPidResult::RESTART;
}

static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    if (!supportsPidFdOpen())
        return waitStepWithoutPidFd(timeout_in_ms);

    // pidfd_open cannot be interrupted, no EINTR handling
    int pid_fd = syscall_pidfd_open(pid);

    if (pid_fd < 0)
    {
        if (errno == ESRCH)
            return PollPidResult::RESTART;

        /// Refused rather than failed: a seccomp profile (older container runtimes deny
        /// `pidfd_open`) or a kernel built without it. The process is there all the same, and
        /// it is waited for as on a kernel that has no `pidfd` at all.
        if (errno == EPERM || errno == EACCES || errno == ENOSYS)
            return waitStepWithoutPidFd(timeout_in_ms);

        return PollPidResult::FAILED;
    }

    /// Releases pid_fd on every return path, including poll timeout and error.
    SCOPE_EXIT(
    {
        [[maybe_unused]] int err = close(pid_fd);
        chassert(!err || errno == EINTR);
    });

    struct pollfd pollfd{};
    pollfd.fd = pid_fd;
    pollfd.events = POLLIN;

    /// A signal interrupting `poll` must not restart with the full timeout; returning
    /// RESTART lets `waitForPid` re-evaluate the deadline-bounded remaining budget.
    int ready = poll(&pollfd, 1, timeout_in_ms);

    if (ready < 0 && errno == EINTR)
        return PollPidResult::RESTART;

    if (ready == 0)
        return PollPidResult::TIMED_OUT;
    if (ready < 0)
        return PollPidResult::FAILED;

    return PollPidResult::RESTART;
}

#elif defined(OS_DARWIN) || defined(OS_FREEBSD)

#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <sys/event.h>
#include <err.h>

namespace DB
{

static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    int kq = kqueue();
    if (kq == -1)
        return PollPidResult::FAILED;

    /// Releases the kqueue fd on every return path, including early filter-add errors.
    SCOPE_EXIT({ close(kq); });

    struct kevent change{};
    change.ident = 0;

    EV_SET(&change, pid, EVFILT_PROC, EV_ADD, NOTE_EXIT, 0, NULL);

    int event_add_result = HANDLE_EINTR(kevent(kq, &change, 1, NULL, 0, NULL));
    if (event_add_result == -1)
    {
        if (errno == ESRCH)
            return PollPidResult::RESTART;

        return PollPidResult::FAILED;
    }

    struct kevent event{};
    event.ident = 0;

    struct timespec remaining_timespec = {.tv_sec = timeout_in_ms / 1000, .tv_nsec = (timeout_in_ms % 1000) * 1000000};

    /// A signal interrupting `kevent` must not restart with the full timeout; returning
    /// RESTART lets `waitForPid` re-evaluate the deadline-bounded remaining budget.
    int ret = kevent(kq, nullptr, 0, &event, 1, &remaining_timespec);

    if (ret < 0 && errno == EINTR)
        return PollPidResult::RESTART;

    if (ret == 0)
        return PollPidResult::TIMED_OUT;
    if (ret < 0)
        return PollPidResult::FAILED;

    return PollPidResult::RESTART;
}
#elif defined(OS_SUNOS)

#include <libproc.h>

namespace DB
{

/// Grab the process, wait for it to change state, and check whether it's
/// terminated.
static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    PollPidResult result = PollPidResult::TIMED_OUT;
    int rc, perr;
    struct ps_prochandle *hdl;

    hdl = Pgrab(pid, PGRAB_RETAIN | PGRAB_FORCE | PGRAB_NOSTOP, &perr);
    if (hdl == NULL)
    {
        if (perr == G_NOPROC)
            return PollPidResult::RESTART;
        return PollPidResult::FAILED;
    }

    rc = Pstopstatus(hdl, PCWSTOP, timeout_in_ms);
    if (rc < 0)
        result = errno == ENOENT ? PollPidResult::RESTART : PollPidResult::FAILED;
    if (rc == 0)
    {
        int state = Pstate(hdl);
        if (state == PS_DEAD || state == PS_UNDEAD)
            result = PollPidResult::RESTART;
    }

    Pfree(hdl);
    return result;
}
#elif defined(OS_WASM)

namespace DB
{

/// WebAssembly has no child processes: there is no `fork` and no `exec`, so nothing to wait for.
static PollPidResult pollPid(pid_t /*pid*/, int /*timeout_in_ms*/)
{
    return PollPidResult::FAILED;
}
#else
    #error "Unsupported OS type"
#endif

ChildState peekChildState(pid_t pid, bool blocking)
{
#if defined(OS_WASM)
    (void)pid;
    (void)blocking;
    return ChildState::NOT_OUR_CHILD;
#else
    siginfo_t info{};
    int res = HANDLE_EINTR(waitid(P_PID, static_cast<id_t>(pid), &info, WEXITED | WNOWAIT | (blocking ? 0 : WNOHANG)));
    if (res != 0)
        return ChildState::NOT_OUR_CHILD;
    /// With `WNOHANG` and a child that has not exited, `waitid` succeeds and leaves `info` zeroed.
    /// glibc defines `si_pid` as a macro that names itself, which `-Wdisabled-macro-expansion` reports.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    const pid_t exited_pid = info.si_pid;
#pragma clang diagnostic pop
    return exited_pid == pid ? ChildState::EXITED : ChildState::RUNNING;
#endif
}

/// 1 if `pid` has exited (and, unless `leave_unreaped`, is reaped), 0 if it is running, -1 on error.
static int checkPidExited(pid_t pid, bool leave_unreaped)
{
    if (leave_unreaped)
    {
        switch (peekChildState(pid, /*blocking=*/ false))
        {
            case ChildState::EXITED: return 1;
            case ChildState::RUNNING: return 0;
            case ChildState::NOT_OUR_CHILD: return -1;
        }
    }

    int status = 0;
    int waitpid_res = HANDLE_EINTR(waitpid(pid, &status, WNOHANG));
    if (waitpid_res == pid)
        return 1;
    return waitpid_res == 0 ? 0 : -1;
}

bool waitForPid(pid_t pid, size_t timeout_in_seconds, bool leave_unreaped)
{
    return waitForPidMilliseconds(pid, timeout_in_seconds * 1000, leave_unreaped) == WaitForPidResult::EXITED;
}

WaitForPidResult waitForPidMilliseconds(pid_t pid, size_t timeout_in_milliseconds, bool leave_unreaped)
{
    Stopwatch watch;

    if (timeout_in_milliseconds == 0)
    {
        /// If there is no timeout before signal try to waitpid 1 time without block so we can avoid sending
        /// signal if process is already normally terminated.
        const int exited = checkPidExited(pid, leave_unreaped);
        return exited == 1 ? WaitForPidResult::EXITED : (exited == 0 ? WaitForPidResult::TIMEOUT : WaitForPidResult::ERROR);
    }

    /// If timeout is positive, poll until the process exits or the total wall
    /// clock since function entry exceeds the limit. The remaining budget is
    /// derived from the `watch` started at function entry (never reset), so
    /// that a `pollPid` that returns early - a signal, or the short steps it
    /// takes without a `pidfd` - still subtracts real elapsed time.

    const Int64 total_timeout_ms = static_cast<Int64>(timeout_in_milliseconds);
    while (true)
    {
        int exited = checkPidExited(pid, leave_unreaped);
        if (exited == 1)
            return WaitForPidResult::EXITED;

        if (exited != 0)
            return WaitForPidResult::ERROR;

        const Int64 remaining_ms = total_timeout_ms - static_cast<Int64>(watch.elapsedMilliseconds());
        if (remaining_ms <= 0)
            return WaitForPidResult::TIMEOUT;

        PollPidResult result = pollPid(pid, static_cast<int>(remaining_ms));
        if (result == PollPidResult::FAILED)
            return WaitForPidResult::ERROR;
        if (result == PollPidResult::TIMED_OUT)
            return WaitForPidResult::TIMEOUT;
    }
}

}
#pragma clang diagnostic pop
