#pragma once
#include <sys/types.h>

namespace DB
{
enum class WaitForPidResult
{
    EXITED,
    TIMEOUT,
    ERROR,
};

/// Waits up to `timeout_in_milliseconds` for the child `pid` to exit, reaping it. Distinguishes an
/// expired deadline from a failed wait, so a caller cannot retry errors in a busy loop. With
/// `leave_unreaped`, a child that has exited is left a zombie (`waitid` with `WNOWAIT`), so its pid -
/// and the number of the process group it leads - cannot be reused until it is reaped.
WaitForPidResult waitForPidMilliseconds(pid_t pid, size_t timeout_in_milliseconds, bool leave_unreaped = false);

enum class ChildState
{
    RUNNING,
    EXITED,
    /// `waitid` fails (`ECHILD`): the pid was reaped already or is not a child of this process,
    /// so it may belong to anybody now.
    NOT_OUR_CHILD,
};

/// The state of the child `pid`, without reaping it. With `blocking`, waits for it to exit.
ChildState peekChildState(pid_t pid, bool blocking);

#if defined(OS_LINUX)
int syscall_pidfd_open(pid_t pid);
int syscall_pidfd_send_signal(int pidfd, int sig);

/// `pidfd_open` for a wait on the child `pid`: the descriptor, or -1 with `errno` set. A refusal
/// (`EPERM`, `EACCES`, `ENOSYS` - a seccomp profile) is not an error for a wait, which then polls in
/// short steps, but it is logged, once per process, so that the polling has a stated reason.
int openPidFdForWaiting(pid_t pid);
#endif

}
