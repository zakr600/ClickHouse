#pragma once
#include <sys/types.h>

namespace DB
{
/*
 * Waits for a specific pid with timeout
 * Returns `true` if process terminated successfully in specified timeout or `false` otherwise
 * With `leave_unreaped`, a child that has exited is left a zombie (`waitid` with `WNOWAIT`), so
 * its pid - and the number of the process group it leads - cannot be reused until it is reaped.
 */
bool waitForPid(pid_t pid, size_t timeout_in_seconds, bool leave_unreaped = false);

/// The same, with the timeout in milliseconds.
bool waitForPidMilliseconds(pid_t pid, size_t timeout_in_milliseconds, bool leave_unreaped = false);

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
#endif

}
