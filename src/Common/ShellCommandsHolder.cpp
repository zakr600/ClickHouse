#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ShellCommandsHolder.h>

#include <Common/waitForPid.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace DB
{

ShellCommandsHolder & ShellCommandsHolder::instance()
{
    static ShellCommandsHolder instance;
    return instance;
}

void ShellCommandsHolder::removeCommand(pid_t pid)
{
    std::lock_guard lock(mutex);
    bool is_erased = shell_commands.erase(pid);
    LOG_TRACE(log, "Try to erase command with the pid {}, is_erased: {}", pid, is_erased);
}

void ShellCommandsHolder::addCommand(std::unique_ptr<ShellCommand> command)
{
    std::lock_guard lock(mutex);
    pid_t command_pid = command->getPid();
    if (command->waitIfProccesTerminated())
    {
        LOG_TRACE(log, "Pid {} already finished. Do not insert it.", command_pid);
        return;
    }

    auto [iterator, is_inserted] = shell_commands.try_emplace(command_pid, std::move(command));
    if (is_inserted)
    {
        LOG_TRACE(log, "Inserted the command with pid {}", command_pid);
        return;
    }

    if (iterator->second->isWaitCalled())
    {
        iterator->second = std::move(command);
        LOG_TRACE(log, "Replaced the command with pid {}", command_pid);
        return;
    }
    /// We got two active ShellCommand with the same pid.
    /// Probably it is a bug, will try to replace the old shell command with a new one.
    chassert(false);

    LOG_WARNING(log, "The PID already presented in active shell commands, will try to replace with a new one.");

    iterator->second->setDoNotTerminate();
    iterator->second = std::move(command);
}

void ShellCommandsHolder::addSignalledChild(pid_t pid)
{
    std::lock_guard lock(mutex);
    signalled_children.insert(pid);

    /// One that is gone already needs no thread to wait for it.
    reapSignalledChildren();
    if (signalled_children.empty())
        return;

    if (!reaper)
    {
        reaper_wakeup.open();
        reaper_wakeup.setNonBlockingReadWrite();
        reaper.emplace([this] { runReaper(); });
        return;
    }

    /// The reaper is asleep on the children it knew of: woken to wait for this one too. A full pipe
    /// already holds a wake-up, which is all this is.
    char byte = 0;
    [[maybe_unused]] ssize_t res = ::write(reaper_wakeup.fds_rw[1], &byte, 1);
}

ShellCommandsHolder::~ShellCommandsHolder()
{
    std::optional<std::thread> thread;
    {
        std::lock_guard lock(mutex);
        reaper_shutdown = true;
        if (reaper)
        {
            char byte = 0;
            [[maybe_unused]] ssize_t res = ::write(reaper_wakeup.fds_rw[1], &byte, 1);
        }
        thread = std::move(reaper);
    }
    if (thread)
        thread->join();
}

void ShellCommandsHolder::reapSignalledChildren()
{
    for (auto it = signalled_children.begin(); it != signalled_children.end();)
    {
        int res = 0;
        do
            res = ::waitpid(*it, nullptr, WNOHANG);
        while (res < 0 && errno == EINTR);

        /// Still running, or (an error other than `ECHILD`) not known to be gone: tried again later.
        if (res == 0 || (res < 0 && errno != ECHILD))
        {
            ++it;
            continue;
        }

        LOG_TRACE(log, "Reaped the signalled command with the pid {}", *it);
        it = signalled_children.erase(it);
    }
}

void ShellCommandsHolder::runReaper()
{
    /// Without a `pidfd` for some child (an older kernel, a seccomp profile that refuses it) there is
    /// nothing to sleep on until it exits: it is looked at again in steps of this.
    static constexpr int step_without_pidfd_ms = 100;

    std::vector<pollfd> fds;
    std::vector<int> pidfds;
    while (true)
    {
        try
        {
            int timeout_ms = -1;
            {
                std::lock_guard lock(mutex);
                if (reaper_shutdown)
                    return;

                reapSignalledChildren();

                fds.clear();
                fds.push_back({.fd = reaper_wakeup.fds_rw[0], .events = POLLIN, .revents = 0});
                for (pid_t pid : signalled_children)
                {
                    int pidfd = -1;
#if defined(OS_LINUX)
                    pidfd = openPidFdForWaiting(pid);
#endif
                    if (pidfd < 0)
                    {
                        timeout_ms = step_without_pidfd_ms;
                        continue;
                    }
                    pidfds.push_back(pidfd);
                    fds.push_back({.fd = pidfd, .events = POLLIN, .revents = 0});
                }
            }
            SCOPE_EXIT({
                for (int pidfd : pidfds)
                    ::close(pidfd);
                pidfds.clear();
            });

            if (::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeout_ms) < 0 && errno != EINTR)
                LOG_WARNING(log, "Cannot wait for signalled commands to exit: {}", errnoToString());

            char buffer[64];
            while (::read(reaper_wakeup.fds_rw[0], buffer, sizeof(buffer)) > 0)
            {
            }
        }
        catch (...)
        {
            /// The thread must not end on an exception; whatever it was, the next round tries again.
            tryLogCurrentException(log);
        }
    }
}
}
