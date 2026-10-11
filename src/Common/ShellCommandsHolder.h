#pragma once

#include <Common/Logger.h>
#include <Common/PipeFDs.h>
#include <Common/ShellCommand.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

/** The holder class for running background shell processes.
*/
class ShellCommandsHolder final : public boost::noncopyable
{
public:
    static ShellCommandsHolder & instance();

    /// Called on `SIGCHLD` for `pid`.
    void removeCommand(pid_t pid);
    void addCommand(std::unique_ptr<ShellCommand> command);

    /// A child that `~ShellCommand` signalled to terminate and nobody waits for any more: reaped as
    /// soon as it exits, by a thread of this holder, so that it does not stay a zombie for as long as
    /// the process runs - and without whoever signalled it blocking until it exits.
    void addSignalledChild(pid_t pid);

    ~ShellCommandsHolder();

private:
    /// Reaps every signalled child that has exited.
    void reapSignalledChildren() TSA_REQUIRES(mutex);

    /// The reaper thread: sleeps until a signalled child exits (on its `pidfd`) or another one is
    /// added (`reaper_wakeup`), and reaps.
    void runReaper();

    using ShellCommands = std::unordered_map<pid_t, std::unique_ptr<ShellCommand>>;

    std::mutex mutex;
    ShellCommands shell_commands TSA_GUARDED_BY(mutex);
    std::unordered_set<pid_t> signalled_children TSA_GUARDED_BY(mutex);
    std::optional<std::thread> reaper TSA_GUARDED_BY(mutex);
    /// Opened under `mutex` before the reaper starts and never changed after, so the reaper reads it
    /// without the lock.
    LazyPipeFDs reaper_wakeup;
    bool reaper_shutdown TSA_GUARDED_BY(mutex) = false;

    LoggerPtr log = getLogger("ShellCommandsHolder");
};

}
