#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/SharedMemoryRegion.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Processors/Sources/ShellCommandSource.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric ExecutableUDFSharedMemoryPooledBytes;
    extern const Metric MemoryTrackingUnmeasured;
}

namespace DB
{

/// The descriptor number under which the command's process inherits the shared-memory region.
/// Chosen above the three standard streams, and the shared-memory transport admits no extra pipes
/// (it takes exactly one input), so nothing else in the child is numbered here.
inline constexpr int SHARED_MEMORY_CHILD_FD = 3;

class ShellCommandHolder
{
public:
    /// Builds the process. It is given the descriptors the child has to inherit - the shared-memory
    /// region, as `{child_fd, parent_fd}` - which is why the region has to exist before the
    /// process does: a `memfd` has no name a process could open later, so the only way for the
    /// command to reach it is to have been started with it.
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>(const std::vector<std::pair<int, int>> & inherited_fds)>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    ~ShellCommandHolder()
    {
        /// The idle worker goes first, before the region and its charge: it holds a descriptor to
        /// the region, so the pages stay resident for as long as it lives, and a member is only
        /// destroyed after this body - which would drop the charge while the worker still holds
        /// the pages, for the whole wait `~ShellCommand` starts with. Its stdin is closed first,
        /// so that a worker written to exit on EOF does so at once rather than sitting out
        /// `command_termination_timeout` blocked on its next request. Past that budget a
        /// shared-memory worker's process group is sent `SIGKILL` and the worker reaped
        /// (`ShellCommand::Config::own_process_group`), so nothing in it can write into the region
        /// once the charge below is gone.
        if (returned_command)
        {
            /// `closeInputs` flushes and closes the pipes and throws on failure; this destructor
            /// must not, and it still has to release the process, the region and its charge.
            try
            {
                returned_command->closeInputs();
            }
            catch (...)
            {
                tryLogCurrentException("ShellCommandHolder");
            }
            returned_command.reset();
        }

        shared_memory.reset();

        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);
    }

    /// Whether the next `buildCommand` hands back a process that has already served a borrow. A
    /// caller that has to distinguish "this worker may have left something on its pipes" from "this
    /// process was started a moment ago" needs to ask before building.
    bool hasReturnedCommand() const { return returned_command != nullptr; }

    /// Hands back the process that served the previous borrow, or starts a new one. A new one
    /// inherits the region this holder owns at that moment, so it has to have been created
    /// already (see `getOrCreateSharedMemory`); a returned one inherited it when it was started.
    std::unique_ptr<ShellCommand> buildCommand()
    {
        if (returned_command)
            return std::move(returned_command);

        return func(inheritedRegionFds());
    }

    /// The descriptors a process started now has to inherit: the region under child descriptor
    /// `SHARED_MEMORY_CHILD_FD`, which is also the number its request names it by.
    std::vector<std::pair<int, int>> inheritedRegionFds() const
    {
        std::vector<std::pair<int, int>> fds;
        if (shared_memory)
            fds.emplace_back(SHARED_MEMORY_CHILD_FD, shared_memory->fd());
        return fds;
    }

    void returnCommand(std::unique_ptr<ShellCommand> command)
    {
        returned_command = std::move(command);
    }

    /// The process that served the previous borrow, still held here, or null if the next
    /// `buildCommand` would start a fresh one. For the probes a borrow runs on a reused worker
    /// before it builds anything on it: they read its pipes, and what they decide - keep it, drop
    /// it, or drop it together with its region - has to be decided before the region is taken
    /// over, because taking it over is what a dropped worker's region must not survive.
    ShellCommand * returnedCommand() const { return returned_command.get(); }

    /// Who borrowed this worker last: the user, and the roles the query ran with.
    ///
    /// A pooled region is not cleared between borrows, and a pool serves the queries of every
    /// user: what one query left in the region, the command can read while serving the next.
    /// Over the pipes a command only ever saw what it was sent. The borrower's identity is what
    /// tells "the next query" from "a query of somebody else": a borrow by a different one scrubs
    /// the region (`scrubRegionForBorrower`), a borrow by the same one does not pay for it. The
    /// identity is the user's id and the current roles, not the user's name, for the reasons the
    /// query result cache keys its entries by the same pair (`QueryResultCache::Key`): a user
    /// created under the name of a dropped one is not the dropped one, and the roles of one user
    /// can be tied to different row policies, so what a query saw under one set of roles must not
    /// be readable by the command under another.
    struct BorrowerIdentity
    {
        std::optional<UUID> user_id;
        std::vector<UUID> current_roles;

        bool operator==(const BorrowerIdentity &) const = default;
    };

    const std::optional<BorrowerIdentity> & lastBorrower() const { return last_borrower; }
    void recordBorrower(BorrowerIdentity borrower) { last_borrower = std::move(borrower); }

    /// The shared-memory region for this process, created once and reused across pool borrows.
    ///
    /// Creating and growing the region does not charge any memory tracker here: while the holder is
    /// borrowed, the borrowing query owns the charge (see `releaseChargeToBorrower`).
    SharedMemoryRegionPtr getOrCreateSharedMemory(size_t size, bool & created)
    {
        if (!shared_memory)
        {
            shared_memory = std::make_shared<SharedMemoryRegion>(size);
            created = true;
        }
        else
            created = false;

        return shared_memory;
    }

    /// What the region costs - its footprint as `sharedMemoryRegionOverTheCap` last read it, or the
    /// source's own cap check on the same region - or zero if there is none. Lets the borrower charge
    /// its query memory tracker for the right number of bytes BEFORE the region is created or reused,
    /// because creating one commits its pages, and the server for what a pooled worker holds while
    /// it is idle. The footprint rather than the mapped size: a growth that reserved its pages but
    /// could not map them, a command that extended the file behind the server's back, or one that
    /// committed pages past its end, all leave the region costing more than the mapping shows, and
    /// those pages cost what any others do (`SharedMemoryRegion::refreshFootprint`). Read at every
    /// borrow and every hand-back, so that whatever the command added is charged from then on.
    size_t lastSeenSharedMemorySize() const
    {
        return shared_memory ? shared_memory->footprint() : 0;
    }

    /// The region if it is over the cap - re-read now (`SharedMemoryRegion::isOverTheCap`) - or
    /// null otherwise. What a borrow checks before it builds anything on the worker.
    SharedMemoryRegion * sharedMemoryRegionOverTheCap(size_t max_size) const
    {
        if (!shared_memory)
            return nullptr;
        shared_memory->refreshFootprint();
        if (shared_memory->isOverTheCap(max_size))
            return shared_memory.get();
        return nullptr;
    }

    /// Drops the returned process and its region together, so that the next `buildCommand` starts
    /// a fresh process with a fresh region. A process and its region live and die together - the
    /// process reached it by inheriting its descriptor at `exec` - so there is no dropping
    /// one without the other.
    ///
    /// A borrow can discard the worker before it has taken the region's charge over
    /// (`releaseChargeToBorrower`), while the holder still charges it globally. The charge is
    /// dropped only after both are gone: the region stays resident until the process has been
    /// signalled and reaped, so it has to stay counted until then. Once the borrower has taken the charge over there is nothing left here
    /// to drop, and the borrower's own charge covers the region until it is gone.
    void discardWorkerAndRegion() noexcept
    {
        /// Nobody waits for how a worker that is thrown away exits: it is signalled at once rather
        /// than given `command_termination_timeout`, which whoever drops it would sit out.
        if (returned_command)
            returned_command->discardWithoutGrace();
        returned_command.reset();
        shared_memory.reset();

        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);

        /// The region the next borrow creates is a fresh, zero-filled file with nobody's data in
        /// it: there is no previous borrower to scrub it for, and a `memset` of a region that
        /// is already zero would be a wasted write of its whole size.
        last_borrower.reset();
    }

    /// A region is charged to one memory tracker at a time, chosen by who can observe it:
    /// while the holder is borrowed, the borrowing query's tracker owns the charge, so the memory
    /// limit of that query still covers the region; while the holder sits idle in the process pool
    /// the region stays mapped with no query to charge, so the global tracker owns it instead.
    /// The charge is handed over in both directions rather than taken twice, because a query
    /// charge already propagates up into `total_memory_tracker` — charging both would count the
    /// same bytes twice there and let a handful of pooled workers exhaust
    /// `max_server_memory_usage` on paper. A charge cannot be moved between trackers atomically,
    /// so for the moment between releasing it on one side and taking it on the other the region
    /// is charged to neither (see `cleanup`): the global figure is low by the region's size, and
    /// by the sum of them when several hand-overs coincide. The invariant is about the states
    /// between hand-overs, not inside them.
    ///
    /// With `memory_worker_correct_memory_tracker` on (the default) `MemoryWorker` replaces the
    /// global tracker's value with a measurement on every tick, and no measurement it uses sees the
    /// region's pages (they are `shmem`). So every charge for a region, the query's and the
    /// holder's alike, is also counted in `MemoryTrackingUnmeasured`, which the worker adds to the
    /// measurement: the charge survives the correction and keeps counting towards
    /// `max_server_memory_usage`.
    ///
    /// Called by the borrower right before it charges its query for the region, after it has
    /// inspected the returned worker and discarded it if it had to: a discarded worker takes its
    /// charge with it (`discardWorkerAndRegion`), so the region stays counted while the process
    /// is being destroyed.
    void releaseChargeToBorrower()
    {
        if (persistent_memory_charge)
            unchargePersistentMemory(persistent_memory_charge);
    }

    /// Restore the idle charge from a footprint measured and capped by the borrower before it
    /// released the query charge. The borrower discards the worker if that measurement fails.
    /// This changes accounting for existing memory and must not throw during cleanup.
    void acquireChargeFromBorrower(size_t bytes) noexcept
    {
        /// A borrow that failed before it took the charge over (`releaseChargeToBorrower`) leaves
        /// the holder still charging the region, so the charge is brought to the new figure rather
        /// than added on top of what is there.
        if (bytes == persistent_memory_charge)
            return;

        if (bytes > persistent_memory_charge)
            chargePersistentMemory(bytes - persistent_memory_charge);
        else
            unchargePersistentMemory(persistent_memory_charge - bytes);
    }

private:
    /// Straight into the server-wide tracker, past this thread's: no query owns these bytes. And
    /// without the limit: the charge accounts for pages that are there already, so it cannot be
    /// refused - which is also what lets the hand-over be `noexcept`.
    void chargePersistentMemory(size_t bytes) noexcept
    {
        total_memory_tracker.adjustWithUntrackedMemory(static_cast<Int64>(bytes));
        persistent_memory_charge += bytes;
        CurrentMetrics::add(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));

        /// The same bytes, reported on their own. The server-wide tracker they were just added to
        /// carries everything else the server allocates as well, so it cannot answer how much of it
        /// is regions held by idle pooled workers - which is the part an administrator sizing
        /// `max_server_memory_usage` against a pool has to know, and the only part a test of this
        /// hand-over can assert exactly.
        CurrentMetrics::add(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
    }

    void unchargePersistentMemory(size_t bytes) noexcept
    {
        total_memory_tracker.adjustWithUntrackedMemory(-static_cast<Int64>(bytes));
        persistent_memory_charge -= bytes;
        CurrentMetrics::sub(CurrentMetrics::MemoryTrackingUnmeasured, static_cast<Int64>(bytes));

        CurrentMetrics::sub(CurrentMetrics::ExecutableUDFSharedMemoryPooledBytes, static_cast<Int64>(bytes));
    }

    std::unique_ptr<ShellCommand> returned_command;
    ShellCommandBuilderFunc func;
    SharedMemoryRegionPtr shared_memory;
    std::optional<BorrowerIdentity> last_borrower;
    size_t persistent_memory_charge = 0;
};

}
