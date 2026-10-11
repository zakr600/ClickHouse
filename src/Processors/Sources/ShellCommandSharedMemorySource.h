#pragma once

#include <Processors/Sources/ShellCommandHolder.h>
#include <Processors/Sources/ShellCommandSource.h>

namespace DB
{

/// The source of the shared-memory transport of `ShellCommandSourceCoordinator`: the command's input
/// and output go through a region shared with its process, and its pipes carry only the control
/// frames. Creates the region and, through `build_command`, the process - in that order, since the
/// process inherits the region's descriptor at `exec` - or reuses those of the pooled
/// `command_holder`. One input pipe.
Pipe createShellCommandSharedMemoryPipe(
    ContextPtr context,
    const ShellCommandSourceCoordinator::Configuration & coordinator_configuration,
    SharedHeader sample_block,
    ShellCommandHolder::ShellCommandBuilderFunc build_command,
    Pipe input_pipe,
    const ShellCommandSourceConfiguration & source_configuration,
    ShellCommandHolderPtr command_holder,
    std::shared_ptr<ProcessPool> process_pool);

}
