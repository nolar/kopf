import enum

from kopf._cogs.aiokits import aioenums


class DaemonStoppingReason(enum.Flag):
    """
    A reason or reasons of daemon being terminated.

    Daemons are signalled to exit usually for two reasons: the operator itself
    is exiting or restarting, so all daemons of all resources must stop;
    or the individual resource was deleted, but the operator continues running.

    No matter the reason, the daemons must exit, so one and only one stop-flag
    is used. Some daemons can check the reason of exiting if it is important.

    There can be multiple reasons combined (in rare cases, all of them).
    """
    DONE = enum.auto()  # whatever the reason and the status, the asyncio task has exited.
    FILTERS_MISMATCH = enum.auto()  # the resource does not match the filters anymore.
    RESOURCE_DELETED = enum.auto()  # the resource was deleted, the asyncio task is still awaited.
    OPERATOR_PAUSING = enum.auto()  # the operator is pausing, the asyncio task is still awaited.
    OPERATOR_EXITING = enum.auto()  # the operator is exiting, the asyncio task is still awaited.
    DAEMON_SIGNALLED = enum.auto()  # the stopper flag was set, the asyncio task is still awaited.
    DAEMON_CANCELLED = enum.auto()  # the asyncio task was cancelled, the thread can be running.
    DAEMON_ABANDONED = enum.auto()  # we gave up on the asyncio task, the thread can be running.


DaemonStopper = aioenums.FlagSetter[DaemonStoppingReason]
DaemonStopped = aioenums.FlagWaiter[DaemonStoppingReason]
SyncDaemonStopperChecker = aioenums.SyncFlagWaiter[DaemonStoppingReason]  # deprecated
AsyncDaemonStopperChecker = aioenums.AsyncFlagWaiter[DaemonStoppingReason]  # deprecated
