"""
Synchronisation primitives and helper functions.
"""
import asyncio
import concurrent.futures
import enum
import threading
import time
from typing import Optional, Union, Any, TYPE_CHECKING


if TYPE_CHECKING:
    asyncio_Future = asyncio.Future[Any]
else:
    asyncio_Future = asyncio.Future

Flag = Union[asyncio_Future, asyncio.Event, concurrent.futures.Future, threading.Event]


async def wait_flag(
        flag: Optional[Flag],
) -> Any:
    """
    Wait for a flag to be raised.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    if flag is None:
        pass
    elif isinstance(flag, asyncio.Future):
        return await flag
    elif isinstance(flag, asyncio.Event):
        return await flag.wait()
    elif isinstance(flag, concurrent.futures.Future):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, flag.result)
    elif isinstance(flag, threading.Event):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, flag.wait)
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")


async def raise_flag(
        flag: Optional[Flag],
) -> None:
    """
    Raise a flag.

    Non-asyncio primitives are generally not our worry,
    but we support them for convenience.
    """
    if flag is None:
        pass
    elif isinstance(flag, asyncio.Future):
        flag.set_result(None)
    elif isinstance(flag, asyncio.Event):
        flag.set()
    elif isinstance(flag, concurrent.futures.Future):
        flag.set_result(None)
    elif isinstance(flag, threading.Event):
        flag.set()
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")


class Toggle:
    """
    An synchronisation primitive that can be awaited both until set or cleared.

    For one-directional toggles, `asyncio.Event` is sufficient.
    But these events cannot be awaited until cleared.

    The bi-directional toggles are needed in some places in the code, such as
    in the population/depletion of a `Vault`, or as an operator's freeze-mode.
    """

    def __init__(
            self,
            __val: bool = False,
    ) -> None:
        super().__init__()
        self._condition = asyncio.Condition()
        self._state: bool = bool(__val)

    def __bool__(self) -> bool:
        """
        In the boolean context, a toggle evaluates to its current on/off state.

        An equivalent of `.is_on` / `.is_off`.
        """
        return bool(self._state)

    def is_on(self) -> bool:
        """ Check if the toggle is currently on (opposite of `.is_off`). """
        return bool(self._state)

    def is_off(self) -> bool:
        """ Check if the toggle is currently off (opposite of `.is_on`). """
        return bool(not self._state)

    async def turn_on(self) -> None:
        """ Turn the toggle on, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = True
            self._condition.notify_all()

    async def turn_off(self) -> None:
        """ Turn the toggle off, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = False
            self._condition.notify_all()

    async def wait_for_on(self) -> None:
        """ Wait until the toggle is turned on (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: self._state)

    async def wait_for_off(self) -> None:
        """ Wait until the toggle is turned off (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: not self._state)


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
    NONE = 0
    DONE = enum.auto()  # whatever the reason and the status, the asyncio task has exited.
    FILTERS_MISMATCH = enum.auto()  # the resource does not match the filters anymore.
    RESOURCE_DELETED = enum.auto()  # the resource was deleted, the asyncio task is still awaited.
    OPERATOR_EXITING = enum.auto()  # the operator is exiting, the asyncio task is still awaited.
    DAEMON_SIGNALLED = enum.auto()  # the stopper flag was set, the asyncio task is still awaited.
    DAEMON_CANCELLED = enum.auto()  # the asyncio task was cancelled, the thread can be running.
    DAEMON_ABANDONED = enum.auto()  # we gave up on the asyncio task, the thread can be running.


class DaemonStopper:
    """
    A boolean flag indicating that the daemon should stop and exit.

    Every daemon gets a ``stopper`` kwarg, which is an event-like object.
    The stopper is raised in two cases:

    * The corresponding k8s object is deleted, so the daemon should stop.
    * The whole operator is stopping, so all the daemons should stop too.

    The stopper flag is a graceful way of a daemon termination.
    If the daemons do not react to their stoppers, and continue running,
    their tasks are cancelled by raising a `asyncio.CancelledError`.

    .. warning::
        In case of synchronous handlers, which are executed in the threads,
        this can lead to the OS resource leakage:
        there is no way to kill a thread in Python, so it will continue running
        forever or until failed (e.g. on an API call for an absent resource).
        The orphan threads will block the operator's process from exiting,
        thus affecting the speed of restarts.
    """

    def __init__(self) -> None:
        super().__init__()
        self.when: Optional[float] = None
        self.reason = DaemonStoppingReason.NONE
        self.sync_checker = SyncDaemonStopperChecker(self)
        self.async_checker = AsyncDaemonStopperChecker(self)
        self.sync_event = threading.Event()
        self.async_event = asyncio.Event()

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.is_set()}, reason={self.reason}>'

    def is_set(self, reason: Optional[DaemonStoppingReason] = None) -> bool:
        """
        Check if the daemon stopper is set: at all or for specific reason.
        """
        return ((reason is None or reason in self.reason) and self.sync_event.is_set())

    def set(self, *, reason: DaemonStoppingReason) -> None:
        self.when = self.when if self.when is not None else time.monotonic()
        self.reason |= reason
        self.sync_event.set()
        self.async_event.set()  # it is thread-safe: always called in operator's event loop.


class DaemonStopperChecker:

    """
    A minimalistic read-only checker for the daemons from the user side.

    This object is fed into the :kwarg:`stopped` kwarg for the handlers.

    The actual stopper is hidden from the users, and is an internal class.
    The users should not be able to trigger the stopping activities or
    check the reasons of stopping (or know about them at all).

    Usage::

        @kopf.daemon('zalando.org', 'v1', 'kopfexamples')
        def handler(stopped, **kwargs):
            while not stopped:
                ...
                stopped.wait(60)
    """

    def __init__(self, stopper: DaemonStopper) -> None:
        super().__init__()
        self._stopper = stopper

    def __repr__(self) -> str:
        return repr(self._stopper)

    def __bool__(self) -> bool:
        return self._stopper.is_set()

    def is_set(self) -> bool:
        return self._stopper.is_set()

    @property
    def reason(self) -> DaemonStoppingReason:
        return self._stopper.reason


class SyncDaemonStopperChecker(DaemonStopperChecker):
    def wait(self, timeout: Optional[float] = None) -> bool:
        self._stopper.sync_event.wait(timeout=timeout)
        return bool(self)


class AsyncDaemonStopperChecker(DaemonStopperChecker):
    async def wait(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._stopper.async_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return bool(self)
