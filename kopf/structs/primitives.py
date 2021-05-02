"""
Synchronisation primitives and helper functions.
"""
import asyncio
import collections.abc
import concurrent.futures
import enum
import threading
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Collection, Generator, \
                   Generic, Iterable, Iterator, Optional, Set, TypeVar, Union

from kopf.aiokits import aiotasks

Flag = Union[aiotasks.Future, asyncio.Event, concurrent.futures.Future, threading.Event]


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


def check_flag(
        flag: Optional[Flag],
) -> Optional[bool]:
    """
    Check if a flag is raised.
    """
    if flag is None:
        return None
    elif isinstance(flag, asyncio.Future):
        return flag.done()
    elif isinstance(flag, asyncio.Event):
        return flag.is_set()
    elif isinstance(flag, concurrent.futures.Future):
        return flag.done()
    elif isinstance(flag, threading.Event):
        return flag.is_set()
    else:
        raise TypeError(f"Unsupported type of a flag: {flag!r}")


async def condition_chain(
        source: asyncio.Condition,
        target: asyncio.Condition,
) -> None:
    """
    A condition chain is a "clean" hack to attach one condition to another.

    It is a "clean" (not "dirty") hack to wake up the webhook configuration
    managers when either the resources are revised (as seen in the insights),
    or a new client config is yielded from the webhook server.
    """
    async with source:
        while True:
            await source.wait()
            async with target:
                target.notify_all()


_T = TypeVar('_T')


class Container(Generic[_T]):

    def __init__(self) -> None:
        super().__init__()
        self.changed = asyncio.Condition()
        self._values: Collection[_T] = []  # 0..1 item

    def get_nowait(self) -> _T:  # used mostly in testing
        try:
            return next(iter(self._values))
        except StopIteration:
            raise LookupError("No value is stored in the container.") from None

    async def set(self, value: _T) -> None:
        async with self.changed:
            self._values = [value]
            self.changed.notify_all()

    async def wait(self) -> _T:
        async with self.changed:
            await self.changed.wait_for(lambda: self._values)
        try:
            return next(iter(self._values))
        except StopIteration:  # impossible because of the condition's predicate
            raise LookupError("No value is stored in the container.") from None

    async def reset(self) -> None:
        async with self.changed:
            self._values = []
            self.changed.notify_all()

    async def as_changed(self) -> AsyncIterator[_T]:
        async with self.changed:
            while True:
                try:
                    yield next(iter(self._values))
                except StopIteration:
                    pass
                await self.changed.wait()


# Mind the value: it can be bool-evaluatable but non-bool -- always convert it.
class Toggle:
    """
    An synchronisation primitive that can be awaited both until set or cleared.

    For one-directional toggles, `asyncio.Event` is sufficient.
    But these events cannot be awaited until cleared.

    The bi-directional toggles are needed in some places in the code, such as
    in the population/depletion of a `Vault`, or as in the operator's pause.

    The optional name is used only for hinting in reprs. It can be used when
    there are many toggles, and they need to be distinguished somehow.
    """

    def __init__(
            self,
            __state: bool = False,
            *,
            name: Optional[str] = None,
            condition: Optional[asyncio.Condition] = None,
    ) -> None:
        super().__init__()
        self._condition = condition if condition is not None else asyncio.Condition()
        self._state: bool = bool(__state)
        self._name = name

    def __repr__(self) -> str:
        clsname = self.__class__.__name__
        toggled = 'on' if self._state else 'off'
        if self._name is None:
            return f'<{clsname}: {toggled}>'
        else:
            return f'<{clsname}: {self._name}: {toggled}>'

    def __bool__(self) -> bool:
        raise NotImplementedError  # to protect against accidental misuse

    def is_on(self) -> bool:
        return self._state

    def is_off(self) -> bool:
        return not self._state

    async def turn_to(self, __state: bool) -> None:
        """ Turn the toggle on/off, and wake up the tasks waiting for that. """
        async with self._condition:
            self._state = bool(__state)
            self._condition.notify_all()

    async def wait_for(self, __state: bool) -> None:
        """ Wait until the toggle is turned on/off as expected (if not yet). """
        async with self._condition:
            await self._condition.wait_for(lambda: self._state == bool(__state))

    @property
    def name(self) -> Optional[str]:
        return self._name


class ToggleSet(Collection[Toggle]):
    """
    A read-only checker for multiple toggles.

    The toggle-checker does not have its own state to be turned on/off.

    The positional argument is a function, usually :func:`any` or :func:`all`,
    which takes an iterable of all individual toggles' states (on/off),
    and calculates the overall state of the toggle set.

    With :func:`any`, the set is "on" when at least one child toggle is "on"
    (and it has at least one child), and it is "off" when all children toggles
    are "off" (or if it has no children toggles at all).

    With :func:`all`, the set is "on" when all of its children toggles are "on"
    (or it has no children at all), and it is "off" when at least one child
    toggle is "off" (and there is at least one toggle).

    The multi-toggle sets are used mostly for operator pausing,
    e.g. in peering and in index pre-population. For a practical example,
    in peering, every individual peering identified by name and namespace has
    its own individual toggle to manage, but the whole set of toggles of all
    names & namespaces is used for pausing the operator as one single toggle.
    In index pre-population, the toggles are used on the operator's startup
    to temporarily delay the actual resource handling until all index-handlers
    of all involved resources and resource kinds are processed and stored.

    Note: the set can only contain toggles that were produced by the set;
    externally produced toggles cannot be added, since they do not share
    the same condition object, which is used for synchronisation/notifications.
    """

    def __init__(self, fn: Callable[[Iterable[bool]], bool]) -> None:
        super().__init__()
        self._condition = asyncio.Condition()
        self._toggles: Set[Toggle] = set()
        self._fn = fn

    def __repr__(self) -> str:
        return repr(self._toggles)

    def __len__(self) -> int:
        return len(self._toggles)

    def __iter__(self) -> Iterator[Toggle]:
        return iter(self._toggles)

    def __contains__(self, toggle: object) -> bool:
        return toggle in self._toggles

    def __bool__(self) -> bool:
        raise NotImplementedError  # to protect against accidental misuse

    def is_on(self) -> bool:
        return self._fn(toggle.is_on() for toggle in self._toggles)

    def is_off(self) -> bool:
        return not self.is_on()

    async def wait_for(self, __state: bool) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self.is_on() == bool(__state))

    async def make_toggle(
            self,
            __val: bool = False,
            *,
            name: Optional[str] = None,
    ) -> Toggle:
        toggle = Toggle(__val, name=name, condition=self._condition)
        async with self._condition:
            self._toggles.add(toggle)
            self._condition.notify_all()
        return toggle

    async def drop_toggle(self, toggle: Toggle) -> None:
        async with self._condition:
            self._toggles.discard(toggle)
            self._condition.notify_all()

    async def drop_toggles(self, toggles: Iterable[Toggle]) -> None:
        async with self._condition:
            self._toggles.difference_update(toggles)
            self._condition.notify_all()


FlagReasonT = TypeVar('FlagReasonT', bound=enum.Flag)


class FlagSetter(Generic[FlagReasonT]):
    """
    A boolean flag indicating that the daemon should stop and exit.

    Every daemon gets a ``stopped`` kwarg, which is an event-like object.
    The stopped flag is raised in two cases:

    * The corresponding k8s object is deleted, so the daemon should stop.
    * The whole operator is stopping, so all the daemons should stop too.

    The stopped flag is a graceful way of a daemon termination.
    If the daemons do not react to their stoppers and continue running,
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
        self.reason: Optional[FlagReasonT] = None
        self.sync_event = threading.Event()
        self.async_event = asyncio.Event()
        self.sync_waiter: SyncFlagWaiter[FlagReasonT] = SyncFlagWaiter(self)
        self.async_waiter: AsyncFlagWaiter[FlagReasonT] = AsyncFlagWaiter(self)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.is_set()}, reason={self.reason}>'

    def is_set(self, reason: Optional[FlagReasonT] = None) -> bool:
        """
        Check if the daemon stopper is set: at all or for a specific reason.
        """
        matching_reason = reason is None or (self.reason is not None and reason in self.reason)
        return matching_reason and self.sync_event.is_set()

    def set(self, reason: Optional[FlagReasonT] = None) -> None:
        reason = reason if reason is not None else self.reason  # to keep existing values
        self.when = self.when if self.when is not None else time.monotonic()
        self.reason = reason if self.reason is None or reason is None else self.reason | reason
        self.sync_event.set()
        self.async_event.set()  # it is thread-safe: always called in operator's event loop.


class FlagWaiter(Generic[FlagReasonT]):
    """
    A minimalistic read-only checker for the daemons from the user side.

    This object is fed into the :kwarg:`stopped` kwarg for the handlers.

    The flag setter is hidden from the users, and is an internal class.
    The users should not be able to trigger the stopping activities.

    Usage::

        @kopf.daemon('kopfexamples')
        def handler(stopped, **kwargs):
            while not stopped:
                ...
                stopped.wait(60)
    """

    def __init__(self, setter: FlagSetter[FlagReasonT]) -> None:
        super().__init__()
        self._setter = setter

    def __repr__(self) -> str:
        return repr(self._setter)

    def __bool__(self) -> bool:
        return self._setter.is_set()

    def is_set(self) -> bool:
        return self._setter.is_set()

    @property
    def reason(self) -> Optional[FlagReasonT]:
        return self._setter.reason

    # See the docstring for AsyncFlagPromise for explanation.
    def wait(self, timeout: Optional[float] = None) -> "FlagWaiter[FlagReasonT]":
        # Presumably, `await stopped.wait(n).wait(m)` in async mode.
        raise NotImplementedError("Please report the use-case in the issue tracker if needed.")

    # See the docstring for AsyncFlagPromise for explanation.
    def __await__(self) -> Generator[None, None, "FlagWaiter[FlagReasonT]"]:
        # Presumably, `await stopped` in either sync or async mode.
        raise NotImplementedError("Daemon stoppers should not be awaited directly. "
                                  "Use `await stopped.wait()`.")


class SyncFlagWaiter(FlagWaiter[FlagReasonT], Generic[FlagReasonT]):
    def wait(self, timeout: Optional[float] = None) -> "SyncFlagWaiter[FlagReasonT]":
        self._setter.sync_event.wait(timeout=timeout)
        return self


class AsyncFlagWaiter(FlagWaiter[FlagReasonT], Generic[FlagReasonT]):
    def wait(self, timeout: Optional[float] = None) -> "AsyncFlagPromise[FlagReasonT]":
        # A new checker instance, which is awaitable and returns the original checker in the end.
        return AsyncFlagPromise(self, timeout=timeout)


class AsyncFlagPromise(FlagWaiter[FlagReasonT],
                       Awaitable[AsyncFlagWaiter[FlagReasonT]],
                       Generic[FlagReasonT]):
    """
    An artificial future-like promise for ``await stopped.wait(...)``.

    This is a low-level "clean hack" to simplify the publicly faced types.
    As a trade-off, the complexity moves to the implementation side.

    The only goal is to accept one and only one type for the ``stopped`` kwarg
    in ``@kopf.daemon()`` handlers (protocol `callbacks.ResourceDaemonFn`)
    regardless of whether they are sync or async, but still usable for both.

    For this, the kwarg is announced with the base type `DaemonStoppingFlag`,
    not as a union of sync or async, or any other double-typed approach.

    The challenge is to support ``stopped.wait()`` in both sync & async modes:

    * sync: ``stopped.wait(float) -> bool``.
    * async: ``await stopped.wait(float) -> bool``.

    The sync case is the primary case as there are no alternatives to it.
    The async case is secondary because such use is discouraged (but supported);
    it is recommended to rely on regular task cancellations in async daemons.

    To solve it, instead of returning a ``bool``, a ``bool``-evaluable object
    is returned --- the checker itself. This solves the primary (sync) use-case:
    just sleep for the requested duration and return bool-evaluable self.

    To follow the established signatures of the primary (sync) use-case,
    the secondary (async) use-case also returns an instance of the checker:
    but not "self"! Instead, it creates a new time-limited & awaitable checker
    for every call to ``stopped.wait(n)``, limiting its life by ``n`` seconds.

    Extra checker instances add some tiny memory overhead, but this is fine
    since the use-case is discouraged and there is a better native alternative.

    Then, going back through the class hierarchy, all classes are made awaitable
    so that this functionality becomes exposed via the declared base class.
    But all checkers except the time-limited one prohibit waiting for them.
    """

    def __init__(self, waiter: AsyncFlagWaiter[FlagReasonT], *, timeout: Optional[float]) -> None:
        super().__init__(waiter._setter)
        self._timeout = timeout
        self._waiter = waiter

    def __await__(self) -> Generator[None, None, AsyncFlagWaiter[FlagReasonT]]:
        name = f"time-limited waiting for the daemon stopper {self._setter!r}"
        coro = asyncio.wait_for(self._setter.async_event.wait(), timeout=self._timeout)
        task = aiotasks.create_task(coro, name=name)
        try:
            yield from task
        except asyncio.TimeoutError:
            pass
        return self._waiter  # the original checker! not the time-limited one!


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


DaemonStopper = FlagSetter[DaemonStoppingReason]
DaemonStopped = FlagWaiter[DaemonStoppingReason]
SyncDaemonStopperChecker = SyncFlagWaiter[DaemonStoppingReason]  # deprecated
AsyncDaemonStopperChecker = AsyncFlagWaiter[DaemonStoppingReason]  # deprecated


async def sleep_or_wait(
        delays: Union[None, float, Collection[Union[None, float]]],
        wakeup: Optional[asyncio.Event] = None,
) -> Optional[float]:
    """
    Measure the sleep time: either until the timeout, or until the event is set.

    Returns the number of seconds left to sleep, or ``None`` if the sleep was
    not interrupted and reached its specified delay (an equivalent of ``0``).
    In theory, the result can be ``0`` if the sleep was interrupted precisely
    the last moment before timing out; this is unlikely to happen though.
    """
    passed_delays = delays if isinstance(delays, collections.abc.Collection) else [delays]
    actual_delays = [delay for delay in passed_delays if delay is not None]
    minimal_delay = min(actual_delays) if actual_delays else 0

    # Do not go for the real low-level system sleep if there is no need to sleep.
    if minimal_delay <= 0:
        return None

    awakening_event = wakeup if wakeup is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        start_time = loop.time()
        await asyncio.wait_for(awakening_event.wait(), timeout=minimal_delay)
    except asyncio.TimeoutError:
        return None  # interruptable sleep is over: uninterrupted.
    else:
        end_time = loop.time()
        duration = end_time - start_time
        return max(0, minimal_delay - duration)
