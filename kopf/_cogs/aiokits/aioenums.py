import asyncio
import enum
import threading
import time
from typing import Awaitable, Generator, Generic, Optional, TypeVar

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
        return f'<{self.__class__.__name__}: {self.is_set()}, reason={self.reason}>'

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
    in ``@kopf.daemon()`` handlers (protocol :class:`callbacks.DaemonFn`)
    regardless of whether they are sync or async, but still usable for both.

    For this, the kwarg is announced with the base type :class:`DaemonStopped`,
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
        task = asyncio.create_task(coro, name=name)
        try:
            yield from task
        except asyncio.TimeoutError:
            pass  # the requested time limit is reached, exit regardless of the state
        return self._waiter  # the original checker! not the time-limited one!
