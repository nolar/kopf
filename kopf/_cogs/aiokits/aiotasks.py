"""
Helpers for orchestrating asyncio tasks.

These utilities only support tasks, not more generic futures, coroutines,
or other awaitables. In most case where we use it, we need specifically tasks,
as we not only wait for them, but also cancel them.

Anyway, ``asyncio`` wraps all awaitables and coroutines into tasks on almost
all function calls with multiple awaiables (e.g. :func:`asyncio.wait`),
so there is no added overhead; instead, the implicit overhead is made explicit.
"""
import asyncio
from typing import TYPE_CHECKING, Any, Callable, Collection, Coroutine, \
                   NamedTuple, Optional, Set, Tuple, TypeVar

from kopf._cogs.helpers import typedefs

_T = TypeVar('_T')

# A workaround for a difference in tasks at runtime and type-checking time.
# Otherwise, at runtime: TypeError: 'type' object is not subscriptable.
if TYPE_CHECKING:
    Future = asyncio.Future[Any]
    Task = asyncio.Task[Any]
else:
    Future = asyncio.Future
    Task = asyncio.Task


async def cancel_coro(
        coro: Coroutine[Any, Any, Any],
        *,
        name: Optional[str] = None,
) -> None:
    """
    Cancel the coroutine if the wrapped code block is cancelled or fails.

    All coroutines must be awaited to prevent RuntimeWarnings/ResourceWarnings.
    As such, we generally need to create a dummy task and cancel it immediately.
    Despite asyncio tasks are lightweight, they still create an object and thus
    consume memory. This can be undesired when applied at scale --- e.g.
    in the multiplexer: when the watcher exits, it cancels all pending workers.

    To save memory, we first try to close the coroutine with no dummy task.
    As a fallback, the coroutine is cancelled gracefully via a dummy task.

    The context manager should be applied to all async code in the managing
    (i.e. parent/wrapping) coroutine from the beginning of it till the managed
    coro is actually awaited and executed.
    """
    try:
        # A dirty (undocumented) way to close a coro, but it saves memory.
        coro.close()  # OR: coro.throw(asyncio.CancelledError())
    except AttributeError:
        # The official way is to create an extra task object, thus to waste some memory.
        corotask = asyncio.create_task(coro=coro, name=name)
        corotask.cancel()
        try:
            await corotask
        except asyncio.CancelledError:
            pass  # cancellations are expected at this point


async def guard(
        coro: Coroutine[Any, Any, Any],
        name: str,
        *,
        flag: Optional[asyncio.Event] = None,
        finishable: bool = False,
        cancellable: bool = False,
        logger: Optional[typedefs.Logger] = None,
) -> None:
    """
    A guard for a presumably eternal (never-finishing) task.

    An "eternal" task is a task that never exits unless explicitly cancelled.
    If it does, this is a misbehaviour that is logged. Errors are always logged.
    Cancellations are also logged except if the task is said to be cancellable.

    It is used for background tasks that are started but never awaited/checked,
    so that the errors are not escalated properly; or if they are occasionally
    awaited/checked with a significant delay after an error possibly happend,
    but needs to be logged as soon as it happens.
    """
    capname = name.capitalize()

    # Guarded tasks can have prerequisites, which are set in other tasks.
    if flag is not None:
        try:
            await flag.wait()
        except asyncio.CancelledError:
            await cancel_coro(coro=coro, name=name)
            raise

    try:
        await coro
    except asyncio.CancelledError:
        if logger is not None and not cancellable:
            logger.debug(f"{capname} is cancelled.")
        raise
    except Exception as e:
        if logger is not None:
            logger.exception(f"{capname} has failed: {e}")
        raise
    else:
        if logger is not None and not finishable:
            logger.warning(f"{capname} has finished unexpectedly.")


def create_guarded_task(
        coro: Coroutine[Any, Any, Any],
        name: str,
        *,
        flag: Optional[asyncio.Event] = None,
        finishable: bool = False,
        cancellable: bool = False,
        logger: Optional[typedefs.Logger] = None,
) -> Task:
    """
    Create a guarded eternal task. See :func:`guard` for explanation.

    This is only a shortcut for named task creation (name is used in 2 places).
    """
    return asyncio.create_task(
        name=name,
        coro=guard(
            name=name,
            coro=coro,
            flag=flag,
            finishable=finishable,
            cancellable=cancellable,
            logger=logger))


async def wait(
        tasks: Collection[Task],
        *,
        timeout: Optional[float] = None,
        return_when: Any = asyncio.ALL_COMPLETED,
) -> Tuple[Set[Task], Set[Task]]:
    """
    A safer version of :func:`asyncio.wait` -- does not fail on an empty list.
    """
    if not tasks:
        return set(), set()
    done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=return_when)
    return done, pending


async def stop(
        tasks: Collection[Task],
        *,
        title: str,
        quiet: bool = False,
        cancelled: bool = False,
        interval: Optional[float] = None,
        logger: Optional[typedefs.Logger] = None,
) -> Tuple[Set[Task], Set[Task]]:
    """
    Cancel the tasks and wait for them to finish; log if some are stuck.

    By default, all stopping activities are logged. In the quiet mode, logs
    only the tasks that are stuck longer than the designated polling interval,
    and their eventual exit if they were reported as stuck at least once.

    If the interval is not set, no polling is performed, and the stopping
    should happen in one iteration (even if it is going to take an eternity).

    The stopping itself does not have timeouts. It always ends either with
    the tasks stopped/exited, or with the stop-routine itself being cancelled.

    For better logging only, if the stopping routine is marked as performing
    the cancellation already (via ``cancelled=True``), then the cancellation
    of the stopping routine is considered as "double-cancelling".
    This does not affect the behaviour, but only the log messages.
    """
    captitle = title.capitalize()

    if not tasks:
        if logger is not None and not quiet:
            logger.debug(f"{captitle} tasks stopping is skipped: no tasks given.")
        return set(), set()

    for task in tasks:
        task.cancel()

    iterations = 0
    done_ever: Set[Task] = set()
    pending: Set[Task] = set(tasks)
    while pending:
        iterations += 1

        # If the waiting (current) task is cancelled before the wait is over,
        # propagate the cancellation to all the awaited (sub-) tasks, and let them finish.
        try:
            done_now, pending = await wait(pending, timeout=interval)
        except asyncio.CancelledError:
            # If the waiting (current) task is cancelled while propagating the cancellation
            # (i.e. double-cancelled), let it fail without graceful cleanup. It is urgent, it seems.
            pending = {task for task in tasks if not task.done()}
            if logger is not None and (not quiet or pending or iterations > 1):
                are = 'are' if not pending else 'are not'
                why = 'double-cancelling at stopping' if cancelled else 'cancelling at stopping'
                logger.debug(f"{captitle} tasks {are} stopped: {why}; tasks left: {pending!r}")
            raise  # the repeated cancellation, handled specially.
        else:
            # If the cancellation is propagated normally and the awaited (sub-) tasks exited,
            # consider it as a successful cleanup.
            if logger is not None and (not quiet or pending or iterations > 1):
                are = 'are' if not pending else 'are not'
                why = 'cancelling normally' if cancelled else 'finishing normally'
                logger.debug(f"{captitle} tasks {are} stopped: {why}; tasks left: {pending!r}")
            done_ever |= done_now

    return done_ever, pending


async def reraise(
        tasks: Collection[Task],
) -> None:
    """
    Re-raise errors from tasks, if any. Do nothing if all tasks have succeeded.
    """
    for task in tasks:
        try:
            task.result()  # can raise the regular (non-cancellation) exceptions.
        except asyncio.CancelledError:
            pass  # re-raise anything except regular cancellations/exits


async def all_tasks(
        *,
        ignored: Collection[Task] = frozenset(),
) -> Collection[Task]:
    """
    Return all tasks in the current event loop.

    Equivalent to :func:`asyncio.all_tasks`, but with an exlcusion list.
    The exclusion list is used to exclude the tasks that existed at a point
    in time in the past, to only get the tasks that appeared since then.
    """
    current_task = asyncio.current_task()
    return {task for task in asyncio.all_tasks()
            if task is not current_task and task not in ignored}


class SchedulerJob(NamedTuple):
    coro: Coroutine[Any, Any, Any]
    name: Optional[str]


class Scheduler:
    """
    An scheduler/orchestrator/executor for "fire-and-forget" tasks.

    Coroutines can be spawned via this scheduler and forgotten: no need to wait
    for them or to check their status --- the scheduler will take care of it.

    It is a simplified equivalent of aiojobs, but compatible with Python 3.10.
    Python 3.10 removed the explicit event loops (deprecated since Python 3.7),
    which broke aiojobs. At the same time, aiojobs looks unmaintained
    and contains no essential changes since July 2019 (i.e. for 2+ years).
    Hence the necessity to replicate the functionality.

    The scheduler is needed only for internal use and is not exposed to users.
    It is mainly used in the multiplexer (splitting a single stream of events
    of a resource kind into multiple queues of individual resource objects).
    Therefore, many of the features of aiojobs are removed as unnecessary:
    no individual task/job handling or closing, no timeouts, etc.

    .. note::

        Despite all coros will be wrapped into tasks sooner or later,
        and despite it is convincing to do this earlier and manage the tasks
        rather than our own queue of coros+names, do not do this:
        we want all tasks to refer to their true coros in their reprs,
        not to wrappers which wait until the running capacity is available.
    """

    def __init__(
            self,
            *,
            limit: Optional[int] = None,
            exception_handler: Optional[Callable[[BaseException], None]] = None,
    ) -> None:
        super().__init__()
        self._closed = False
        self._limit = limit
        self._exception_handler = exception_handler
        self._condition = asyncio.Condition()
        self._pending_coros: asyncio.Queue[SchedulerJob] = asyncio.Queue()
        self._running_tasks: Set[Task] = set()
        self._cleaning_queue: asyncio.Queue[Task] = asyncio.Queue()
        self._cleaning_task = asyncio.create_task(self._task_cleaner(), name=f"cleaner of {self!r}")
        self._spawning_task = asyncio.create_task(self._task_spawner(), name=f"spawner of {self!r}")

    def empty(self) -> bool:
        """ Check if the scheduler has nothing to do. """
        return self._pending_coros.empty() and not self._running_tasks

    async def wait(self) -> None:
        """
        Wait until the scheduler does nothing, i.e. idling (all tasks are done).
        """
        async with self._condition:
            await self._condition.wait_for(self.empty)

    async def close(self) -> None:
        """
        Stop accepting new tasks and cancel all running/pending ones.
        """

        # Running tasks are cancelled here. Pending tasks are cancelled at actual spawning.
        self._closed = True
        for task in self._running_tasks:
            task.cancel()

        # Wait until all tasks are fully done (it can take some time). This also includes
        # the pending coros, which are spawned and instantly cancelled (to prevent RuntimeWarnings).
        await self.wait()

        # Cleanup the scheduler's own resources.
        await stop({self._spawning_task, self._cleaning_task}, title="scheduler", quiet=True)

    async def spawn(
            self,
            coro: Coroutine[Any, Any, Any],
            *,
            name: Optional[str] = None,
    ) -> None:
        """
        Schedule a coroutine for ownership and eventual execution.

        Coroutine ownership ensures that all "fire-and-forget" coroutines
        that were passed to the scheduler will be awaited (to prevent warnings),
        even if the scheduler is closed before the coroutines are started.
        If a coroutine is added to a closed scheduler, it will be instantly
        cancelled before raising the scheduler's exception.
        """
        if self._closed:
            await cancel_coro(coro=coro, name=name)
            raise RuntimeError("Cannot add new coroutines to a closed and inactive scheduler.")
        async with self._condition:
            await self._pending_coros.put(SchedulerJob(coro=coro, name=name))
            self._condition.notify_all()  # -> task_spawner()

    def _can_spawn(self) -> bool:
        return (not self._pending_coros.empty() and
                (self._limit is None or len(self._running_tasks) < self._limit))

    async def _task_spawner(self) -> None:
        """ An internal meta-task to actually start pending coros as tasks. """
        while True:
            async with self._condition:
                await self._condition.wait_for(self._can_spawn)

                # Spawn as many tasks as allowed and as many coros as available at the moment.
                # Since nothing monitors the tasks "actively", we configure them to report back
                # when they are finished --- to be awaited and released "passively".
                while self._can_spawn():
                    coro, name = self._pending_coros.get_nowait()  # guaranteed by the predicate
                    task = asyncio.create_task(coro=coro, name=name)
                    task.add_done_callback(self._task_done_callback)
                    self._running_tasks.add(task)
                    if self._closed:
                        task.cancel()  # used to await the coros without executing them.

    async def _task_cleaner(self) -> None:
        """ An internal meta-task to cleanup the actually finished tasks. """
        while True:
            task = await self._cleaning_queue.get()

            # Await the task from an outer context to prevent RuntimeWarnings/ResourceWarnings.
            try:
                await task
            except BaseException:
                # The errors are handled in the done-callback. Suppress what has leaked for safety.
                pass

            # Ping other tasks to refill the pool of running tasks (or to close the scheduler).
            async with self._condition:
                self._running_tasks.discard(task)
                self._condition.notify_all()  # -> task_spawner() & close()

    def _task_done_callback(self, task: Task) -> None:
        # When a "fire-and-forget" task is done, release its system resources immediately:
        # nothing else is going to explicitly "await" for it any time soon, so we must do it.
        # But since a callback cannot be async, "awaiting" is done in a background utility task.
        self._running_tasks.discard(task)
        self._cleaning_queue.put_nowait(task)

        # If failed, initiate a callback defined by the owner of the task (if any).
        exc: Optional[BaseException]
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            exc = None
        if exc is not None and self._exception_handler is not None:
            self._exception_handler(exc)
