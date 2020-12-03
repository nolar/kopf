"""
Helpers for orchestrating asyncio tasks.

These utilities only support tasks, not more generic futures, coroutines,
or other awaitables. In most case where we use it, we need specifically tasks,
as we not only wait for them, but also cancel them.

Anyway, ``asyncio`` wraps all awaitables and coroutines into tasks on almost
all function calls with multiple awaiables (e.g. :func:`asyncio.wait`),
so there is no added overhead; intstead, the implicit overhead is made explicit.
"""
import asyncio
import logging
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Collection, Coroutine, \
                   Generator, Optional, Set, Tuple, TypeVar, Union, cast

_T = TypeVar('_T')

# A workaround for a difference in tasks at runtime and type-checking time.
# Otherwise, at runtime: TypeError: 'type' object is not subscriptable.
if TYPE_CHECKING:
    Future = asyncio.Future[Any]
    Task = asyncio.Task[Any]
else:
    Future = asyncio.Future
    Task = asyncio.Task

# Accept `name=` always, but simulate it for Python 3.7 to do nothing.
if sys.version_info >= (3, 8):
    create_task = asyncio.create_task
else:
    def create_task(
            coro: Union[Generator[Any, None, _T], Awaitable[_T]],
            *,
            name: Optional[str] = None,  # noqa
    ) -> Task:
        return asyncio.create_task(coro)


async def guard(
        coro: Coroutine[Any, Any, Any],
        name: str,
        *,
        flag: Optional[asyncio.Event] = None,
        finishable: bool = False,
        cancellable: bool = False,
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None,
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
        await flag.wait()

    try:
        await coro
    except asyncio.CancelledError:
        if logger is not None and not cancellable:
            logger.debug(f"{capname} is cancelled.")
        raise
    except Exception as e:
        if logger is not None:
            logger.exception(f"{capname} has failed: %s", e)
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
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None,
) -> Task:
    """
    Create a guarded eternal task. See :func:`guard` for explanation.

    This is only a shortcut for named task creation (name is used in 2 places).
    """
    return create_task(
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
    return cast(Set[Task], done), cast(Set[Task], pending)


async def stop(
        tasks: Collection[Task],
        *,
        title: str,
        quiet: bool = False,
        cancelled: bool = False,
        interval: Optional[float] = None,
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None,
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
            pass


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
