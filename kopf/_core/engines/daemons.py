"""
Daemons are background tasks accompanying the individual resource objects.

Every ``@kopf.daemon`` and ``@kopf.timer`` handler produces a separate
asyncio task to either directly execute the daemon, or to trigger one-shot
handlers by schedule. The wrapping tasks are always async; the sync functions
are called in thread executors as part of a regular handler invocation.

These tasks are remembered in the per-resources *memories* (arbitrary data
containers) through the life-cycle of the operator.

Since the operators are event-driven conceptually, there are no background tasks
running for every individual resources normally (i.e. without the daemons),
so there are no connectors between the operator's root tasks and the daemons,
so there is no way to stop/kill/cancel the daemons when the operator exits.

For this, there is an artificial root task spawned to kill all the daemons
when the operator exits, and all root tasks are gracefully/forcedly terminated.
Otherwise, all the daemons would be considered as "hung" tasks and will be
force-killed after some timeout -- which can be avoided, since we are aware
of the daemons, and they are not actually "hung".
"""
import abc
import asyncio
import dataclasses
import time
import warnings
from typing import Collection, Dict, Iterable, List, Mapping, \
                   MutableMapping, Optional, Sequence, Set

from kopf._cogs.aiokits import aiotasks, aiotime, aiotoggles
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, ids, patches
from kopf._core.actions import application, execution, lifecycles, loggers, progression
from kopf._core.intents import causes, handlers as handlers_, stoppers


@dataclasses.dataclass(frozen=True)
class Daemon:
    task: aiotasks.Task  # a guarding task of the daemon.
    logger: typedefs.Logger
    handler: handlers_.SpawningHandler
    stopper: stoppers.DaemonStopper  # a signaller for the termination and its reason.


@dataclasses.dataclass(frozen=False)
class DaemonsMemory:
    # For background and timed threads/tasks (invoked with the kwargs of the last-seen body).
    live_fresh_body: Optional[bodies.Body] = None
    idle_reset_time: float = dataclasses.field(default_factory=time.monotonic)
    forever_stopped: Set[ids.HandlerId] = dataclasses.field(default_factory=set)
    running_daemons: Dict[ids.HandlerId, Daemon] = dataclasses.field(default_factory=dict)


class DaemonsMemoriesIterator(metaclass=abc.ABCMeta):
    """
    Re-iterable view of all the running daemons for the `daemon_killer`.

    Implemented in `Memories`. This is a clean hack to resolve circular imports
    (the daemon killer needs memories, but the memories contain Daemon records)
    by splitting the specialised interface (this class) from the implementation.
    """
    @abc.abstractmethod
    def iter_all_daemon_memories(self) -> Iterable[DaemonsMemory]:
        raise NotImplementedError


async def spawn_daemons(
        *,
        settings: configuration.OperatorSettings,
        handlers: Sequence[handlers_.SpawningHandler],
        daemons: MutableMapping[ids.HandlerId, Daemon],
        cause: causes.SpawningCause,
        memory: DaemonsMemory,
) -> Collection[float]:
    """
    Ensure that all daemons are spawned for this individual resource.

    This function can be called multiple times on multiple handling cycles
    (though usually should be called on the first-seen occasion), so it must
    be idempotent: not having duplicating side-effects on multiple calls.
    """
    if memory.live_fresh_body is None:  # for type-checking; "not None" is ensured in processing.
        raise RuntimeError("A daemon is spawned with None as body. This is a bug. Please report.")
    for handler in handlers:
        if handler.id not in daemons:
            stopper = stoppers.DaemonStopper()
            daemon_cause = causes.DaemonCause(
                resource=cause.resource,
                indices=cause.indices,
                logger=cause.logger,
                memo=cause.memo,
                body=memory.live_fresh_body,
                patch=patches.Patch(),  # not the same as the one-shot spawning patch!
                stopper=stopper,  # for checking (passed to kwargs)
            )
            daemon = Daemon(
                stopper=stopper,  # for stopping (outside of causes)
                handler=handler,
                logger=loggers.LocalObjectLogger(body=cause.body, settings=settings),
                task=asyncio.create_task(_runner(
                    settings=settings,
                    daemons=daemons,  # for self-garbage-collection
                    handler=handler,
                    cause=daemon_cause,
                    memory=memory,
                ), name=f'runner of {handler.id}'),  # sometimes, daemons; sometimes, timers.
            )
            daemons[handler.id] = daemon
    return []


async def match_daemons(
        *,
        settings: configuration.OperatorSettings,
        handlers: Sequence[handlers_.SpawningHandler],
        daemons: MutableMapping[ids.HandlerId, Daemon],
) -> Collection[float]:
    """
    Re-match the running daemons with the filters, and stop those mismatching.

    Stopping can take a few iterations, same as `stop_daemons` would do.
    """
    matching_daemon_ids = {handler.id for handler in handlers}
    mismatching_daemons = {
        daemon.handler.id: daemon
        for daemon in daemons.values()
        if daemon.handler.id not in matching_daemon_ids
    }
    delays = await stop_daemons(
        settings=settings,
        daemons=mismatching_daemons,
        reason=stoppers.DaemonStoppingReason.FILTERS_MISMATCH,
    )
    return delays


async def stop_daemons(
        *,
        settings: configuration.OperatorSettings,
        daemons: Mapping[ids.HandlerId, Daemon],
        reason: stoppers.DaemonStoppingReason = stoppers.DaemonStoppingReason.RESOURCE_DELETED,
) -> Collection[float]:
    """
    Terminate all daemons of an individual resource (gracefully and by force).

    All daemons are terminated in parallel to speed up the termination
    (especially taking into account that some daemons can take time to finish).

    The daemons are asked to terminate as soon as the object is marked
    for deletion. It can take some time until the deletion handlers also
    finish their work. The object is not physically deleted until all
    the daemons are terminated (by putting a finalizer on it).

    **Notes on this non-trivial implementation:**

    There is a same-purpose function `stop_daemon`, which works fully in-memory.
    That method is used when killing the daemons on operator exit.
    This method is used when the resource is deleted.

    The difference is that in this method (termination with delays and patches),
    other on-deletion handlers can be happening at the same time as the daemons
    are being terminated (it can take time due to backoffs and timeouts).
    In the end, the finalizer should be removed only once all deletion handlers
    have succeeded and all daemons are terminated -- not earlier than that.
    None of this (handlers and finalizers) is needed for the operator exiting.

    To know "when" the next check of daemons should be performed:

    * EITHER the operator should block this resource's processing and wait until
      the daemons are terminated -- thus leaking daemon's abstractions and logic
      and tools (e.g. a task scheduler) to the upper level of processing;

    * OR the daemons termination should mimic the change-detection handlers
      and simulate the delays with multiple handling cycles -- in order to
      re-check the daemon's status regularly until they are done.

    Both of this approaches have the same complexity. But the latter one
    keep the logic isolated into the daemons module/routines (a bit cleaner).

    Hence, these duplicating methods of termination for different cases
    (as by their surrounding circumstances: deletion handlers and finalizers).
    """
    delays: List[float] = []
    now = time.monotonic()
    for daemon in list(daemons.values()):
        logger = daemon.logger
        stopper = daemon.stopper
        age = (now - (stopper.when or now))

        handler = daemon.handler
        if isinstance(handler, handlers_.DaemonHandler):
            backoff = handler.cancellation_backoff
            timeout = handler.cancellation_timeout
            polling = handler.cancellation_polling or settings.background.cancellation_polling
        elif isinstance(handler, handlers_.TimerHandler):
            backoff = None
            timeout = None
            polling = settings.background.cancellation_polling
        else:
            raise RuntimeError(f"Unsupported daemon handler: {handler!r}")

        # Whatever happens with other flags & logs & timings, this flag must be surely set.
        if not stopper.is_set(reason=reason):
            stopper.set(reason=reason)
            await _wait_for_instant_exit(settings=settings, daemon=daemon)

        # Try different approaches to exiting the daemon based on timings.
        if daemon.task.done():
            pass  # same as if the daemon is not in the structure anymore (self-deleted on exit).

        elif backoff is not None and age < backoff:
            if not stopper.is_set(reason=stoppers.DaemonStoppingReason.DAEMON_SIGNALLED):
                stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_SIGNALLED)
                logger.debug(f"{handler} is signalled to exit gracefully.")
                await _wait_for_instant_exit(settings=settings, daemon=daemon)
            if not daemon.task.done():  # due to "instant exit"
                delays.append(backoff - age)

        elif timeout is not None and age < timeout + (backoff or 0):
            if not stopper.is_set(reason=stoppers.DaemonStoppingReason.DAEMON_CANCELLED):
                stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_CANCELLED)
                logger.debug(f"{handler} is signalled to exit by force.")
                daemon.task.cancel()
                await _wait_for_instant_exit(settings=settings, daemon=daemon)
            if not daemon.task.done():  # due to "instant exit"
                delays.append(timeout + (backoff or 0) - age)

        elif timeout is not None:
            if not stopper.is_set(reason=stoppers.DaemonStoppingReason.DAEMON_ABANDONED):
                stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_ABANDONED)
                logger.warning(f"{handler} did not exit in time. Leaving it orphaned.")
                warnings.warn(f"{handler} did not exit in time.", ResourceWarning)

        else:
            logger.debug(f"{handler} is still exiting. The next check is in {polling} seconds.")
            delays.append(polling)

    return delays


async def daemon_killer(
        *,
        settings: configuration.OperatorSettings,
        memories: DaemonsMemoriesIterator,
        operator_paused: aiotoggles.ToggleSet,
) -> None:
    """
    An operator's root task to kill the daemons on the operator's demand.

    The "demand" comes in two cases: when the operator is exiting (gracefully
    or not), and when the operator is pausing because of peering. In that case,
    all watch-streams are disconnected, and all daemons/timers should stop.

    When pausing, the daemons/timers are stopped via their regular stopping
    procedure: with graceful or forced termination, backoffs, timeouts.

    .. warning::

        Each daemon will be respawned on the next K8s watch-event strictly
        after the previous daemon is fully stopped.
        There are never 2 instances of the same daemon running in parallel.

        In normal cases (enough time is given to stop), this is usually done
        by the post-pause re-listing event. In rare cases when the re-pausing
        happens faster than the daemon is stopped (highly unlikely to happen),
        that event can be missed because the daemon is being stopped yet,
        so the respawn can happen with a significant delay.

        This issue is considered low-priority & auxiliary, so as the peering
        itself. It can be fixed later. Workaround: make daemons to exit fast.
    """
    # Unlimited job pool size —- the same as if we would be managing the tasks directly.
    # Unlimited timeout in `close()` -- since we have our own per-daemon timeout management.
    scheduler = aiotasks.Scheduler()
    try:
        while True:

            # Stay here while the operator is running normally, until it is paused.
            await operator_paused.wait_for(True)

            # The stopping tasks are "fire-and-forget" -- we do not get (or care of) the result.
            # The daemons remain resumable, since they exit not on their own accord.
            for memory in memories.iter_all_daemon_memories():
                for daemon in memory.running_daemons.values():
                    await scheduler.spawn(
                        name=f"pausing stopper of {daemon}",
                        coro=stop_daemon(
                            settings=settings,
                            daemon=daemon,
                            reason=stoppers.DaemonStoppingReason.OPERATOR_PAUSING))

            # Stay here while the operator is paused, until it is resumed.
            # The fresh stream of watch-events will spawn new daemons naturally.
            await operator_paused.wait_for(False)

    # Terminate all running daemons when the operator exits (and this task is cancelled).
    finally:
        for memory in memories.iter_all_daemon_memories():
            for daemon in memory.running_daemons.values():
                await scheduler.spawn(
                    name=f"exiting stopper of {daemon}",
                    coro=stop_daemon(
                        settings=settings,
                        daemon=daemon,
                        reason=stoppers.DaemonStoppingReason.OPERATOR_EXITING))
        await scheduler.wait()  # prevent insta-cancelling our own coros (daemon stoppers).
        await scheduler.close()


async def stop_daemon(
        *,
        settings: configuration.OperatorSettings,
        daemon: Daemon,
        reason: stoppers.DaemonStoppingReason,
) -> None:
    """
    Stop a single daemon.

    The purpose is the same as in `stop_daemons`, but this function
    is called on operator exiting, so there is no multi-step handling,
    everything happens in memory and linearly (while respecting the timing).

    For explanation on different implementations, see `stop_daemons`.
    """
    handler = daemon.handler
    if isinstance(handler, handlers_.DaemonHandler):
        backoff = handler.cancellation_backoff
        timeout = handler.cancellation_timeout
    elif isinstance(handler, handlers_.TimerHandler):
        backoff = None
        timeout = None
    else:
        raise RuntimeError(f"Unsupported daemon handler: {handler!r}")

    # Whatever happens with other flags & logs & timings, this flag must be surely set.
    daemon.stopper.set(reason=reason)
    await _wait_for_instant_exit(settings=settings, daemon=daemon)

    if daemon.task.done():
        daemon.logger.debug(f"{handler} has exited gracefully.")

    # Try different approaches to exiting the daemon based on timings.
    if not daemon.task.done() and backoff is not None:
        daemon.stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_SIGNALLED)
        daemon.logger.debug(f"{handler} is signalled to exit gracefully.")
        await aiotasks.wait([daemon.task], timeout=backoff)

    if not daemon.task.done() and timeout is not None:
        daemon.stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_CANCELLED)
        daemon.logger.debug(f"{handler} is signalled to exit by force.")
        daemon.task.cancel()
        await aiotasks.wait([daemon.task], timeout=timeout)

    if not daemon.task.done():
        daemon.stopper.set(reason=stoppers.DaemonStoppingReason.DAEMON_ABANDONED)
        daemon.logger.warning(f"{handler} did not exit in time. Leaving it orphaned.")
        warnings.warn(f"{handler} did not exit in time.", ResourceWarning)


async def _wait_for_instant_exit(
        *,
        settings: configuration.OperatorSettings,
        daemon: Daemon,
) -> None:
    """
    Wait for a kind-of-instant exit of a daemon/timer.

    It might be so, that the daemon exits instantly (if written properly).
    Avoid resource patching and unnecessary handling cycles in this case:
    just give the asyncio event loop an extra time & cycles to finish it.

    There is nothing "instant", of course. Any code takes some time to execute.
    We just assume that the "instant" is something defined by a small timeout
    and a few zero-time asyncio cycles (read as: zero-time `await` statements).
    """

    if daemon.task.done():
        pass

    elif settings.background.instant_exit_timeout is not None:
        await aiotasks.wait([daemon.task], timeout=settings.background.instant_exit_timeout)

    elif settings.background.instant_exit_zero_time_cycles is not None:
        for _ in range(settings.background.instant_exit_zero_time_cycles):
            await asyncio.sleep(0)
            if daemon.task.done():
                break


async def _runner(
        *,
        settings: configuration.OperatorSettings,
        daemons: MutableMapping[ids.HandlerId, Daemon],
        handler: handlers_.SpawningHandler,
        memory: DaemonsMemory,
        cause: causes.DaemonCause,
) -> None:
    """
    Guard a running daemon during its life cycle.

    Note: synchronous daemons are awaited to the exit and postpone cancellation.
    The runner will not exit until the thread exits. See `invoke` for details.
    """
    stopper = cause.stopper

    try:
        if isinstance(handler, handlers_.DaemonHandler):
            await _daemon(settings=settings, handler=handler, cause=cause)
        elif isinstance(handler, handlers_.TimerHandler):
            await _timer(settings=settings, handler=handler, cause=cause, memory=memory)
        else:
            raise RuntimeError("Cannot determine which task wrapper to use. This is a bug.")

    finally:

        # Prevent future re-spawns for those exited on their own, for no reason.
        # Only the filter-mismatching or peering-pausing daemons can be re-spawned.
        if stopper.reason is None:
            memory.forever_stopped.add(handler.id)

        # If this daemon is never going to be called again, we can release the
        # live_fresh_body to save some memory.
        if handler.id in memory.forever_stopped:
            # If any other running daemon is referencing this Kubernetes
            # resource, we can't free it
            can_free = True
            this_daemon = daemons[handler.id]
            for running_daemon in memory.running_daemons.values():
                if running_daemon is not this_daemon:
                    can_free = False
                    break
            if can_free:
                memory.live_fresh_body = None

        # Save the memory by not remembering the exited daemons (they may be never re-spawned).
        del daemons[handler.id]

        # Whatever happened, make sure the sync threads of asyncio threaded executor are notified:
        # in a hope that they will exit maybe some time later to free the OS/asyncio resources.
        # A possible case: operator is exiting and cancelling all "hung" non-root tasks, etc.
        stopper.set(reason=stoppers.DaemonStoppingReason.DONE)


async def _daemon(
        *,
        settings: configuration.OperatorSettings,
        handler: handlers_.DaemonHandler,
        cause: causes.DaemonCause,
) -> None:
    """
    A long-running guarding task for a resource daemon handler.

    The handler is executed either once or repeatedly, based on the handler
    declaration.

    A few kinds of errors are suppressed, those expected from the daemons when
    they are cancelled due to the resource deletion.
    """
    resource = cause.resource
    stopper = cause.stopper
    logger = cause.logger
    patch = cause.patch
    body = cause.body

    if handler.initial_delay is not None:
        await aiotime.sleep(handler.initial_delay, wakeup=cause.stopper.async_event)

    # Similar to activities (in-memory execution), but applies patches on every attempt.
    state = progression.State.from_scratch().with_handlers([handler])
    while not stopper.is_set() and not state.done:

        outcomes = await execution.execute_handlers_once(
            lifecycle=lifecycles.all_at_once,  # there is only one anyway
            settings=settings,
            handlers=[handler],
            cause=cause,
            state=state,
        )
        state = state.with_outcomes(outcomes)
        progression.deliver_results(outcomes=outcomes, patch=patch)
        await application.patch_and_check(
            settings=settings,
            resource=resource,
            logger=logger,
            patch=patch,
            body=body,
        )
        patch.clear()

        # The in-memory sleep does not react to resource changes, but only to stopping.
        if state.delay:
            await aiotime.sleep(state.delay, wakeup=cause.stopper.async_event)

    if stopper.is_set():
        logger.debug(f"{handler} has exited on request and will not be retried or restarted.")
    else:
        logger.debug(f"{handler} has exited on its own and will not be retried or restarted.")


async def _timer(
        *,
        settings: configuration.OperatorSettings,
        handler: handlers_.TimerHandler,
        memory: DaemonsMemory,
        cause: causes.DaemonCause,
) -> None:
    """
    A long-running guarding task for resource timer handlers.

    Each individual handler for each individual k8s-object gets its own task.
    Despite asyncio can schedule the delayed execution of the callbacks
    with ``loop.call_later()`` and ``loop.call_at()``, we do not use them:

    * First, the callbacks are synchronous, making it impossible to patch
      the k8s-objects with the returned results of the handlers.

    * Second, our timers are more sophisticated: they track the last-seen time,
      obey the idle delays, and are instantly terminated/cancelled on the object
      deletion or on the operator exit.

    * Third, sharp timing would require an external timestamp storage anyway,
      which is easier to keep as a local variable inside of a function.

    It is hard to implement all of this with native asyncio timers.
    It is much easier to have an extra task which mostly sleeps,
    but calls the handling functions from time to time.
    """
    resource = cause.resource
    stopper = cause.stopper
    logger = cause.logger
    patch = cause.patch
    body = cause.body

    if handler.initial_delay is not None:
        await aiotime.sleep(handler.initial_delay, wakeup=stopper.async_event)

    # Similar to activities (in-memory execution), but applies patches on every attempt.
    state = progression.State.from_scratch().with_handlers([handler])
    while not stopper.is_set():  # NB: ignore state.done! it is checked below explicitly.

        # Reset success/failure retry counters & timers if it has succeeded. Keep it if failed.
        # Every next invocation of a successful handler starts the retries from scratch (from zero).
        if state.done:
            state = progression.State.from_scratch().with_handlers([handler])

        # Both `now` and `last_seen_time` are moving targets: the last seen time is updated
        # on every watch-event received, and prolongs the sleep. The sleep is never shortened.
        if handler.idle is not None:
            while not stopper.is_set() and time.monotonic() - memory.idle_reset_time < handler.idle:
                delay = memory.idle_reset_time + handler.idle - time.monotonic()
                await aiotime.sleep(delay, wakeup=stopper.async_event)
            if stopper.is_set():
                continue

        # Remember the start time for the sharp timing and idle-time-waster below.
        started = time.monotonic()

        # Execute the handler as usually, in-memory, but handle its outcome on every attempt.
        outcomes = await execution.execute_handlers_once(
            lifecycle=lifecycles.all_at_once,  # there is only one anyway
            settings=settings,
            handlers=[handler],
            cause=cause,
            state=state,
        )
        state = state.with_outcomes(outcomes)
        progression.deliver_results(outcomes=outcomes, patch=patch)
        await application.patch_and_check(
            settings=settings,
            resource=resource,
            logger=logger,
            patch=patch,
            body=body,
        )
        patch.clear()

        # For temporary errors, override the schedule by the one provided by errors themselves.
        # It can be either a delay from TemporaryError, or a backoff for an arbitrary exception.
        if not state.done:
            await aiotime.sleep(state.delays, wakeup=stopper.async_event)

        # For sharp timers, calculate how much time is left to fit the interval grid:
        #       |-----|-----|-----|-----|-----|-----|---> (interval=5, sharp=True)
        #       [slow_handler]....[slow_handler]....[slow...
        elif handler.interval is not None and handler.sharp:
            passed_duration = time.monotonic() - started
            remaining_delay = handler.interval - (passed_duration % handler.interval)
            await aiotime.sleep(remaining_delay, wakeup=stopper.async_event)

        # For regular (non-sharp) timers, simply sleep from last exit to the next call:
        #       |-----|-----|-----|-----|-----|-----|---> (interval=5, sharp=False)
        #       [slow_handler].....[slow_handler].....[slow...
        elif handler.interval is not None:
            await aiotime.sleep(handler.interval, wakeup=stopper.async_event)

        # For idle-only no-interval timers, wait till the next change (i.e. idling reset).
        # NB: This will skip the handler in the same tact (1/64th of a second) even if changed.
        elif handler.idle is not None:
            while memory.idle_reset_time <= started:
                await aiotime.sleep(handler.idle, wakeup=stopper.async_event)

        # Only in case there are no intervals and idling, treat it as a one-shot handler.
        # This makes the handler practically meaningless, but technically possible.
        else:
            break
