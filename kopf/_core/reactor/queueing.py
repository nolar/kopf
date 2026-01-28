"""
Kubernetes watching/streaming and the per-object queueing system.

The framework can handle multiple resources at once.
Every custom resource type is "watched" (as in ``kubectl get --watch``)
in a separate asyncio task in the never-ending loop.

The events for this resource type (of all its objects) are then pushed
to the per-object queues, which are created and destroyed dynamically.
The per-object queues are created on demand.

Every object is identified by its uid, and is handled sequentially:
i.e. the low-level events are processed in the order of their arrival.
Other objects are handled in parallel in their own sequential tasks.

To prevent the memory leaks over the long run, the queues and the workers
of each object are destroyed if no new events arrive for some time.
The destruction delay (usually a few seconds, maybe minutes) is needed
to prevent the often queue/worker destruction and re-creation
in case the events are for any reason delayed by Kubernetes.

The conversion of the low-level watch-events to the high-level causes
is done in the :mod:`kopf._core.reactor.processing` routines.
"""
import asyncio
import contextlib
import enum
import logging
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, NamedTuple, NewType, Protocol

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.clients import watching
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references

logger = logging.getLogger(__name__)


class WatchStreamProcessor(Protocol):
    async def __call__(
            self,
            *,
            raw_event: bodies.RawEvent,
            stream_pressure: asyncio.Event | None = None,  # None for tests
            resource_indexed: aiotoggles.Toggle | None = None,  # None for tests & observation
            operator_indexed: aiotoggles.ToggleSet | None = None,  # None for tests & observation
            consistency_time: float | None = None,  # None for tests
    ) -> str | None:  # patched resource version, if patched
        ...


# An end-of-stream marker sent from the watcher to the workers.
# See: https://www.python.org/dev/peps/pep-0484/#support-for-singleton-types-in-unions
class EOS(enum.Enum):
    token = enum.auto()


if TYPE_CHECKING:
    WatchEventQueue = asyncio.Queue[bodies.RawEvent | EOS]
else:
    WatchEventQueue = asyncio.Queue


class Stream(NamedTuple):
    """ A single object's stream of watch-events, with some extra helpers. """
    backlog: WatchEventQueue
    pressure: asyncio.Event  # means: "hurry up, there are new events queued again"


ObjectUid = NewType('ObjectUid', str)
ObjectRef = tuple[references.Resource, ObjectUid]
Streams = MutableMapping[ObjectRef, Stream]


def get_uid(raw_event: bodies.RawEvent) -> ObjectUid:
    """
    Retrieve or simulate an identifier of an object unique both in time & space.

    It is used as a key in mappings of framework-internal system resources,
    such as tasks and queues. It is never exposed to the users, even in logs.
    The keys are only persistent during a lifetime of a single process.
    They can be safely changed across different versions.

    In most cases, UIDs are sufficient -- as populated by K8s itself.
    However, some resources have no UIDs: e.g. ``v1/ComponentStatus``:

    .. code-block:: yaml

        apiVersion: v1
        kind: ComponentStatus
        metadata:
          creationTimestamp: null
          name: controller-manager
          selfLink: /api/v1/componentstatuses/controller-manager
        conditions:
        - message: ok
          status: "True"
          type: Healthy

    Note that ``selfLink`` is deprecated and will stop being populated
    since K8s 1.20. Other fields are not always sufficient to ensure uniqueness
    both in space and time: in the example above, the creation time is absent.

    In this function, we do our best to provide a fallback scenario in case
    UIDs are absent. All in all, having slightly less unique identifiers
    is better than failing the whole resource handling completely.
    """
    if 'uid' in raw_event['object']['metadata']:
        uid = raw_event['object']['metadata']['uid']
    else:
        ids = [
            raw_event['object'].get('kind'),
            raw_event['object'].get('apiVersion'),
            raw_event['object']['metadata'].get('name'),
            raw_event['object']['metadata'].get('namespace'),
            raw_event['object']['metadata'].get('creationTimestamp'),
        ]
        uid = '//'.join([s or '-' for s in ids])
    return ObjectUid(uid)


def get_version(raw_event: bodies.RawEvent | EOS) -> str | None:
    if isinstance(raw_event, EOS):
        return None
    else:
        return raw_event.get('object', {}).get('metadata', {}).get('resourceVersion')


async def watcher(
        *,
        namespace: references.Namespace,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        processor: WatchStreamProcessor,
        operator_paused: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        operator_indexed: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        resource_indexed: aiotoggles.Toggle | None = None,  # None for tests & non-indexable
) -> None:
    """
    The watchers watches for the resource events via the API, and spawns the workers for every object.

    All resources and objects are done in parallel, but one single object is handled sequentially
    (otherwise, concurrent handling of multiple events of the same object could cause data damage).

    The watcher is as non-blocking and async, as possible. It does neither call any external routines,
    nor it makes the API calls via the sync libraries.

    The watcher is generally a never-ending task (unless an error happens or it is cancelled).
    The workers, on the other hand, are limited approximately to the life-time of an object's event.

    Watchers spend their time in the infinite watch stream, not in task waiting.
    The only valid way for a worker to wake up the watcher is to cancel it:
    this will terminate any i/o operation with ``asyncio.CancelledError``, where
    we can make a decision on whether it was a real cancellation, or our own.
    """

    # In case of a failed worker, stop the watcher, and escalate to the operator to stop it.
    watcher_task = asyncio.current_task()
    worker_error: BaseException | None = None
    def exception_handler(exc: BaseException) -> None:
        nonlocal worker_error, watcher_task
        if worker_error is None:
            worker_error = exc
            if watcher_task is not None:  # never happens, but is needed for type-checking.
                watcher_task.cancel()

    # All per-object workers are handled as fire-and-forget jobs via the scheduler,
    # and communicated via the per-object event queues.
    signaller = asyncio.Condition()
    scheduler = aiotasks.Scheduler(limit=settings.queueing.worker_limit,
                                   exception_handler=exception_handler)
    streams: Streams = {}

    try:
        # Either use the existing object's queue, or create a new one together with the per-object job.
        # "Fire-and-forget": we do not wait for the result; the job destroys itself when it is fully done.
        stream = watching.infinite_watch(
            settings=settings,
            resource=resource, namespace=namespace,
            operator_paused=operator_paused,
        )
        async for raw_event in stream:

            # If the listing is over (even if it was empty), the resource kind is pre-indexed.
            # At this moment, only the individual workers/processors can block the global readiness.
            if raw_event is watching.Bookmark.LISTED:
                if operator_indexed is not None and resource_indexed is not None:
                    await operator_indexed.drop_toggle(resource_indexed)

            # Whatever is bookmarked there, don't let it go to the multiplexer. Handle it above.
            if isinstance(raw_event, watching.Bookmark):
                continue

            # Multiplex the raw events to per-resource workers/queues. Start the new ones if needed.
            key: ObjectRef = (resource, get_uid(raw_event))
            try:
                # Feed the worker, as fast as possible, no extra activities.
                streams[key].pressure.set()  # interrupt current sleeps, if any.
                await streams[key].backlog.put(raw_event)
            except KeyError:

                # Block the operator's readiness for individual resource's index handlers.
                # But NOT when the readiness is already achieved once! After that, ignore it.
                # NB: Strictly before the worker starts -- the processor can be too slow, too late.
                resource_object_indexed: aiotoggles.Toggle | None = None
                if operator_indexed is not None and operator_indexed.is_on():
                    operator_indexed = None
                if operator_indexed is not None and resource_indexed is not None:
                    resource_object_indexed = await operator_indexed.make_toggle(name=f"{key!r}")

                # Start the worker, and feed it initially. Starting can be moderately slow.
                streams[key] = Stream(backlog=asyncio.Queue(), pressure=asyncio.Event())
                streams[key].pressure.set()  # interrupt current sleeps, if any.
                await streams[key].backlog.put(raw_event)
                await scheduler.spawn(
                    name=f'worker for {key}',
                    coro=worker(
                        signaller=signaller,
                        resource_indexed=resource_object_indexed,
                        operator_indexed=operator_indexed,
                        processor=processor,
                        settings=settings,
                        streams=streams,
                        key=key,
                    ))

    except asyncio.CancelledError:
        if worker_error is None:
            raise
        else:
            raise RuntimeError("Event processing has failed with an unrecoverable error. "
                               "This seems to be a framework bug. "
                               "The operator will stop to prevent damage.") from worker_error
    finally:
        # Allow the existing workers to finish gracefully before killing them.
        # Ensure the depletion is done even if the watcher is double-cancelled (e.g. in tests).
        depletion_task = asyncio.create_task(_wait_for_depletion(
            signaller=signaller,
            scheduler=scheduler,
            streams=streams,
            settings=settings,
        ))
        while not depletion_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(depletion_task)

        # Terminate all the fire-and-forget per-object jobs if they are still running.
        # Ensure the scheduler is closed even if the watcher is double-cancelled (e.g. in tests).
        closing_task = asyncio.create_task(scheduler.close())
        while not closing_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(closing_task)


async def worker(
        *,
        signaller: asyncio.Condition,
        settings: configuration.OperatorSettings,
        processor: WatchStreamProcessor,
        resource_indexed: aiotoggles.Toggle | None = None,  # None for tests & observation
        operator_indexed: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        streams: Streams,
        key: ObjectRef,
) -> None:
    """
    A single worker for a single resource object, each running in its own task.

    An object worker consumes the events from the object-dedicated queue filled
    by the watcher of the whole resource kind (i.e. of all objects of that kind)
    and invokes the processor (which implies handlers) for that specific object.

    The processor is an internal coroutine of the framework, not of the user.
    There are several types of processors involved:

    - For operator peering: monitors the peers and suspends/resumes operations.
    - For cluster observation: spawns new watchers for new CRDs/namespaces.
    - For resource handling: detects changes and calls the user-side handlers.

    The worker is time-limited: it exits as soon as all the object's events
    have been processed and there are no new events for some time of idling
    (a few seconds -- to prevent exiting and recreating the workers too often).
    The watcher will spawn a new worker when (and if) new events arrive.
    Such early exiting saves system resources (RAM) on large clusters with low
    activity, since we do not keep a running worker for every dormant object.
    """
    loop = asyncio.get_running_loop()
    backlog = streams[key].backlog
    pressure = streams[key].pressure
    shouldstop = False
    consistency_time: float | None = None  # None if nothing is expected/awaited.
    expected_version: str | None = None  # None/non-None is synced with the patch-end-time.
    try:
        while not shouldstop:

            # Get an event ASAP (no delay) if possible. But expect the queue can be empty.
            # Save memory by finishing the worker if the backlog is empty for some time.
            timeout = max(settings.queueing.idle_timeout,
                          consistency_time - loop.time() if consistency_time is not None else 0)
            try:
                raw_event = await asyncio.wait_for(backlog.get(), timeout=timeout)
            except asyncio.TimeoutError:
                # A tricky part! Under high-load or with synchronous blocks of asyncio event-loop,
                # it is possible that the timeout happens while the queue is filled: depending on
                # the order in which the coros/waiters are checked once control returns to asyncio.
                # As a work-around, we double-check the queue and exit only if it is truly empty;
                # if not, run as normally. IMPORTANT: There MUST be NO async/await-code between
                # "break" and "finally", so that the queue is not populated again.
                # TODO: LATER: Test the described scenario. I have found no ways to simulate
                #  a timeout while the queue is filled -- neither with pure Python nor with mocks.
                if backlog.empty():
                    break
                else:
                    continue

            # Exit gracefully and immediately on the end-of-stream marker sent by the watcher.
            if isinstance(raw_event, EOS):
                break  # out of the worker.

            # Keep track of the resource's consistency for high-level (state-dependent) handlers.
            # See `settings.persistence.consistency_timeout` for the explanation of consistency.
            if expected_version is not None and expected_version == get_version(raw_event):
                expected_version = None
                consistency_time = None

            # Relieve the pressure only if this is the last event, thus letting the processor sleep.
            # If there are more events to process, fast-skip the sleep as if they have just arrived.
            if backlog.empty():
                pressure.clear()

            # Process the event. It might include sleeping till the time of consistency assumption
            # (i.e. ignoring that the patched version was not received and behaving as if it was).
            newer_patch_version = await processor(
                raw_event=raw_event,
                stream_pressure=pressure,
                resource_indexed=resource_indexed,
                operator_indexed=operator_indexed,
                consistency_time=consistency_time,
            )

            # With every new PATCH API call (if done), restart the consistency waiting.
            if newer_patch_version is not None and settings.persistence.consistency_timeout:
                expected_version = newer_patch_version
                consistency_time = loop.time() + settings.persistence.consistency_timeout

    except Exception:
        # Log the error for every worker: there can be several of them failing at the same time,
        # but only one will trigger the watcher's failure -- others could be lost if not logged.
        logger.exception(f"Event processing has failed with an unrecoverable error for {key}.")
        raise

    finally:
        # Whether an exception or a break or a success, notify the caller, and garbage-collect our queue.
        # The queue must not be left in the queue-cache without a corresponding job handling this queue.
        try:
            del streams[key]
        except KeyError:
            pass  # already absent

        # Notify the depletion routine about the changes in the workers'/streams' overall state.
        # * This should happen STRICTLY AFTER the removal from the streams[], and
        # * This should happen A MOMENT BEFORE the job ends (within the scheduler's close_timeout).
        async with signaller:
            signaller.notify_all()


async def _wait_for_depletion(
        *,
        signaller: asyncio.Condition,
        scheduler: aiotasks.Scheduler,
        settings: configuration.OperatorSettings,
        streams: Streams,
) -> None:

    # Notify all the workers to finish now. Wake them up if they are waiting in the queue-getting.
    for stream in streams.values():
        await stream.backlog.put(EOS.token)

    # Wait for the queues to be depleted, but only if there are some workers running.
    # Continue with the tasks termination if the timeout is reached, no matter the queues.
    # NB: the scheduler is checked for a case of mocked workers; otherwise, the streams are enough.
    async with signaller:
        try:
            await asyncio.wait_for(
                signaller.wait_for(lambda: not streams or scheduler.empty()),
                timeout=settings.queueing.exit_timeout)
        except asyncio.TimeoutError:
            pass  # if not depleted as configured, proceed with what's left and let it fail

    # The last check if the termination is going to be graceful or not.
    if streams:
        logger.warning(f"Unprocessed streams left for {list(streams.keys())!r}.")
