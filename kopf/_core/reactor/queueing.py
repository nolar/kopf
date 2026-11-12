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
import collections
import contextlib
import enum
import logging
import types
from typing import TYPE_CHECKING, Any, NamedTuple, NewType, Protocol, Self

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.clients import watching
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references
from kopf._core.intents import registries

logger = logging.getLogger(__name__)


# None (by default) signals to perform all activities, so this mode is not declared explicitly here.
# This mode is used only in partial execution phases below, and only in a very limited set of tests.
class ProcessingMode(enum.Enum):
    INDEXING_ONLY = enum.auto()
    HANDLING_ONLY = enum.auto()


class WatchStreamProcessor(Protocol):
    async def __call__(
            self,
            *,
            raw_event: bodies.RawEvent,
            stream_pressure: asyncio.Event | None = None,  # None for tests
            processing_mode: ProcessingMode | None = None,  # None means doing everything
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


UID = NewType('UID', str)


def get_uid(raw_event: bodies.RawEvent) -> UID:
    """
    Retrieve or simulate an identifier of an object unique both in time & space.

    It is used as a key in mappings of framework-internal system resources,
    such as tasks and queues. It is never exposed to the users, even in logs.
    The keys are only persistent during a lifetime of a single process.
    They can be safely changed across different versions.

    In most cases, UIDs are sufficient --- as populated by K8s itself.
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
    return UID(uid)


def get_version(raw_event: bodies.RawEvent | EOS) -> str | None:
    if isinstance(raw_event, EOS):
        return None
    else:
        return raw_event.get('object', {}).get('metadata', {}).get('resourceVersion')


class WorkerPool(contextlib.AbstractAsyncContextManager[Any]):
    def __init__(
            self,
            *,
            settings: configuration.OperatorSettings,
            resource: references.Resource,
            processor: WatchStreamProcessor,
    ) -> None:
        super().__init__()
        self.settings = settings
        self.resource = resource
        self.processor = processor
        self.scheduler = aiotasks.Scheduler(limit=settings.queueing.worker_limit,
                                            exception_handler=self.workers_exception_handler)
        self.watcher_task = asyncio.current_task()
        self.worker_error: BaseException | None = None
        self.signaller = asyncio.Condition()
        self.streams: dict[UID, Stream] = {}

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: types.TracebackType | None,
    ) -> None:

        # Allow the existing workers to finish gracefully before killing them.
        # Ensure the depletion is done even if the watcher is double-cancelled (e.g. in tests).
        depletion_task = asyncio.create_task(self.wait_for_depletion(
            timeout=self.settings.queueing.exit_timeout,
        ))
        while not depletion_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(depletion_task)

        # Terminate all the fire-and-forget per-object jobs if they are still running.
        # Ensure the scheduler is closed even if the watcher is double-cancelled (e.g. in tests).
        closing_task = asyncio.create_task(self.scheduler.close())
        while not closing_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(closing_task)

        # It is CancelledError because we do cancel() in a worker's exception handler below.
        if isinstance(exc_val, asyncio.CancelledError) and self.worker_error is not None:
            raise RuntimeError("Event processing has failed with an unrecoverable error. "
                               "This seems to be a framework bug. "
                               "The operator will stop to prevent damage.") from self.worker_error

    def workers_exception_handler(self, exc: BaseException) -> None:
        # In case of a failed worker, stop the watcher, and escalate to the operator to stop it.
        if self.worker_error is None:
            self.worker_error = exc
            if self.watcher_task is not None:  # never happens, but is needed for type-checking.
                self.watcher_task.cancel()

    async def worker(self, *, uid: UID, mode: ProcessingMode | None) -> None:
        """
        A single worker for a single resource object, running in its own task.

        An object worker consumes the events from the object-dedicated queue
        filled by the watcher of the whole resource kind (i.e. of all objects
        of that kind) and invokes the processor for that specific object.

        The processor is an internal coroutine of the framework, not of a user.
        There are several types of processors involved:

        - For operator peering: monitors the peers; suspends/resumes operations.
        - For cluster observation: spawns new watchers for new CRDs/namespaces.
        - For resource handling: detects changes & calls the user-side handlers.

        The worker is time-limited: it exits as soon as all the object's events
        have been processed and there are no new events for some time of idling
        (a few seconds—to prevent exiting and recreating the workers too often).
        The watcher will spawn a new worker when (and if) new events arrive.
        Such early exiting saves system resources (RAM) in big clusters with low
        activity, since we do not keep a worker task for every dormant object.
        """
        loop = asyncio.get_running_loop()
        backlog = self.streams[uid].backlog
        pressure = self.streams[uid].pressure
        shouldstop = False
        consistency_time: float | None = None  # None if nothing is expected/awaited.
        expected_version: str | None = None  # None/non-None is synced with the patch-end-time.
        try:
            while not shouldstop:

                # Get an event ASAP (no delay) if possible. But expect the queue can be empty.
                # Save memory by finishing the worker if the backlog is empty for some time.
                timeout = max(self.settings.queueing.idle_timeout if mode is None else 0,
                              consistency_time - loop.time() if consistency_time is not None else 0)
                try:
                    if timeout == 0:
                        raw_event = backlog.get_nowait()
                    else:
                        raw_event = await asyncio.wait_for(backlog.get(), timeout=timeout)
                except (TimeoutError, asyncio.QueueEmpty):
                    # Tricky! Under load or with synchronous blocks of asyncio event-loop, it is
                    # possible that the timeout happens while the queue is filled: depending on
                    # the order in which the coro-tasks are checked once control returns to asyncio.
                    # As a workaround, we double-check the queue and exit only if it is truly empty;
                    # if not, run as normally. IMPORTANT: There MUST be NO async/await-code between
                    # "break" and "finally", so that the queue is not populated again.
                    # TODO: LATER: Test the described scenario. I have found no ways to simulate
                    #  a timeout while the queue is filled—neither with pure Python nor with mocks.
                    if backlog.empty():
                        break
                    else:
                        continue

                # Exit gracefully and immediately on the end-of-stream marker sent by the watcher.
                if isinstance(raw_event, EOS):
                    break  # out of the worker.

                # Keep track of the resource's consistency for high-level state-dependent handlers.
                # See `settings.persistence.consistency_timeout` for the explanation of consistency.
                if expected_version is not None and expected_version == get_version(raw_event):
                    expected_version = None
                    consistency_time = None

                # Relieve the pressure only if this is the last event, letting the processor sleep.
                # If there are more events to process, skip the sleep as if they have just arrived.
                if backlog.empty():
                    pressure.clear()

                # Process the event. It might include sleeping till the time of assumed consistency
                # (i.e. ignoring that the patched version was not received & behaving as if it was).
                newer_patch_version = await self.processor(
                    raw_event=raw_event,
                    stream_pressure=pressure,
                    processing_mode=mode,
                    consistency_time=consistency_time,
                )

                # With every new PATCH API call (if done), restart the consistency waiting.
                if newer_patch_version is not None and self.settings.persistence.consistency_timeout:
                    expected_version = newer_patch_version
                    consistency_time = loop.time() + self.settings.persistence.consistency_timeout

        except Exception:
            # Log the error for every worker: there can be several of them failing at the same time,
            # but only one will trigger the watcher's failure -- others could be lost if not logged.
            logger.exception(f"Event processing has failed with an unrecoverable error for {uid}.")
            raise

        finally:
            # On an exception, a break, or a success, notify the caller & garbage-collect our queue.
            # The queue must not be left in the queue dict without a task handling this queue.
            try:
                del self.streams[uid]
            except KeyError:
                pass  # already absent

            # Notify the depletion routine about the changes in the workers'/streams' overall state.
            # * This must happen STRICTLY AFTER the removal from the streams[], and
            # * This must happen A MOMENT BEFORE the job ends, within the scheduler's close_timeout.
            async with self.signaller:
                self.signaller.notify_all()

    async def wait_for_depletion(self, *, timeout: float | None) -> None:

        # Tell all the workers to finish now. Wake them up if they are waiting in the queue-getting.
        # The finalizing workers wil ignore the extra EOS, before they remove their stream anyway.
        for stream in list(self.streams.values()):
            await stream.backlog.put(EOS.token)

        # Wait for the queues to be depleted, but only if there are some workers running.
        # Continue with the tasks termination if the timeout is reached, no matter the queues.
        # NB: the scheduler check is for mocked workers in tests; otherwise, the streams are enough.
        async with self.signaller:
            try:
                await asyncio.wait_for(
                    self.signaller.wait_for(lambda: not self.streams or self.scheduler.empty()),
                    timeout=timeout)
            except TimeoutError:
                pass  # if not depleted as configured, proceed with what's left and let it fail

        # The last check if the termination is going to be graceful or not.
        if self.streams:
            logger.warning(f"Unprocessed streams left for {list(self.streams.keys())!r}.")

    async def spawn_and_feed(
            self,
            raw_event: bodies.RawEvent | watching.Bookmark,
            mode: ProcessingMode | None,
    ) -> None:
        # Whatever is bookmarked there, don't let it go to the multiplexer. Handle it above.
        if isinstance(raw_event, watching.Bookmark):
            return

        # Skip BOOKMARK events from K8s: they carry no object identity (no uid/name/namespace),
        # and are only useful for resource version tracking (already done in the watch stream).
        if raw_event.get('type') == 'BOOKMARK':
            return

        # Multiplex the raw events to per-resource workers/queues. Start the new ones if needed.
        uid = get_uid(raw_event)
        match mode:
            case ProcessingMode.INDEXING_ONLY:
                worker_type = 'initial indexer'
            case ProcessingMode.HANDLING_ONLY:
                worker_type = 'buffered processor'
            case _:
                worker_type = 'worker'
        try:
            # Feed the worker, as fast as possible, no extra activities.
            self.streams[uid].pressure.set()  # interrupt current sleeps, if any.
            await self.streams[uid].backlog.put(raw_event)
        except KeyError:
            # Start the worker, and feed it initially. Starting can be moderately slow.
            self.streams[uid] = Stream(backlog=asyncio.Queue(), pressure=asyncio.Event())
            self.streams[uid].pressure.set()  # interrupt current sleeps, if any.
            await self.streams[uid].backlog.put(raw_event)
            await self.scheduler.spawn(
                name=f'{worker_type} for {uid}',
                coro=self.worker(uid=uid, mode=mode))


async def watcher(
        *,
        namespace: references.Namespace,
        settings: configuration.OperatorSettings,
        registry: registries.OperatorRegistry,
        resource: references.Resource,
        processor: WatchStreamProcessor,
        operator_paused: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        operator_indexed: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        resource_indexed: aiotoggles.Toggle | None = None,  # None for tests & non-indexable
) -> None:
    """
    Watch for the resource events via the API, and spawn the workers per object.

    All resources and objects are done in parallel, but one single object
    is handled sequentially (otherwise, concurrent handling of multiple events
    of the same object could cause data damage).

    The watcher is as non-blocking and async as possible. It does not call any
    external routines, nor does it make the API calls via the sync libraries.

    The watcher is generally a never-ending task (unless an error happens or
    it is cancelled). The workers, on the other hand, are limited approximately
     to the life-time of an object's event.

    Watchers spend their time in the infinite watch stream, not in task waiting.
    The only valid way for a worker to wake up the watcher is to cancel it:
    this will terminate any i/o operation with ``asyncio.CancelledError``, where
    we can make a decision on whether it was a real cancellation, or our own.
    """
    # All per-object workers are handled as fire-and-forget jobs via the scheduler,
    # and communicated via the per-object event queues.
    async with WorkerPool(
        settings=settings,
        resource=resource,
        processor=processor,
    ) as pool:

        # Either use the existing object's queue, or create a new one together with the object task.
        # "Fire-and-forget": do not wait for result; the task destroys itself when it is fully done.
        stream = watching.infinite_watch(
            settings=settings,
            resource=resource, namespace=namespace,
            operator_paused=operator_paused,
        )

        # An ordered buffer for pending events between pre-indexing and post-processing.
        # List vs. deque: same memory size overall, same O(1) on appending.
        # But deque allows draining it with O(1) while we are processing it later.
        initial_events: collections.deque[bodies.RawEvent] = collections.deque()

        # Pre-index the resources of this kind without blocking the scheduler by pending workers.
        # Accumulate the events precisely in the order of arrival for later re-processing.
        # We use the usual scheduler to cap at the worker limit even for indexing.
        if registry._indexing.has_handlers(resource):
            async for raw_event in stream:
                # If the listing is over (even if it was empty), the resource kind is pre-indexed.
                if isinstance(raw_event, watching.Bookmark):
                    break  # out of the indexing phase

                initial_events.append(raw_event)
                await pool.spawn_and_feed(raw_event, mode=ProcessingMode.INDEXING_ONLY)

        # Report this resource kind as indexed asap.
        if operator_indexed is not None and resource_indexed is not None:
            await operator_indexed.drop_toggle(resource_indexed)

        # Deplete the workers bound with mode=INDEX_ONLY, free up all the keys.
        # NB: the tasks can still be running briefly; we care only about streams[] for our controls.
        await pool.wait_for_depletion(timeout=None)

        # Wait for other resource kinds to finish indexing.
        if operator_indexed is not None:
            await operator_indexed.wait_for(True)

        # Now, the whole operator is indexed. Process the accumulated events without re-indexing.
        # Drain the buffer and reduce the memory footprint with O(1) while we process the events.
        while initial_events:
            raw_event = initial_events.popleft()
            await pool.spawn_and_feed(raw_event, mode=ProcessingMode.HANDLING_ONLY)

        # Free up memory from the accumulated initial events asap.
        del initial_events

        # Deplete the workers bound with mode=PROCESS_ONLY, free up all the keys.
        # NB: the tasks can still be running briefly; we care only about streams[] for our controls.
        await pool.wait_for_depletion(timeout=None)

        # Now, stream normally ad infinitum, with indexing & processing combined.
        async for raw_event in stream:
            await pool.spawn_and_feed(raw_event, mode=None)
