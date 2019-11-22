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
The destruction delay (usually few seconds, maybe minutes) is needed
to prevent the often queue/worker destruction and re-creation
in case the events are for any reason delayed by Kubernetes.

The conversion of the low-level watch-events to the high-level causes
is done in the `kopf.reactor.handling` routines.
"""

import asyncio
import enum
import logging
import time
from typing import Tuple, Union, MutableMapping, NewType, NamedTuple, TYPE_CHECKING, cast, Optional

import aiojobs
from typing_extensions import Protocol

from kopf import config
from kopf.clients import watching
from kopf.structs import bodies
from kopf.structs import primitives
from kopf.structs import resources

logger = logging.getLogger(__name__)


class WatcherCallback(Protocol):
    async def __call__(
            self,
            *,
            event: bodies.Event,
            replenished: asyncio.Event,
    ) -> None: ...


# An end-of-stream marker sent from the watcher to the workers.
# See: https://www.python.org/dev/peps/pep-0484/#support-for-singleton-types-in-unions
class EOS(enum.Enum):
    token = enum.auto()


if TYPE_CHECKING:
    WatchEventQueue = asyncio.Queue[Union[bodies.Event, EOS]]
else:
    WatchEventQueue = asyncio.Queue


class Stream(NamedTuple):
    """ A single object's stream of watch-events, with some extra helpers. """
    watchevents: WatchEventQueue
    replenished: asyncio.Event  # means: "hurry up, there are new events queued again"


ObjectUid = NewType('ObjectUid', str)
ObjectRef = Tuple[resources.Resource, ObjectUid]
Streams = MutableMapping[ObjectRef, Stream]


# TODO: add the label_selector support for the dev-mode?
async def watcher(
        namespace: Union[None, str],
        resource: resources.Resource,
        handler: WatcherCallback,
        freeze_mode: Optional[primitives.Toggle] = None,
) -> None:
    """
    The watchers watches for the resource events via the API, and spawns the handlers for every object.

    All resources and objects are done in parallel, but one single object is handled sequentially
    (otherwise, concurrent handling of multiple events of the same object could cause data damage).

    The watcher is as non-blocking and async, as possible. It does neither call any external routines,
    nor it makes the API calls via the sync libraries.

    The watcher is generally a never-ending task (unless an error happens or it is cancelled).
    The workers, on the other hand, are limited approximately to the life-time of an object's event.
    """

    # All per-object workers are handled as fire-and-forget jobs via the scheduler,
    # and communicated via the per-object event queues.
    scheduler = await aiojobs.create_scheduler(limit=config.WorkersConfig.queue_workers_limit)
    streams: Streams = {}
    try:
        # Either use the existing object's queue, or create a new one together with the per-object job.
        # "Fire-and-forget": we do not wait for the result; the job destroys itself when it is fully done.
        stream = watching.infinite_watch(
            resource=resource, namespace=namespace,
            freeze_mode=freeze_mode,
        )
        async for event in stream:
            key = cast(ObjectRef, (resource, event['object']['metadata']['uid']))
            try:
                streams[key].replenished.set()  # interrupt current sleeps, if any.
                await streams[key].watchevents.put(event)
            except KeyError:
                streams[key] = Stream(watchevents=asyncio.Queue(), replenished=asyncio.Event())
                streams[key].replenished.set()  # interrupt current sleeps, if any.
                await streams[key].watchevents.put(event)
                await scheduler.spawn(worker(handler=handler, streams=streams, key=key))
    finally:
        # Allow the existing workers to finish gracefully before killing them.
        await _wait_for_depletion(scheduler=scheduler, streams=streams)

        # Forcedly terminate all the fire-and-forget per-object jobs, of they are still running.
        await asyncio.shield(scheduler.close())


async def worker(
        handler: WatcherCallback,
        streams: Streams,
        key: ObjectRef,
) -> None:
    """
    The per-object workers consume the object's events and invoke the handler.

    The handler is expected to be an async coroutine, always the one from the framework.
    In fact, it is either a peering handler, which monitors the peer operators,
    or a generic resource handler, which internally calls the registered synchronous handlers.

    The per-object worker is a time-limited task, which ends as soon as all the object's events
    have been handled. The watcher will spawn a new job when and if the new events arrive.

    To prevent the queue/job deletion and re-creation to happen too often, the jobs wait some
    reasonable, but small enough time (few seconds) before actually finishing --
    in case the new events are there, but the API or the watcher task lags a bit.
    """
    watchevents = streams[key].watchevents
    replenished = streams[key].replenished
    shouldstop = False
    try:
        while not shouldstop:

            # Try ASAP, but give it few seconds for the new events to arrive, maybe.
            # If the queue is empty for some time, then indeed finish the object's worker.
            # If the queue is filled, use the latest event only (within the short timeframe).
            # If an EOS marker is received, handle the last real event, then finish the worker ASAP.
            try:
                event = await asyncio.wait_for(
                    watchevents.get(),
                    timeout=config.WorkersConfig.worker_idle_timeout)
            except asyncio.TimeoutError:
                break
            else:
                try:
                    while True:
                        prev_event = event
                        next_event = await asyncio.wait_for(
                            watchevents.get(),
                            timeout=config.WorkersConfig.worker_batch_window)
                        shouldstop = shouldstop or isinstance(next_event, EOS)
                        event = prev_event if isinstance(next_event, EOS) else next_event
                except asyncio.TimeoutError:
                    pass

            # Exit gracefully and immediately on the end-of-stream marker sent by the watcher.
            if isinstance(event, EOS):
                break

            # Try the handler. In case of errors, show the error, but continue the queue processing.
            replenished.clear()
            try:
                await handler(event=event, replenished=replenished)
            except Exception as e:
                # TODO: handler is a functools.partial. make the prints a bit nicer by removing it.
                logger.exception(f"{handler} failed with an exception. Ignoring the event.")
                # raise

    finally:
        # Whether an exception or a break or a success, notify the caller, and garbage-collect our queue.
        # The queue must not be left in the queue-cache without a corresponding job handling this queue.
        try:
            del streams[key]
        except KeyError:
            pass


async def _wait_for_depletion(
        *,
        scheduler: aiojobs.Scheduler,
        streams: Streams,
) -> None:

    # Notify all the workers to finish now. Wake them up if they are waiting in the queue-getting.
    for stream in streams.values():
        await stream.watchevents.put(EOS.token)

    # Wait for the queues to be depleted, but only if there are some workers running.
    # Continue with the tasks termination if the timeout is reached, no matter the queues.
    started = time.perf_counter()
    while streams and \
            scheduler.active_count and \
            time.perf_counter() - started < config.WorkersConfig.worker_exit_timeout:
        await asyncio.sleep(config.WorkersConfig.worker_exit_timeout / 100.)

    # The last check if the termination is going to be graceful or not.
    if streams:
        logger.warning("Unprocessed streams left for %r.", list(streams.keys()))
