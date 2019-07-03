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
import functools
import logging
import time
from typing import Optional, Callable, Tuple, Union, MutableMapping, NewType

import aiojobs

from kopf import config
from kopf.clients import watching
from kopf.engines import peering
from kopf.engines import posting
from kopf.reactor import handling
from kopf.reactor import lifecycles
from kopf.reactor import registries

logger = logging.getLogger(__name__)

ObjectUid = NewType('ObjectUid', str)
ObjectRef = Tuple[registries.Resource, ObjectUid]
Queues = MutableMapping[ObjectRef, asyncio.Queue]

EOS = object()
""" An end-of-stream marker sent from the watcher to the workers. """


# TODO: add the label_selector support for the dev-mode?
async def watcher(
        namespace: Union[None, str],
        resource: registries.Resource,
        handler: Callable,
):
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
    queues = {}
    try:
        # Either use the existing object's queue, or create a new one together with the per-object job.
        # "Fire-and-forget": we do not wait for the result; the job destroys itself when it is fully done.
        async for event in watching.infinite_watch(resource=resource, namespace=namespace):
            key = (resource, event['object']['metadata']['uid'])
            try:
                await queues[key].put(event)
            except KeyError:
                queues[key] = asyncio.Queue()
                await queues[key].put(event)
                await scheduler.spawn(worker(handler=handler, queues=queues, key=key))

        # Allow the existing workers to finish gracefully before killing them.
        await _wait_for_depletion(scheduler=scheduler, queues=queues)

    finally:
        # Forcedly terminate all the fire-and-forget per-object jobs, of they are still running.
        await scheduler.close()


async def worker(
        handler: Callable,
        queues: Queues,
        key: ObjectRef,
):
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
    queue = queues[key]
    shouldstop = False
    try:
        while not shouldstop:

            # Try ASAP, but give it few seconds for the new events to arrive, maybe.
            # If the queue is empty for some time, then indeed finish the object's worker.
            # If the queue is filled, use the latest event only (within the short timeframe).
            # If an EOS marker is received, handle the last real event, then finish the worker ASAP.
            try:
                event = await asyncio.wait_for(queue.get(), timeout=config.WorkersConfig.worker_idle_timeout)
            except asyncio.TimeoutError:
                break
            else:
                try:
                    while True:
                        prev_event = event
                        next_event = await asyncio.wait_for(
                            queue.get(), timeout=config.WorkersConfig.worker_batch_window
                        )
                        shouldstop = shouldstop or next_event is EOS
                        event = prev_event if next_event is EOS else next_event
                except asyncio.TimeoutError:
                    pass

            # Exit gracefully and immediately on the end-of-stream marker sent by the watcher.
            if event is EOS:
                break

            # Try the handler. In case of errors, show the error, but continue the queue processing.
            try:
                await handler(event=event)
            except Exception as e:
                # TODO: handler is a functools.partial. make the prints a bit nicer by removing it.
                logger.exception(f"{handler} failed with an exception. Ignoring the event.")
                # raise

    finally:
        # Whether an exception or a break or a success, notify the caller, and garbage-collect our queue.
        # The queue must not be left in the queue-cache without a corresponding job handling this queue.
        try:
            del queues[key]
        except KeyError:
            pass


def create_tasks(
        loop: asyncio.AbstractEventLoop,
        lifecycle: Optional[Callable] = None,
        registry: Optional[registries.BaseRegistry] = None,
        standalone: bool = False,
        priority: int = 0,
        peering_name: str = peering.PEERING_DEFAULT_NAME,
        namespace: Optional[str] = None,
):
    """
    Create all the tasks needed to run the operator, but do not spawn/start them.
    The tasks are properly inter-connected depending on the runtime specification.
    They can be injected into any event loop as needed.
    """

    # The freezer and the registry are scoped to this whole task-set, to sync them all.
    lifecycle = lifecycle if lifecycle is not None else lifecycles.get_default_lifecycle()
    registry = registry if registry is not None else registries.get_default_registry()
    event_queue = asyncio.Queue()
    freeze = asyncio.Event()
    tasks = []

    # K8s-event posting. Events are queued in-memory and posted in the background.
    # NB: currently, it is a global task, but can be made per-resource or per-object.
    tasks.extend([
        loop.create_task(posting.poster(
            event_queue=event_queue)),
    ])

    # Monitor the peers, unless explicitly disabled.
    ourselves: Optional[peering.Peer] = peering.Peer.detect(
        id=peering.detect_own_id(), priority=priority,
        standalone=standalone, namespace=namespace, name=peering_name,
    )
    if ourselves:
        tasks.extend([
            loop.create_task(peering.peers_keepalive(
                ourselves=ourselves)),
            loop.create_task(watcher(
                namespace=namespace,
                resource=ourselves.resource,
                handler=functools.partial(peering.peers_handler,
                                          ourselves=ourselves,
                                          freeze=freeze))),  # freeze is set/cleared
        ])

    # Resource event handling, only once for every known resource (de-duplicated).
    for resource in registry.resources:
        tasks.extend([
            loop.create_task(watcher(
                namespace=namespace,
                resource=resource,
                handler=functools.partial(handling.custom_object_handler,
                                          lifecycle=lifecycle,
                                          registry=registry,
                                          resource=resource,
                                          event_queue=event_queue,
                                          freeze=freeze))),  # freeze is only checked
        ])

    return tasks


def run(
        loop: Optional[asyncio.AbstractEventLoop] = None,
        lifecycle: Optional[Callable] = None,
        registry: Optional[registries.BaseRegistry] = None,
        standalone: bool = False,
        priority: int = 0,
        peering_name: str = peering.PEERING_DEFAULT_NAME,
        namespace: Optional[str] = None,
):
    """
    Serve the events for all the registered resources and handlers.

    This process typically never ends, unless an unhandled error happens
    in one of the consumers/producers.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    tasks = create_tasks(
        loop=loop,
        lifecycle=lifecycle,
        registry=registry,
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering_name=peering_name,
    )

    # Run the presumably infinite tasks until one of them fails (they never exit normally).
    try:
        done, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))
    except asyncio.CancelledError:
        done, pending = [], tasks

    # Allow the remaining tasks to handle the cancellation before re-raising (e.g. via try-finally).
    # The errors in the cancellation stage will be ignored anyway (never re-raised below).
    for task in pending:
        task.cancel()
    if pending:
        cancelled, pending = loop.run_until_complete(asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED))
        assert not pending  # must be empty by now, the tasks are either done or cancelled.
    else:
        # when pending list is empty, let's say cancelled is empty too
        cancelled = []

    # Check the results of the non-cancelled tasks, and re-raise of there were any exceptions.
    # The cancelled tasks are not re-raised, as it is a normal flow for the "first-completed" run.
    # TODO: raise all of the cancelled+done, if there were 2+ failed ones.
    for task in list(cancelled) + list(done):
        try:
            task.result()  # can raise the regular (non-cancellation) exceptions.
        except asyncio.CancelledError:
            pass


async def _wait_for_depletion(*, scheduler, queues):

    # Notify all the workers to finish now. Wake them up if they are waiting in the queue-getting.
    for queue in queues.values():
        await queue.put(EOS)

    # Wait for the queues to be depleted, but only if there are some workers running.
    # Continue with the tasks termination if the timeout is reached, no matter the queues.
    started = time.perf_counter()
    while queues and \
            scheduler.active_count and \
            time.perf_counter() - started < config.WorkersConfig.worker_exit_timeout:
        await asyncio.sleep(config.WorkersConfig.worker_exit_timeout / 100.)

    # The last check if the termination is going to be graceful or not.
    if queues:
        logger.warning("Unprocessed queues left for %r.", list(queues.keys()))
