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
from typing import Optional, Callable, Tuple, Union, MutableMapping, NewType

import aiojobs
import kubernetes.watch

from kopf.reactor.handling import custom_object_handler
from kopf.reactor.lifecycles import get_default_lifecycle
from kopf.reactor.peering import PEERING_CRD_RESOURCE, PEERING_DEFAULT_NAME
from kopf.reactor.peering import peers_keepalive, peers_handler, Peer, detect_own_id
from kopf.reactor.registry import get_default_registry, BaseRegistry, Resource
from kopf.reactor.watching import streaming_aiter

logger = logging.getLogger(__name__)

ObjectUid = NewType('ObjectUid', str)
ObjectRef = Tuple[Resource, ObjectUid]
Queues = MutableMapping[ObjectRef, asyncio.Queue]


# TODO: add the label_selector support for the dev-mode?
async def watcher(
        namespace: Union[None, str],
        resource: Resource,
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
    scheduler = await aiojobs.create_scheduler(limit=10)
    queues = {}
    try:
        while True:

            # Make a Kubernetes call to watch for the events via the API.
            w = kubernetes.watch.Watch()
            api = kubernetes.client.CustomObjectsApi()
            api_fn = api.list_cluster_custom_object
            stream = w.stream(api_fn, resource.group, resource.version, resource.plural)
            async for event in streaming_aiter(stream):

                # "410 Gone" is for the "resource version too old" error, we must restart watching.
                # The resource versions are lost by k8s after few minutes (as per the official doc).
                # The error occurs when there is nothing happening for few minutes. This is normal.
                if event['type'] == 'ERROR' and event['object']['code'] == 410:
                    logger.debug("Restarting the watch-stream for %r", resource)
                    break  # out of for-cycle, to the while-true-cycle.

                # Other watch errors should be fatal for the operator.
                if event['type'] == 'ERROR':
                    raise Exception(f"Error in the watch-stream: {event['object']}")

                # Ensure that the event is something we understand and can handle.
                if event['type'] not in ['ADDED', 'MODIFIED', 'DELETED']:
                    logger.warning("Ignoring an unsupported event type: %r", event)
                    continue

                # Filter out all unrelated events as soon as possible (before queues), and silently.
                # TODO: Reimplement via api.list_namespaced_custom_object, and API-level filtering.
                ns = event['object'].get('metadata', {}).get('namespace', None)
                if namespace is not None and ns is not None and ns != namespace:
                    continue

                # Either use the existing object's queue, or create a new one together with the per-object job.
                # "Fire-and-forget": we do not wait for the result; the job destroys itself when it is fully done.
                key = (resource, event['object']['metadata']['uid'])
                try:
                    await queues[key].put(event)
                except KeyError:
                    queues[key] = asyncio.Queue()
                    await queues[key].put(event)
                    await scheduler.spawn(worker(handler=handler, queues=queues, key=key))

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
    try:
        while True:

            # Try ASAP, but give it few seconds for the new events to arrive, maybe.
            # If the queue is empty for some time, then indeed finish the object's worker.
            # If the queue is filled, use the latest event only (within the short timeframe).
            try:
                event = await asyncio.wait_for(queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            else:
                try:
                    while True:
                        event = await asyncio.wait_for(queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    pass

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
        lifecycle: Optional[Callable] = None,
        registry: Optional[BaseRegistry] = None,
        standalone: bool = False,
        priority: int = 0,
        peering: str = PEERING_DEFAULT_NAME,
        namespace: Optional[str] = None,
):
    """
    Create all the tasks needed to run the operator, but do not spawn/start them.
    The tasks are properly inter-connected depending on the runtime specification.
    They can be injected into any event loop as needed.
    """

    # The freezer and the registry are scoped to this whole task-set, to sync them all.
    lifecycle = lifecycle if lifecycle is not None else get_default_lifecycle()
    registry = registry if registry is not None else get_default_registry()
    freeze = asyncio.Event()
    tasks = []

    # Monitor the peers, unless explicitly disabled.
    ourselves: Optional[Peer] = Peer.detect(standalone, peering, id=detect_own_id(), priority=priority, namespace=namespace)
    if ourselves:
        tasks.extend([
            asyncio.Task(peers_keepalive(ourselves=ourselves)),
            asyncio.Task(watcher(namespace=None,  # peering is cluster-object
                                 resource=PEERING_CRD_RESOURCE,
                                 handler=functools.partial(peers_handler,
                                                           ourselves=ourselves,
                                                           freeze=freeze))),  # freeze is set/cleared
        ])

    # Resource event handling, only once for every known resource (de-duplicated).
    for resource in registry.resources:
        tasks.extend([
            asyncio.Task(watcher(namespace=namespace,
                                 resource=resource,
                                 handler=functools.partial(custom_object_handler,
                                                           lifecycle=lifecycle,
                                                           registry=registry,
                                                           resource=resource,
                                                           freeze=freeze))),  # freeze is only checked
        ])

    return tasks


def run(
        lifecycle: Optional[Callable] = None,
        registry: Optional[BaseRegistry] = None,
        standalone: bool = False,
        priority: int = 0,
        loop: Optional[asyncio.BaseEventLoop] = None,
        peering: str = PEERING_DEFAULT_NAME,
        namespace: Optional[str] = None,
):
    """
    Serve the events for all the registered resources and handlers.

    This process typically never ends, unless an unhandled error happens
    in one of the consumers/producers.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    tasks = create_tasks(
        lifecycle=lifecycle,
        registry=registry,
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering=peering,
    )

    # Run the presumably infinite tasks until one of them fails (they never exit normally).
    done, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))

    # Allow the remaining tasks to handle the cancellation before re-raising (e.g. via try-finally).
    # The errors in the cancellation stage will be ignored anyway (never re-raised below).
    for task in pending:
        task.cancel()
    cancelled, pending = loop.run_until_complete(asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED))
    assert not pending  # must be empty by now, the tasks are either done or cancelled.

    # Check the results of the non-cancelled tasks, and re-raise of there were any exceptions.
    # The cancelled tasks are not re-raised, as it is a normal flow for the "first-completed" run.
    # TODO: raise all of the cancelled+done, if there were 2+ failed ones.
    for task in cancelled:
        try:
            task.result()  # can raise the regular (non-cancellation) exceptions.
        except asyncio.CancelledError:
            pass
    for task in done:
        task.result()
