import asyncio
import functools
import logging
import signal
import threading
from typing import Optional, Callable

from kopf.engines import peering
from kopf.engines import posting
from kopf.reactor import handling
from kopf.reactor import lifecycles
from kopf.reactor import queueing
from kopf.reactor import registries

logger = logging.getLogger(__name__)


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

    # Run the infinite tasks until one of them fails/exits (they never exit normally).
    # Give some time for the remaining tasks to handle the cancellations (e.g. via try-finally).
    done1, pending1 = _wait_gracefully(loop, tasks, return_when=asyncio.FIRST_COMPLETED)
    done2, pending2 = _wait_cancelled(loop, pending1)
    done3, pending3 = _wait_gracefully(loop, asyncio.all_tasks(loop), timeout=1.0)
    done4, pending4 = _wait_cancelled(loop, pending3)

    # Check the results of the non-cancelled tasks, and re-raise of there were any exceptions.
    # The cancelled tasks are not re-raised, as it is a normal flow.
    _reraise(loop, list(done1) + list(done2) + list(done3) + list(done4))


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
    event_queue = asyncio.Queue(loop=loop)
    freeze_flag = asyncio.Event(loop=loop)
    should_stop = asyncio.Event(loop=loop)
    tasks = []

    # A top-level task for external stopping by setting a stop-flag. Once set,
    # this task will exit, and thus all other top-level tasks will be cancelled.
    tasks.extend([
        loop.create_task(_stop_flag_checker(should_stop)),
    ])

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
            loop.create_task(queueing.watcher(
                namespace=namespace,
                resource=ourselves.resource,
                handler=functools.partial(peering.peers_handler,
                                          ourselves=ourselves,
                                          freeze=freeze_flag))),  # freeze is set/cleared
        ])

    # Resource event handling, only once for every known resource (de-duplicated).
    for resource in registry.resources:
        tasks.extend([
            loop.create_task(queueing.watcher(
                namespace=namespace,
                resource=resource,
                handler=functools.partial(handling.custom_object_handler,
                                          lifecycle=lifecycle,
                                          registry=registry,
                                          resource=resource,
                                          event_queue=event_queue,
                                          freeze=freeze_flag))),  # freeze is only checked
        ])

    # On Ctrl+C or pod termination, cancel all tasks gracefully.
    if threading.current_thread() is threading.main_thread():
        loop.add_signal_handler(signal.SIGINT, should_stop.set)
        loop.add_signal_handler(signal.SIGTERM, should_stop.set)
    else:
        logger.warning("OS signals are ignored: running not in the main thread.")

    return tasks


def _wait_gracefully(loop, tasks, *, timeout=None, return_when=asyncio.ALL_COMPLETED):
    if not tasks:
        return [], []
    try:
        done, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=return_when, timeout=timeout))
    except asyncio.CancelledError:
        # ``asyncio.wait()`` is cancelled, but the tasks can be running.
        done, pending = [], tasks
    return done, pending


def _wait_cancelled(loop, tasks, *, timeout=None):
    for task in tasks:
        task.cancel()
    if tasks:
        done, pending = loop.run_until_complete(asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=timeout))
        assert not pending
        return done, pending
    else:
        return [], []


def _reraise(loop, tasks):
    for task in tasks:
        try:
            task.result()  # can raise the regular (non-cancellation) exceptions.
        except asyncio.CancelledError:
            pass


async def _stop_flag_checker(should_stop):
    try:
        await should_stop.wait()
    except asyncio.CancelledError:
        pass  # operator is stopping for any other reason
    else:
        logger.debug("Stop-flag is raised. Operator is stopping.")
