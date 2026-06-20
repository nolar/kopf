"""
All the functions to write the Kubernetes events for the Kubernetes objects.

They are used internally in the handling routines to show the progress,
and can be used directly from the handlers to add arbitrary custom events.

The actual k8s-event posting runs in the background,
and posts the k8s-events as soon as they are queued.

The k8s-events are queued in two ways:

* Explicit calls to event-posting functions :func:`kopf.event`,
  :func:`kopf.info`, :func:`kopf.warn`, :func:`kopf.exception`.
* Logging messages made on the object logger (above the INFO level by default).

This also includes all logging messages posted by the framework itself.
"""
import asyncio
import contextlib
import logging
import sys
from collections.abc import Hashable, Iterable, Iterator
from contextvars import ContextVar
from typing import TYPE_CHECKING, NamedTuple, NoReturn, cast

from kopf._cogs.aiokits import aiotasks
from kopf._cogs.clients import events
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, dicts, references
from kopf._core.actions import loggers

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    K8sEventQueue = asyncio.Queue["K8sEvent"]
else:
    K8sEventQueue = asyncio.Queue

# Logging and event-posting can happen cross-thread: e.g. in sync-executors.
# We have to remember our main event-loop with the queue consumer, to make
# thread-safe coro calls both from inside that event-loop and from outside.
event_queue_loop_var: ContextVar[asyncio.AbstractEventLoop] = ContextVar('event_queue_loop_var')
event_queue_var: ContextVar[K8sEventQueue] = ContextVar('event_queue_var')

# Per-operator container for settings. We only need a log level from there.
# This variable is dedicated to a posting engine, as the call chain is interrupted
# by user-side handlers (no pass-through `settings` arg).
settings_var: ContextVar[configuration.OperatorSettings] = ContextVar('settings_var')

# How long a per-object poster worker waits for the next event before exiting,
# to avoid leaking queues/tasks for short-lived objects. Mirrors the queueing idle timeout.
WORKER_IDLE_TIMEOUT: float = 1.0


class K8sEvent(NamedTuple):
    """
    A single k8s-event to be posted, with all reference information preserved.
    It can exist and be posted even after the object is garbage-collected.
    """
    ref: bodies.ObjectReference
    type: str
    reason: str
    message: str
    backoffs: float | Iterable[float] = ()


def _event_key(ref: bodies.ObjectReference) -> Hashable:
    """
    A stable per-object routing key for ordering events of one object.

    Prefer the uid (unique in time & space). Fall back to identity fields for
    objects without a uid (e.g. some built-in kinds). Never exposed to users.
    """
    uid = ref.get('uid')
    if uid:
        return uid
    return (ref.get('apiVersion'), ref.get('kind'), ref.get('namespace'), ref.get('name'))


def enqueue(
        ref: bodies.ObjectReference,
        type: str,
        reason: str,
        message: str,
        backoffs: float | Iterable[float],
) -> None:
    loop = event_queue_loop_var.get()
    queue = event_queue_var.get()
    event = K8sEvent(ref=ref, type=type, reason=reason, message=message, backoffs=backoffs)

    # Events can be posted from another thread than the event-loop's thread
    # (e.g. from sync-handlers, or from explicitly started per-object threads),
    # or from the same thread (async-handlers and the framework itself).
    running_loop: asyncio.AbstractEventLoop | None
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if running_loop is loop:
        # Posting from the same event-loop as the poster task and queue are in.
        # Therefore, it is the same thread, and all calls here are thread-safe.
        # Special thread-safe cross-event-loop methods make no effect here.
        queue.put_nowait(event)
    else:
        # No event-loop or another event-loop - assume another thread.
        # Use the cross-thread thread-safe methods. Do not block or wait.
        # Beware of #1212: `run_coroutine_threadsafe(queue.put(…), loop=loop)` is flawed.
        loop.call_soon_threadsafe(queue.put_nowait, event)


def event(
        objs: bodies.Body | Iterable[bodies.Body],
        *,
        type: str,
        reason: str,
        message: str = '',
        backoffs: float | Iterable[float] | None = None,
) -> None:
    settings: configuration.OperatorSettings = settings_var.get()
    if settings.posting.enabled:
        effective = backoffs if backoffs is not None else settings.posting.default_backoffs
        for obj in cast(Iterator[bodies.Body], dicts.walk(objs)):
            ref = bodies.build_object_reference(obj)
            enqueue(ref=ref, type=type, reason=reason, message=message, backoffs=effective)


def info(
        objs: bodies.Body | Iterable[bodies.Body],
        *,
        reason: str,
        message: str = '',
        backoffs: float | Iterable[float] | None = None,
) -> None:
    settings: configuration.OperatorSettings = settings_var.get()
    if settings.posting.enabled and settings.posting.level <= logging.INFO:
        effective = backoffs if backoffs is not None else settings.posting.default_backoffs
        for obj in cast(Iterator[bodies.Body], dicts.walk(objs)):
            ref = bodies.build_object_reference(obj)
            enqueue(ref=ref, type='Normal', reason=reason, message=message, backoffs=effective)


def warn(
        objs: bodies.Body | Iterable[bodies.Body],
        *,
        reason: str,
        message: str = '',
        backoffs: float | Iterable[float] | None = None,
) -> None:
    settings: configuration.OperatorSettings = settings_var.get()
    if settings.posting.level <= logging.WARNING:
        effective = backoffs if backoffs is not None else settings.posting.default_backoffs
        for obj in cast(Iterator[bodies.Body], dicts.walk(objs)):
            ref = bodies.build_object_reference(obj)
            enqueue(ref=ref, type='Warning', reason=reason, message=message, backoffs=effective)


def exception(
        objs: bodies.Body | Iterable[bodies.Body],
        *,
        reason: str = '',
        message: str = '',
        exc: BaseException | None = None,
        backoffs: float | Iterable[float] | None = None,
) -> None:
    if exc is None:
        _, exc, _ = sys.exc_info()
    reason = reason if reason else type(exc).__name__
    message = f'{message} {exc}' if message and exc else f'{exc}' if exc else f'{message}'
    settings: configuration.OperatorSettings = settings_var.get()
    if settings.posting.enabled and settings.posting.level <= logging.ERROR:
        effective = backoffs if backoffs is not None else settings.posting.default_backoffs
        for obj in cast(Iterator[bodies.Body], dicts.walk(objs)):
            ref = bodies.build_object_reference(obj)
            enqueue(ref=ref, type='Error', reason=reason, message=message, backoffs=effective)


async def _poster_worker(
        *,
        subqueues: dict[Hashable, K8sEventQueue],
        key: Hashable,
        resource: references.Resource,
        settings: configuration.OperatorSettings,
) -> None:
    """
    Drain one object's event sub-queue in order, posting each with its backoffs.

    Exits after an idle period so we do not keep a worker per dormant object.
    The router (:func:`poster`) re-spawns a worker when new events arrive.
    """
    backlog = subqueues[key]
    try:
        while True:
            try:
                posted_event = await asyncio.wait_for(backlog.get(), timeout=WORKER_IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                # Double-check to avoid a race where an event arrived exactly at timeout.
                # IMPORTANT: no async/await between this break and the finally-block below.
                if backlog.empty():
                    break
                else:
                    continue

            await events.post_event(
                ref=posted_event.ref,
                type=posted_event.type,
                reason=posted_event.reason,
                message=posted_event.message,
                resource=resource,
                settings=settings,
                logger=logger,
                backoffs=posted_event.backoffs,
            )
    finally:
        # Garbage-collect our sub-queue so the router re-creates it (and us) on demand.
        with contextlib.suppress(KeyError):
            del subqueues[key]


async def poster(
        *,
        event_queue: K8sEventQueue,
        backbone: references.Backbone,
        settings: configuration.OperatorSettings,
) -> NoReturn:
    """
    Route queued events to per-object workers that post them in the background.

    Events of one object are routed to a single per-object sub-queue and posted
    in order by one worker. Different objects are posted concurrently, so a retry
    or backoff for one object never blocks the events of another object.

    Workers are fire-and-forget jobs managed by a scheduler; they self-terminate
    when their object has been idle for a while, and are re-spawned on demand.
    """
    resource = await backbone.wait_for(references.EVENTS)
    scheduler = aiotasks.Scheduler()
    subqueues: dict[Hashable, K8sEventQueue] = {}
    try:
        while True:
            posted_event = await event_queue.get()
            key = _event_key(posted_event.ref)
            try:
                # Fast path: an existing worker is draining this object's sub-queue.
                await subqueues[key].put(posted_event)
            except KeyError:
                # No worker for this object (new or just-exited): create queue + worker.
                subqueues[key] = asyncio.Queue()
                await subqueues[key].put(posted_event)
                await scheduler.spawn(
                    name=f"event poster for {key!r}",
                    coro=_poster_worker(
                        subqueues=subqueues,
                        key=key,
                        resource=resource,
                        settings=settings,
                    ))
    finally:
        # Terminate all per-object workers even if the poster is double-cancelled (tests).
        closing_task = asyncio.create_task(scheduler.close())
        while not closing_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(closing_task)


class K8sPoster(logging.Handler):
    """
    A handler to post all log messages as K8s events.
    """
    if sys.version_info[:2] < (3, 13):
        # Disable this optimisation for Python >= 3.13.
        # The `handle` no longer supports having `None` as lock.
        def createLock(self) -> None:
            # Save some time on unneeded locks. Events are posted in the background.
            # We only put events to the queue, which is already lock-protected.
            self.lock = None

    def filter(self, record: logging.LogRecord) -> bool:
        # Only those which have a k8s object referred (see: `ObjectLogger`).
        # Otherwise, we have nothing to post, and nothing to do.
        # TODO: remove all bool() -- they were needed for Python 3.12 & MyPy 1.8.0 wrong inference.
        settings: configuration.OperatorSettings | None
        settings = getattr(record, 'settings', None)
        level_ok = settings is not None and bool(record.levelno >= settings.posting.level)
        enabled = settings is not None and bool(settings.posting.enabled)
        loggers = settings is not None and bool(settings.posting.loggers)
        has_ref = hasattr(record, 'k8s_ref')
        skipped = hasattr(record, 'k8s_skip') and bool(getattr(record, 'k8s_skip'))
        return enabled and level_ok and loggers and has_ref and not skipped and bool(super().filter(record))

    def emit(self, record: logging.LogRecord) -> None:
        # Same try-except as in e.g. `logging.StreamHandler`.
        try:
            ref = getattr(record, 'k8s_ref')
            type = (
                "Debug" if record.levelno <= logging.DEBUG else
                "Normal" if record.levelno <= logging.INFO else
                "Warning" if record.levelno <= logging.WARNING else
                "Error" if record.levelno <= logging.ERROR else
                "Fatal" if record.levelno <= logging.FATAL else
                logging.getLevelName(record.levelno).capitalize())
            reason = 'Logging'
            message = self.format(record)
            settings: configuration.OperatorSettings | None = getattr(record, 'settings', None)
            backoffs: float | Iterable[float] = \
                settings.posting.logging_backoffs if settings is not None else ()
            enqueue(
                ref=ref,
                type=type,
                reason=reason,
                message=message,
                backoffs=backoffs)
        except Exception:
            self.handleError(record)


loggers.logger.addHandler(K8sPoster())
