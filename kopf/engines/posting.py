"""
All the functions to write the Kubernetes events for the Kubernetes objects.

They are used internally in the handling routines to show the progress,
and can be used directly from the handlers to add arbitrary custom events.

The actual k8s-event posting runs in the background,
and posts the k8s-events as soon as they are queued.

The k8s-events are queued in two ways:

* Explicit calls to `kopf.event`, `kopf.info`, `kopf.warn`, `kopf.exception`.
* Logging messages made on the object logger (above INFO level by default).

This also includes all logging messages posted by the framework itself.
"""
import asyncio
import sys
from contextvars import ContextVar
from typing import Mapping, Text, NamedTuple

from kopf import config
from kopf.clients import events
from kopf.structs import dicts
from kopf.structs import hierarchies

# Logging and event-posting can happen cross-thread: e.g. in sync-executors.
# We have to remember our main event-loop with the queue consumer, to make
# thread-safe coro calls both from inside that event-loop and from outside.
event_queue_loop_var: ContextVar[asyncio.AbstractEventLoop] = ContextVar('event_queue_loop_var')
event_queue_var: ContextVar[asyncio.Queue] = ContextVar('event_queue_var')


class K8sEvent(NamedTuple):
    """
    A single k8s-event to be posted, with all ref-information preserved.
    It can exist and be posted even after the object is garbage-collected.
    """
    ref: Mapping
    type: Text
    reason: Text
    message: Text


def enqueue(ref, type, reason, message):
    loop = event_queue_loop_var.get()
    queue = event_queue_var.get()
    event = K8sEvent(ref=ref, type=type, reason=reason, message=message)

    # Events can be posted from another thread than the event-loop's thread
    # (e.g. from sync-handlers, or from explicitly started per-object threads),
    # or from the same thread (async-handlers and the framework itself).
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
        # Use the cross-thread thread-safe methods. Block until enqueued there.
        future = asyncio.run_coroutine_threadsafe(queue.put(event), loop=loop)
        future.result()  # block, wait, re-raise.


def event(objs, *, type, reason, message=''):
    for obj in dicts.walk(objs):
        ref = hierarchies.build_object_reference(obj)
        enqueue(ref=ref, type=type, reason=reason, message=message)


def info(obj, *, reason, message=''):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_INFO:
        return
    event(obj, type='Normal', reason=reason, message=message)


def warn(obj, *, reason, message=''):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_WARNING:
        return
    event(obj, type='Warning', reason=reason, message=message)


def exception(obj, *, reason='', message='', exc=None):
    if config.EventsConfig.events_loglevel > config.LOGLEVEL_ERROR:
        return
    if exc is None:
        _, exc, _ = sys.exc_info()
    reason = reason if reason else type(exc).__name__
    message = f'{message} {exc}' if message and exc else f'{exc}' if exc else f'{message}'
    event(obj, type='Error', reason=reason, message=message)


async def poster(
        event_queue: asyncio.Queue,
):
    """
    Post events in the background as they are queued.

    When the events come from the logging system, they have
    their reason, type, and other fields adjusted to meet Kubernetes's concepts.

    When the events are explicitly defined via `kopf.event` and similar calls,
    they have these special fields defined already.

    In either case, we pass the queued events directly to the K8s client
    (or a client wrapper/adapter), with no extra processing.

    This task is defined in this module only because all other tasks are here,
    so we keep all forever-running tasks together.
    """
    while True:
        posted_event = await event_queue.get()
        await events.post_event(
            ref=posted_event.ref,
            type=posted_event.type,
            reason=posted_event.reason,
            message=posted_event.message)
