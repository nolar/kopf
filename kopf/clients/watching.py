"""
Watching and streaming watch-events.

Kubernetes client's watching streams are synchronous. To make them asynchronous,
we put them into a `concurrent.futures.ThreadPoolExecutor`,
and yield from there asynchronously.

However, async/await coroutines misbehave with `StopIteration` exceptions
raised by the `next` method: see `PEP-479`_.

As a workaround, we replace `StopIteration` with our custom `StopStreaming`
inherited from `RuntimeError` (as suggested by `PEP-479`_),
and re-implement the generators to make them async.

All of this is a workaround for the standard Kubernetes client's limitations.
They would not be needed if the client library were natively asynchronous.

.. _PEP-479: https://www.python.org/dev/peps/pep-0479/
"""

import asyncio
import logging
from typing import Union

import kubernetes

from kopf.clients import fetching
from kopf.reactor import registries

logger = logging.getLogger(__name__)

DEFAULT_STREAM_TIMEOUT = None
""" The maximum duration of one streaming request. Patched in some tests. """


class WatchingError(Exception):
    """
    Raised when an unexpected error happens in the watch-stream API.
    """


class StopStreaming(RuntimeError):
    """
    Raised when the watch-stream generator ends streaming.
    Replaces `StopIteration`.
    """


def streaming_next(src):
    """
    Same as `next`, but replaces the `StopIteration` with `StopStreaming`.
    """
    try:
        return next(src)
    except StopIteration as e:
        raise StopStreaming(str(e))


async def streaming_aiter(src, loop=None, executor=None):
    """
    Same as `iter`, but asynchronous and stops on `StopStreaming`, not on `StopIteration`.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    while True:
        try:
            yield await loop.run_in_executor(executor, streaming_next, src)
        except StopStreaming:
            return


async def streaming_watch(
        resource: registries.Resource,
        namespace: Union[None, str],
):
    """
    Stream the watch-events from one single API watch-call.
    """

    # First, list the resources regularly, and get the list's resource version.
    # Simulate the events with type "None" event - used in detection of causes.
    rsp = fetching.list_objs(resource=resource, namespace=namespace)
    resource_version = rsp['metadata']['resourceVersion']
    for item in rsp['items']:
        yield {'type': None, 'object': item}

    # Then, watch the resources starting from the list's resource version.
    kwargs = {}
    kwargs.update(dict(resource_version=resource_version) if resource_version else {})
    kwargs.update(dict(timeout_seconds=DEFAULT_STREAM_TIMEOUT) if DEFAULT_STREAM_TIMEOUT else {})
    loop = asyncio.get_event_loop()
    fn = fetching.make_list_fn(resource=resource, namespace=namespace)
    watch = kubernetes.watch.Watch()
    stream = watch.stream(fn, **kwargs)
    async for event in streaming_aiter(stream, loop=loop):

        # "410 Gone" is for the "resource version too old" error, we must restart watching.
        # The resource versions are lost by k8s after few minutes (as per the official doc).
        # The error occurs when there is nothing happening for few minutes. This is normal.
        if event['type'] == 'ERROR' and event['object']['code'] == 410:
            logger.debug("Restarting the watch-stream for %r", resource)
            break  # out of for-cycle, to the while-true-cycle.

        # Other watch errors should be fatal for the operator.
        if event['type'] == 'ERROR':
            raise WatchingError(f"Error in the watch-stream: {event['object']}")

        # Ensure that the event is something we understand and can handle.
        if event['type'] not in ['ADDED', 'MODIFIED', 'DELETED']:
            logger.warning("Ignoring an unsupported event type: %r", event)
            continue

        # Yield normal events to the consumer.
        yield event


async def infinite_watch(
        resource: registries.Resource,
        namespace: Union[None, str],
):
    """
    Stream the watch-events infinitely.

    This routine is extracted only due to difficulty of testing
    of the infinite loops. It is made as simple as possible,
    and is assumed to work without testing.

    This routine never ends gracefully. If a watcher's stream fails,
    a new one is recreated, and the stream continues.
    It only exits with unrecoverable exceptions.
    """
    while True:
        async for event in streaming_watch(resource=resource, namespace=namespace):
            yield event
