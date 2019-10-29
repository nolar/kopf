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
import collections
import concurrent.futures
import logging
from typing import Optional, Iterator, AsyncIterator, cast

import pykube

from kopf import config
from kopf.clients import auth
from kopf.clients import classes
from kopf.clients import fetching
from kopf.structs import bodies
from kopf.structs import resources

logger = logging.getLogger(__name__)

# Pykube declares it inside of a function, not importable from the package/module.
PykubeWatchEvent = collections.namedtuple("WatchEvent", "type object")


class WatchingError(Exception):
    """
    Raised when an unexpected error happens in the watch-stream API.
    """


class StopStreaming(RuntimeError):
    """
    Raised when the watch-stream generator ends streaming.
    Replaces `StopIteration`.
    """


def streaming_next(__src: Iterator[PykubeWatchEvent]) -> PykubeWatchEvent:
    """
    Same as `next`, but replaces the `StopIteration` with `StopStreaming`.
    """
    try:
        return next(__src)
    except StopIteration as e:
        raise StopStreaming(str(e))


async def streaming_aiter(
        __src: Iterator[PykubeWatchEvent],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        executor: Optional[concurrent.futures.Executor] = None,
) -> AsyncIterator[PykubeWatchEvent]:
    """
    Same as `iter`, but asynchronous and stops on `StopStreaming`, not on `StopIteration`.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    while True:
        try:
            yield await loop.run_in_executor(executor, streaming_next, __src)
        except StopStreaming:
            return


async def infinite_watch(
        *,
        resource: resources.Resource,
        namespace: Optional[str],
) -> AsyncIterator[bodies.Event]:
    """
    Stream the watch-events infinitely.

    This routine is extracted because it is difficult to test infinite loops.
    It is made as simple as possible, and is assumed to work without testing.

    This routine never ends gracefully. If a watcher's stream fails,
    a new one is recreated, and the stream continues.
    It only exits with unrecoverable exceptions.
    """
    while True:
        async for event in streaming_watch(resource=resource, namespace=namespace):
            yield event
        await asyncio.sleep(config.WatchersConfig.watcher_retry_delay)


async def streaming_watch(
        *,
        resource: resources.Resource,
        namespace: Optional[str],
) -> AsyncIterator[bodies.Event]:
    """
    Stream the watch-events from one single API watch-call.
    """

    # First, list the resources regularly, and get the list's resource version.
    # Simulate the events with type "None" event - used in detection of causes.
    items, resource_version = await fetching.list_objs_rv(resource=resource, namespace=namespace)
    for item in items:
        yield {'type': None, 'object': item}

    # Then, watch the resources starting from the list's resource version.
    stream = watch_objs(
        resource=resource, namespace=namespace,
        timeout=config.WatchersConfig.default_stream_timeout,
        since=resource_version,
    )
    async for event in stream:

        # "410 Gone" is for the "resource version too old" error, we must restart watching.
        # The resource versions are lost by k8s after few minutes (as per the official doc).
        # The error occurs when there is nothing happening for few minutes. This is normal.
        if event['type'] == 'ERROR' and cast(bodies.Error, event['object'])['code'] == 410:
            logger.debug("Restarting the watch-stream for %r", resource)
            break  # out of for-cycle, to the while-true-cycle.

        # Other watch errors should be fatal for the operator.
        if event['type'] == 'ERROR':
            raise WatchingError(f"Error in the watch-stream: {event['object']}")

        # Ensure that the event is something we understand and can handle.
        if event['type'] not in ['ADDED', 'MODIFIED', 'DELETED']:
            logger.warning("Ignoring an unsupported event type: %r", event)
            continue

        # Yield normal events to the consumer. Errors are already filtered out.
        yield cast(bodies.Event, event)


@auth.reauthenticated_stream
async def watch_objs(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        timeout: Optional[float] = None,
        since: Optional[str] = None,
        api: Optional[pykube.HTTPClient] = None,  # injected by the decorator
) -> AsyncIterator[bodies.RawEvent]:
    """
    Watch objects of a specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if api is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    params = {}
    if timeout is not None:
        params['timeoutSeconds'] = timeout

    src: Iterator[PykubeWatchEvent]
    cls = await classes._make_cls(api=api, resource=resource)
    namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
    lst = cls.objects(api, namespace=pykube.all if namespace is None else namespace)
    src = lst.watch(since=since, params=params)
    async for event in streaming_aiter(iter(src)):
        yield cast(bodies.RawEvent, {
            'type': event.type,
            'object': event.object.obj,
        })
