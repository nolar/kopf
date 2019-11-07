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
import json
import logging
from typing import Optional, Dict, AsyncIterator, cast

import aiohttp

from kopf import config
from kopf.clients import auth
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

    # Repeat through disconnects of the watch as long as the resource version is valid (no errors).
    # The individual watching API calls are disconnected by timeout even if the stream is fine.
    while True:

        # Then, watch the resources starting from the list's resource version.
        stream = watch_objs(
            resource=resource, namespace=namespace,
            timeout=config.WatchersConfig.default_stream_timeout,
            since=resource_version,
        )
        async for event in stream:

            # "410 Gone" is for the "resource version too old" error, we must restart watching.
            # The resource versions are lost by k8s after few minutes (5, as per the official doc).
            # The error occurs when there is nothing happening for few minutes. This is normal.
            if event['type'] == 'ERROR' and cast(bodies.Error, event['object'])['code'] == 410:
                logger.debug("Restarting the watch-stream for %r", resource)
                return  # out of regular stream, to the infinite stream.

            # Other watch errors should be fatal for the operator.
            if event['type'] == 'ERROR':
                raise WatchingError(f"Error in the watch-stream: {event['object']}")

            # Ensure that the event is something we understand and can handle.
            if event['type'] not in ['ADDED', 'MODIFIED', 'DELETED']:
                logger.warning("Ignoring an unsupported event type: %r", event)
                continue

            # Keep the latest seen resource version for continuation of the stream on disconnects.
            body = cast(bodies.Body, event['object'])
            resource_version = body.get('metadata', {}).get('resourceVersion', resource_version)

            # Yield normal events to the consumer. Errors are already filtered out.
            yield cast(bodies.Event, event)


@auth.reauthenticated_stream
async def watch_objs(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        timeout: Optional[float] = None,
        since: Optional[str] = None,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> AsyncIterator[bodies.RawEvent]:
    """
    Watch objects of a specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    params: Dict[str, str] = {}
    params['watch'] = 'true'
    if since is not None:
        params['resourceVersion'] = since
    if timeout is not None:
        params['timeoutSeconds'] = str(timeout)

    # TODO: also add cluster-wide resource when --namespace is set?
    response = await session.get(
        url=resource.get_url(server=session.server, namespace=namespace, params=params),
        timeout=aiohttp.ClientTimeout(total=None),
    )
    response.raise_for_status()

    async with response:
        async for line in response.content:
            event = cast(bodies.RawEvent, json.loads(line.decode("utf-8")))
            yield event
