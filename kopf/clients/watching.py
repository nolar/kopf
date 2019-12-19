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
import contextlib
import json
import logging
from typing import Optional, Dict, AsyncIterator, Any, cast, TYPE_CHECKING

import aiohttp

from kopf import config
from kopf.clients import auth
from kopf.clients import discovery
from kopf.clients import fetching
from kopf.structs import bodies
from kopf.structs import primitives
from kopf.structs import resources

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    asyncio_Future = asyncio.Future[Any]
else:
    asyncio_Future = asyncio.Future


class WatchingError(Exception):
    """
    Raised when an unexpected error happens in the watch-stream API.
    """


async def infinite_watch(
        *,
        resource: resources.Resource,
        namespace: Optional[str],
        freeze_mode: Optional[primitives.Toggle] = None,
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
        stream = streaming_watch(
            resource=resource,
            namespace=namespace,
            freeze_mode=freeze_mode,
        )
        async for event in stream:
            yield event
        await asyncio.sleep(config.WatchersConfig.watcher_retry_delay)


async def streaming_watch(
        *,
        resource: resources.Resource,
        namespace: Optional[str],
        freeze_mode: Optional[primitives.Toggle] = None,
) -> AsyncIterator[bodies.Event]:

    # Prevent both watching and listing while the freeze mode is on, until it is off.
    # Specifically, the watch-stream closes its connection once the freeze mode is on,
    # so the while-true & for-event-in-stream cycles exit, and this coroutine is started
    # again by the `infinite_stream()` (the watcher timeout is swallowed by the freeze time).
    if freeze_mode is not None and freeze_mode.is_on():
        logger.debug("Freezing the watch-stream for %r", resource)
        await freeze_mode.wait_for_off()
        logger.debug("Resuming the watch-stream for %r", resource)

    # A stop-feature is a client-specific way of terminating the streaming HTTPS connection
    # when a freeze-mode is turned on. The low-level API call attaches its `response.close()`
    # to the future's callbacks, and a background task triggers it when the mode is turned on.
    freeze_waiter: asyncio_Future
    if freeze_mode is not None:
        freeze_waiter = asyncio.create_task(freeze_mode.wait_for_on())
    else:
        freeze_waiter = asyncio.Future()  # a dummy just ot have it

    try:
        stream = continuous_watch(
            resource=resource, namespace=namespace,
            freeze_waiter=freeze_waiter,
        )
        async for event in stream:
            yield event
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            freeze_waiter.cancel()
            await freeze_waiter


async def continuous_watch(
        *,
        resource: resources.Resource,
        namespace: Optional[str],
        freeze_waiter: asyncio_Future,
) -> AsyncIterator[bodies.Event]:

    # First, list the resources regularly, and get the list's resource version.
    # Simulate the events with type "None" event - used in detection of causes.
    items, resource_version = await fetching.list_objs_rv(resource=resource, namespace=namespace)
    for item in items:
        yield {'type': None, 'object': item}

    # Repeat through disconnects of the watch as long as the resource version is valid (no errors).
    # The individual watching API calls are disconnected by timeout even if the stream is fine.
    while not freeze_waiter.done():

        # Then, watch the resources starting from the list's resource version.
        stream = watch_objs(
            resource=resource, namespace=namespace,
            timeout=config.WatchersConfig.default_stream_timeout,
            since=resource_version,
            freeze_waiter=freeze_waiter,
        )
        async for event in stream:

            # "410 Gone" is for the "resource version too old" error, we must restart watching.
            # The resource versions are lost by k8s after few minutes (5, as per the official doc).
            # The error occurs when there is nothing happening for few minutes. This is normal.
            if event['type'] == 'ERROR' and cast(bodies.Error, event['object'])['code'] == 410:
                logger.debug("Restarting the watch-stream for %r", resource)
                return  # out of the regular stream, to the infinite stream.

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
        freeze_waiter: asyncio_Future,
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

    is_namespaced = await discovery.is_namespaced(resource=resource, session=session)
    namespace = namespace if is_namespaced else None

    params: Dict[str, str] = {}
    params['watch'] = 'true'
    if since is not None:
        params['resourceVersion'] = since
    if timeout is not None:
        params['timeoutSeconds'] = str(timeout)

    # Talk to the API and initiate a streaming response.
    response = await session.get(
        url=resource.get_url(server=session.server, namespace=namespace, params=params),
        timeout=aiohttp.ClientTimeout(total=None),
    )
    response.raise_for_status()

    # Stream the parsed events from the response until it is closed server-side,
    # or until it is closed client-side by the freeze-waiting future's callbacks.
    response_close_callback = lambda _: response.close()
    freeze_waiter.add_done_callback(response_close_callback)
    try:
        async with response:
            async for line in _iter_jsonlines(response.content):
                event = cast(bodies.RawEvent, json.loads(line.decode("utf-8")))
                yield event
    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError):
        pass
    finally:
        freeze_waiter.remove_done_callback(response_close_callback)


async def _iter_jsonlines(
        content: aiohttp.StreamReader,
        chunk_size: int = 1024 * 1024,
) -> AsyncIterator[bytes]:
    """
    Iterate line by line over the response's content.

    Usage::

        async for line in _iter_lines(response.content):
            pass

    This is an equivalent of::

        async for line in response.content:
            pass

    Except that the aiohttp's line iteration fails if the accumulated buffer
    length is above 2**17 bytes, i.e. 128 KB (`aiohttp.streams.DEFAULT_LIMIT`
    for the buffer's low-watermark, multiplied by 2 for the high-watermark).
    Kubernetes secrets and other fields can be much longer, up to MBs in length.

    The chunk size of 1MB is an empirical guess for keeping the memory footprint
    reasonably low on huge amount of small lines (limited to 1 MB in total),
    while ensuring the near-instant reads of the huge lines (can be a problem
    with a small chunk size due to too many iterations).

    .. seealso::
        https://github.com/zalando-incubator/kopf/issues/275
    """

    # Minimize the memory footprint by keeping at most 2 copies of a yielded line in memory
    # (in the buffer and as a yielded value), and at most 1 copy of other lines (in the buffer).
    buffer = b''
    async for data in content.iter_chunked(chunk_size):
        buffer += data
        del data

        start = 0
        index = buffer.find(b'\n', start)
        while index >= 0:
            line = buffer[start:index]
            if line:
                yield line
            del line
            start = index + 1
            index = buffer.find(b'\n', start)

        if start > 0:
            buffer = buffer[start:]

    if buffer:
        yield buffer
