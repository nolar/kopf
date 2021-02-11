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
import enum
import json
import logging
from typing import AsyncIterator, Dict, Optional, Union, cast

import aiohttp

from kopf.clients import auth, errors, fetching
from kopf.structs import bodies, configuration, primitives, references
from kopf.utilities import aiotasks

logger = logging.getLogger(__name__)


class WatchingError(Exception):
    """
    Raised when an unexpected error happens in the watch-stream API.
    """


class Bookmark(enum.Enum):
    """ Special marks sent in the stream among raw events. """
    LISTED = enum.auto()  # the listing is over, now streaming.


async def infinite_watch(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        operator_paused: Optional[primitives.ToggleSet] = None,  # None for tests & observation
        _iterations: Optional[int] = None,  # used in tests/mocks/fixtures
) -> AsyncIterator[Union[Bookmark, bodies.RawEvent]]:
    """
    Stream the watch-events infinitely.

    This routine is extracted because it is difficult to test infinite loops.
    It is made as simple as possible, and is assumed to work without testing.

    This routine never ends gracefully. If a watcher's stream fails,
    a new one is recreated, and the stream continues.
    It only exits with unrecoverable exceptions.
    """
    how = ' (paused)' if operator_paused is not None and operator_paused.is_on() else ''
    where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'
    logger.debug(f"Starting the watch-stream for {resource} {where}{how}.")
    try:
        while _iterations is None or _iterations > 0:  # equivalent to `while True` in non-test mode
            _iterations = None if _iterations is None else _iterations - 1
            async with streaming_block(
                namespace=namespace,
                resource=resource,
                operator_paused=operator_paused,
            ) as operator_pause_waiter:
                stream = continuous_watch(
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    operator_pause_waiter=operator_pause_waiter,
                )
                async for raw_event in stream:
                    yield raw_event
            await asyncio.sleep(settings.watching.reconnect_backoff)
    finally:
        logger.debug(f"Stopping the watch-stream for {resource} {where}.")


@contextlib.asynccontextmanager
async def streaming_block(
        *,
        resource: references.Resource,
        namespace: references.Namespace,
        operator_paused: Optional[primitives.ToggleSet] = None,  # None for tests & observation
) -> AsyncIterator[aiotasks.Future]:
    """
    Block the execution until un-paused; signal when it is active again.

    This prevents both watching and listing while the operator is paused,
    until it is off. Specifically, the watch-stream closes its connection
    once paused, so the while-true & for-event-in-stream cycles exit,
    and the streaming coroutine is started again by `infinite_stream()`
    (the watcher timeout is swallowed by the pause time).

    Returns a future (or a task) that is set (or finished) when paused again.

    A stop-future is a client-specific way of terminating the streaming HTTPS
    connections when paused again. The low-level streaming API call attaches
    its `response.close()` to the future's "done" callback,
    so that the stream is closed once the operator is paused.

    Note: this routine belongs to watching and does not belong to peering.
    The pause can be managed in any other ways: as an imaginary edge case,
    imagine a operator with UI with a "pause" button that pauses the operator.
    """
    where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'

    # Block until unpaused before even starting the API communication.
    if operator_paused is not None and operator_paused.is_on():
        names = {toggle.name for toggle in operator_paused if toggle.is_on() and toggle.name}
        pause_reason = f" (blockers: {', '.join(names)})" if names else ""
        logger.debug(f"Pausing the watch-stream for {resource} {where}{pause_reason}.")

        await operator_paused.wait_for(False)

        names = {toggle.name for toggle in operator_paused if toggle.is_on() and toggle.name}
        resuming_reason = f" (resolved: {', '.join(names)})" if names else ""
        logger.debug(f"Resuming the watch-stream for {resource} {where}{resuming_reason}.")

    # Create the signalling future for when paused again.
    operator_pause_waiter: aiotasks.Future
    if operator_paused is not None:
        operator_pause_waiter = aiotasks.create_task(
            operator_paused.wait_for(True),
            name=f"pause-waiter for {resource}")
    else:
        operator_pause_waiter = asyncio.Future()  # a dummy just to have it

    # Go for the streaming with the prepared pauseing/unpausing setup.
    try:
        yield operator_pause_waiter
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            operator_pause_waiter.cancel()
            await operator_pause_waiter


async def continuous_watch(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        operator_pause_waiter: aiotasks.Future,
) -> AsyncIterator[Union[Bookmark, bodies.RawEvent]]:

    # First, list the resources regularly, and get the list's resource version.
    # Simulate the events with type "None" event - used in detection of causes.
    items, resource_version = await fetching.list_objs_rv(resource=resource, namespace=namespace)
    for item in items:
        yield {'type': None, 'object': item}

    # Notify the watcher that the initial listing is over, even if there was nothing yielded.
    yield Bookmark.LISTED

    # Repeat through disconnects of the watch as long as the resource version is valid (no errors).
    # The individual watching API calls are disconnected by timeout even if the stream is fine.
    while not operator_pause_waiter.done():

        # Then, watch the resources starting from the list's resource version.
        stream = watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            timeout=settings.watching.server_timeout,
            since=resource_version,
            operator_pause_waiter=operator_pause_waiter,
        )
        async for raw_input in stream:
            raw_type = raw_input['type']
            raw_object = raw_input['object']

            # "410 Gone" is for the "resource version too old" error, we must restart watching.
            # The resource versions are lost by k8s after few minutes (5, as per the official doc).
            # The error occurs when there is nothing happening for few minutes. This is normal.
            if raw_type == 'ERROR' and cast(bodies.RawError, raw_object)['code'] == 410:
                where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'
                logger.debug(f"Restarting the watch-stream for {resource} {where}.")
                return  # out of the regular stream, to the infinite stream.

            # Other watch errors should be fatal for the operator.
            if raw_type == 'ERROR':
                raise WatchingError(f"Error in the watch-stream: {raw_object}")

            # Ensure that the event is something we understand and can handle.
            if raw_type not in ['ADDED', 'MODIFIED', 'DELETED']:
                logger.warning("Ignoring an unsupported event type: %r", raw_input)
                continue

            # Keep the latest seen resource version for continuation of the stream on disconnects.
            body = cast(bodies.RawBody, raw_object)
            resource_version = body.get('metadata', {}).get('resourceVersion', resource_version)

            # Yield normal events to the consumer. Errors are already filtered out.
            yield cast(bodies.RawEvent, raw_input)


@auth.reauthenticated_stream
async def watch_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        timeout: Optional[float] = None,
        since: Optional[str] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
        operator_pause_waiter: aiotasks.Future,
) -> AsyncIterator[bodies.RawInput]:
    """
    Watch objects of a specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    params: Dict[str, str] = {}
    params['watch'] = 'true'
    if since is not None:
        params['resourceVersion'] = since
    if timeout is not None:
        params['timeoutSeconds'] = str(timeout)

    # Stream the parsed events from the response until it is closed server-side,
    # or until it is closed client-side by the pause-waiting future's callbacks.
    try:
        response = await context.session.get(
            url=resource.get_url(server=context.server, namespace=namespace, params=params),
            timeout=aiohttp.ClientTimeout(
                total=settings.watching.client_timeout,
                sock_connect=settings.watching.connect_timeout,
            ),
        )
        await errors.check_response(response)

        response_close_callback = lambda _: response.close()
        operator_pause_waiter.add_done_callback(response_close_callback)
        try:
            async with response:
                async for line in _iter_jsonlines(response.content):
                    raw_input = cast(bodies.RawInput, json.loads(line.decode("utf-8")))
                    yield raw_input
        finally:
            operator_pause_waiter.remove_done_callback(response_close_callback)

    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError):
        pass


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
