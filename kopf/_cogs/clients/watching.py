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
import logging
from typing import AsyncIterator, Dict, Optional, Union, cast

import aiohttp

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.clients import api, errors, fetching
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references

logger = logging.getLogger(__name__)

HTTP_TOO_MANY_REQUESTS_CODE = 429
DEFAULT_RETRY_DELAY_SECONDS = 1


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
        operator_paused: Optional[aiotoggles.ToggleSet] = None,  # None for tests & observation
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
                try:
                    async for raw_event in stream:
                        yield raw_event
                except errors.APIClientError as ex:
                    if ex.code != HTTP_TOO_MANY_REQUESTS_CODE:
                        raise

                    retry_after = ex.details.get("retryAfterSeconds") if ex.details else None
                    retry_wait = retry_after or DEFAULT_RETRY_DELAY_SECONDS
                    logger.warning(
                        f"Receiving `too many requests` error from server, will retry after "
                        f"{retry_wait} seconds. Error details: {ex}"
                    )
                    await asyncio.sleep(retry_wait)
            await asyncio.sleep(settings.watching.reconnect_backoff)
    finally:
        logger.debug(f"Stopping the watch-stream for {resource} {where}.")


@contextlib.asynccontextmanager
async def streaming_block(
        *,
        resource: references.Resource,
        namespace: references.Namespace,
        operator_paused: Optional[aiotoggles.ToggleSet] = None,  # None for tests & observation
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
        operator_pause_waiter = asyncio.create_task(
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
    try:
        objs, resource_version = await fetching.list_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        )
        for obj in objs:
            yield {'type': None, 'object': obj}

    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError):
        return

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
            since=resource_version,
            operator_pause_waiter=operator_pause_waiter,
        )
        async for raw_input in stream:
            raw_type = raw_input['type']
            raw_object = raw_input['object']

            # "410 Gone" is for the "resource version too old" error, we must restart watching.
            # The resource versions are lost by k8s after a few minutes (5 as per the official doc).
            # The error occurs when there is nothing happening for a few minutes. This is normal.
            if raw_type == 'ERROR' and cast(bodies.RawError, raw_object)['code'] == 410:
                where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'
                logger.debug(f"Restarting the watch-stream for {resource} {where}.")
                return  # out of the regular stream, to the infinite stream.

            # Other watch errors should be fatal for the operator.
            if raw_type == 'ERROR':
                raise WatchingError(f"Error in the watch-stream: {raw_object}")

            # Ensure that the event is something we understand and can handle.
            if raw_type not in ['ADDED', 'MODIFIED', 'DELETED']:
                logger.warning(f"Ignoring an unsupported event type: {raw_input!r}")
                continue

            # Keep the latest seen resource version for continuation of the stream on disconnects.
            body = cast(bodies.RawBody, raw_object)
            resource_version = body.get('metadata', {}).get('resourceVersion', resource_version)

            # Yield normal events to the consumer. Errors are already filtered out.
            yield cast(bodies.RawEvent, raw_input)


async def watch_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        since: Optional[str] = None,
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
    params: Dict[str, str] = {}
    params['watch'] = 'true'
    if since is not None:
        params['resourceVersion'] = since
    if settings.watching.server_timeout is not None:
        params['timeoutSeconds'] = str(settings.watching.server_timeout)

    connect_timeout = (
        settings.watching.connect_timeout if settings.watching.connect_timeout is not None else
        settings.networking.connect_timeout if settings.networking.connect_timeout is not None else
        settings.networking.request_timeout
    )

    # Stream the parsed events from the response until it is closed server-side,
    # or until it is closed client-side by the pause-waiting future's callbacks.
    try:
        async for raw_input in api.stream(
            url=resource.get_url(namespace=namespace, params=params),
            logger=logger,
            settings=settings,
            stopper=operator_pause_waiter,
            timeout=aiohttp.ClientTimeout(
                total=settings.watching.client_timeout,
                sock_connect=connect_timeout,
            ),
        ):
            yield raw_input

    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError):
        pass
