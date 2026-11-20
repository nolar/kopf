"""
Watching and streaming watch-events.

Kubernetes client's watching streams are synchronous. To make them asynchronous,
we put them into a :class:`concurrent.futures.ThreadPoolExecutor`,
and yield from there asynchronously.

However, async/await coroutines misbehave with ``StopIteration`` exceptions
raised by the ``next`` method: see `PEP-479`_.

As a workaround, we replace ``StopIteration`` with our custom ``StopStreaming``
inherited from ``RuntimeError`` (as suggested by `PEP-479`_),
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
from collections.abc import AsyncGenerator, AsyncIterator
from typing import cast

import aiohttp

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.clients import api, errors, fetching
from kopf._cogs.configs import configuration
from kopf._cogs.structs import bodies, references

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
        operator_paused: aiotoggles.ToggleSet | None = None,  # None for tests & observation
        _iterations: int | None = None,  # used in tests/mocks/fixtures
) -> AsyncIterator[Bookmark | bodies.RawEvent]:
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
    error_backoff: float = 0
    try:
        while _iterations is None or _iterations > 0:  # equivalent to `while True` in non-test mode
            _iterations = None if _iterations is None else _iterations - 1
            backoff = settings.watching.reconnect_backoff
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
                        # A watched event (not a listed one) means the stream is healthy again.
                        if not isinstance(raw_event, Bookmark) and raw_event['type'] is not None:
                            error_backoff = 0
                        yield raw_event
                except (aiohttp.ClientConnectionError,
                        aiohttp.ClientPayloadError,
                        json.JSONDecodeError,
                        errors.APINotFoundError,
                        errors.APITooManyRequestsError,
                        errors.APIServerError,
                        TimeoutError) as e:
                    # If it has escalated after all the retries, go back to trying anyway.
                    # This stream is not allowed to fail, unlike other regular requests.
                    # Authentication & authorization errors are not retried here: they escalate,
                    # so that the operator is restarted as a whole with fresh credentials.
                    # Consecutive errors double the backoff up to a limit, but the server's
                    # retry-after is obeyed the same way as for the regular API requests.
                    retry_after = e.retry_after if isinstance(e, errors.APIError) else None
                    error_backoff, backoff = errors.get_reconnect_backoff(
                        base=backoff,
                        previous=error_backoff,
                        retry_after=retry_after,
                        enforce_retry_after=settings.networking.enforce_retry_after,
                    )
                    logger.warning(f"Watch-stream for {resource} {where} failed; "
                                   f"reconnecting in {backoff}s: {e!r}")
            await asyncio.sleep(backoff)
    finally:
        logger.debug(f"Stopping the watch-stream for {resource} {where}.")


@contextlib.asynccontextmanager
async def streaming_block(
        *,
        resource: references.Resource,
        namespace: references.Namespace,
        operator_paused: aiotoggles.ToggleSet | None = None,  # None for tests & observation
) -> AsyncIterator[aiotasks.Future]:
    """
    Block the execution until un-paused; signal when it is active again.

    This prevents both watching and listing while the operator is paused,
    until it is off. Specifically, the watch-stream closes its connection
    once paused, so the while-true & for-event-in-stream cycles exit,
    and the streaming coroutine is started again by ``infinite_stream()``
    (the watcher timeout is swallowed by the pause time).

    Returns a future (or a task) that is set (or finished) when paused again.

    A stop-future is a client-specific way of terminating the streaming HTTPS
    connections when paused again. The low-level streaming API call attaches
    its ``response.close()`` to the future's "done" callback,
    so that the stream is closed once the operator is paused.

    Note: this routine belongs to watching and does not belong to peering.
    The pause can be managed in any other ways: as an imaginary edge case,
    imagine an operator with UI with a "pause" button that pauses the operator.
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
) -> AsyncIterator[Bookmark | bodies.RawEvent]:

    # NB: it must remain None for the server-side initial streaming.
    resource_version: str | None = None
    in_initial_streaming: bool = settings.watching.initial_streaming

    # First, list the resources in chunks, and get the list's resource version.
    # Simulate the events with type "None" event - used in detection of causes.
    if not settings.watching.initial_streaming:
        async for chunk, resource_version in fetching.fetch_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        ):
            for obj in chunk:
                yield {'type': None, 'object': obj}
            del chunk  # optional optimization: keep only one chunk in memory

        # Notify the watcher that the initial listing is over, even if there was nothing yielded.
        yield Bookmark.LISTED

    # Repeat through disconnects of the watch as long as the resource version is valid (no errors).
    # The individual watching API calls are disconnected by timeout even if the stream is fine.
    while not operator_pause_waiter.done():

        # Then, watch the resources starting from the list's resource version.
        # Close the stream deterministically on restarts, not whenever it is garbage-collected.
        async with contextlib.aclosing(watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since=resource_version,
            operator_pause_waiter=operator_pause_waiter,
        )) as stream:
            async for raw_input in stream:
                raw_type = raw_input['type']
                raw_object = raw_input['object']

                # "410 Gone" is for the "resource version too old" error, we must restart watching.
                # The resource versions are lost by k8s after a few minutes (5 as per the docs).
                # The error occurs when there is nothing happening for a few minutes. It is normal.
                if raw_type == 'ERROR' and cast(bodies.RawError, raw_object)['code'] == 410:
                    where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'
                    logger.debug(f"Restarting the watch-stream for {resource} {where}.")
                    return  # out of the regular stream, to the infinite stream.

                # "429 Too Many Requests" and "5xx" are temporary server-side failures (e.g., while
                # the API server re-initializes its storage). "404 Not Found" happens when a CRD
                # is deleted before the orchestrator stops this watcher. Escalate them as
                # API errors, so that the infinite stream reconnects with a backoff.
                if raw_type == 'ERROR':
                    status = cast(errors.RawStatus, raw_object)
                    code = status['code']
                    if code == 404:
                        raise errors.APINotFoundError(status, status=code, headers={})
                    if code == 429:
                        raise errors.APITooManyRequestsError(status, status=code, headers={})
                    if 500 <= code < 600:
                        raise errors.APIServerError(status, status=code, headers={})

                    # Other watch errors (including authentication & authorization) are fatal,
                    # so that the operator is restarted with fresh credentials & permissions.
                    raise WatchingError(f"Error in the watch-stream: {raw_object}")

                # Ensure that the event is something we understand and can handle.
                if raw_type not in ['ADDED', 'MODIFIED', 'DELETED', 'BOOKMARK']:
                    logger.warning(f"Ignoring an unsupported event type: {raw_input!r}")
                    continue

                # While in the initial streaming, enforce Kopf's convention for type=None:
                # this is used for skipping such initial events
                # (process_discovered_resource_event()).
                # The very first bookmark ends the initial streaming (see K8s docs).
                if in_initial_streaming:
                    if raw_type == 'ADDED':
                        raw_input['type'] = None
                    if raw_type == 'BOOKMARK':
                        in_initial_streaming = False
                        # Before the raw event itself; as if after fetch_objs().
                        yield Bookmark.LISTED

                # Keep the latest seen resource version to continue the stream on disconnects.
                body = cast(bodies.RawBody, raw_object)
                resource_version = body.get('metadata', {}).get('resourceVersion', resource_version)

                # Yield normal events to the consumer. Errors are already filtered out.
                yield cast(bodies.RawEvent, raw_input)


async def watch_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        since: str | None = None,
        operator_pause_waiter: aiotasks.Future,
) -> AsyncGenerator[bodies.RawInput, None]:
    """
    Watch objects of a specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    # Deduplicate, then sort it to make it somewhat predictable, just for the beauty of logs.
    # NB1: this also applies to namespaces & CRDs in the watch-streams of the root observer.
    # NB2: it is mirrored by the same logic & query filters in the listing operation.
    label_selector = ','.join(sorted(set(settings.watching.label_selectors.collect(resource))))
    field_selector = ','.join(sorted(set(settings.watching.field_selectors.collect(resource))))
    shard_selector, *_ = settings.watching.shard_selectors.collect(resource) or ['']

    params: dict[str, str] = {}
    params['watch'] = 'true'
    params['allowWatchBookmarks'] = 'true'
    if label_selector:
        params['labelSelector'] = label_selector
    if field_selector:
        params['fieldSelector'] = field_selector
    if shard_selector:
        params['shardSelector'] = shard_selector
    if since is not None:
        params['resourceVersion'] = since
    if settings.watching.server_timeout is not None:
        params['timeoutSeconds'] = str(settings.watching.server_timeout)
    if since is None and settings.watching.initial_streaming:
        # NB: strictly in initial watch-streams, not in continuations/reconnections.
        params['sendInitialEvents'] = 'true'
        params['resourceVersionMatch'] = 'NotOlderThan'

    connect_timeout = (
        settings.watching.connect_timeout if settings.watching.connect_timeout is not None else
        settings.networking.connect_timeout if settings.networking.connect_timeout is not None else
        settings.networking.request_timeout
    )

    # Stream the parsed events from the response until it is closed server-side,
    # or until it is closed client-side by the pause-waiting future's callbacks.
    # The inactivity timeout covers only the wait for the next event; if no event arrives
    # within the window, asyncio.timeout() cancels the outer task and we log & return.
    # NB: never yield inside asyncio.timeout(): the consumer's processing time would count
    # as inactivity, and the cancellation would hit the consumer, not this generator (PEP 789).
    # The expired client-side timeout and the disconnects of a stream that has already
    # delivered events (including a truncated final event) are normal: the caller continues
    # from the last resource version. A stream that fails before its first event is escalated,
    # to reconnect with a backoff: e.g. a proxy closing every response cannot cause a busy loop.
    where = f'in {namespace!r}' if namespace is not None else 'cluster-wide'
    started = asyncio.get_running_loop().time()
    received = False
    async with contextlib.aclosing(api.stream(
            url=resource.get_url(namespace=namespace, params=params),
            logger=logger,
            settings=settings,
            stopper=operator_pause_waiter,
            timeout=aiohttp.ClientTimeout(
                total=settings.watching.client_timeout,
                sock_connect=connect_timeout,
            ),
    )) as stream:
        while True:
            timeout = asyncio.timeout(settings.watching.inactivity_timeout)
            try:
                async with timeout:
                    try:
                        raw_input = await anext(stream)
                    except StopAsyncIteration:
                        break
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError,
                    json.JSONDecodeError, TimeoutError) as e:
                elapsed = asyncio.get_running_loop().time() - started
                client_timeout = settings.watching.client_timeout
                if timeout.expired():
                    logger.debug(f"Watch-stream for {resource} {where} is inactive, reconnecting.")
                elif client_timeout is not None and client_timeout > 0 and elapsed >= client_timeout:
                    logger.debug(f"Watch-stream for {resource} {where} has expired, reconnecting.")
                elif received:
                    logger.debug(f"Watch-stream for {resource} {where} is disconnected, "
                                 f"reconnecting: {e!r}")
                else:
                    raise
                break
            received = True
            yield raw_input
