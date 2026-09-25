"""
Tests for the inactivity timeout in watch_objs().

When no events (including bookmarks) arrive within settings.watching.inactivity_timeout,
watch_objs() closes the stream and returns, causing continuous_watch() to reconnect.
"""
import asyncio

from kopf._cogs.clients.watching import Bookmark, continuous_watch, watch_objs


async def test_empty_stream_times_out(settings, resource, namespace, kmock, looptime, assert_logs):
    """A watch stream that sends no events at all is closed after the inactivity timeout."""
    settings.watching.inactivity_timeout = 15.0
    kmock['watch', resource, kmock.namespace(namespace)] << (lambda: asyncio.sleep(99))

    events = []
    async for event in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='100',
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert events == []
    assert looptime == 15
    assert_logs([r"Watch-stream.*inactive.*reconnecting"])


async def test_stalled_stream_times_out(settings, resource, namespace, kmock, looptime, assert_logs):
    """A watch stream that stalls after some events is closed after the inactivity timeout."""
    settings.watching.inactivity_timeout = 15.0
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '101'}}},
        lambda: asyncio.sleep(99),
    )

    events = []
    async for event in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='100',
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 1
    assert events[0]['type'] == 'ADDED'
    assert looptime == 15
    assert_logs([r"Watch-stream.*inactive.*reconnecting"])


async def test_bookmark_event_resets_inactivity_timer(settings, resource, namespace, kmock, looptime, assert_logs):
    """A BOOKMARK event resets the inactivity timer, just like any other event."""
    settings.watching.inactivity_timeout = 30.0
    kmock['watch', resource, kmock.namespace(namespace)] << (
        lambda: asyncio.sleep(15),
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '101'}}},
        lambda: asyncio.sleep(99)
    )

    events = []
    async for event in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='100',
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 1
    assert events[0]['type'] == 'BOOKMARK'
    assert looptime == 15 + 30  # 15s before the bookmark + 30s of inactivity after it
    assert_logs([r"Watch-stream.*inactive.*reconnecting"])


async def test_active_stream_does_not_time_out(settings, resource, namespace, kmock, looptime):
    """A stream delivering events within the timeout window is not closed prematurely."""
    settings.watching.inactivity_timeout = 15.0
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '101'}}},
        lambda: asyncio.sleep(10),  # 10s < 15s timeout: well within the window
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '102'}}},

    )

    events = []
    async for event in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='100',
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 2
    assert looptime == 10  # only the inter-event sleep, no inactivity timeout


async def test_inactivity_reconnects_with_last_resource_version(
        settings, resource, namespace, kmock, looptime):
    """After an inactivity timeout, continuous_watch() reconnects from the last seen resource version."""
    settings.watching.inactivity_timeout = 15.0
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }

    # First watch: advances resource version via a bookmark, then hangs.
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion='100')] << (
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '200'}}},
        lambda: asyncio.sleep(99),
    )

    # Second watch: must be called with the bookmark's resource version, not the list's.
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion='200')] << (
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert looptime == 15
    assert Bookmark.LISTED in events
    assert any(
        isinstance(e, dict) and e.get('type') == 'BOOKMARK'
        and e.get('object', {}).get('metadata', {}).get('resourceVersion') == '200'
        for e in events
    )
