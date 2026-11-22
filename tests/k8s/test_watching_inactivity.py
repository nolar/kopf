"""
Tests for the inactivity timeout in watch_objs().

When no events (including bookmarks) arrive within settings.watching.inactivity_timeout,
watch_objs() closes the stream and returns, causing continuous_watch() to reconnect.
"""
import asyncio

from kopf._cogs.clients import watching
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


async def test_slow_consumer_does_not_time_out(settings, resource, namespace, kmock, looptime):
    """The time spent by the consumer between events is not the stream's inactivity."""
    settings.watching.inactivity_timeout = 15.0
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '101'}}},
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
        await asyncio.sleep(20)  # 20s > 15s timeout: the consumer is slower than the timeout

    assert len(events) == 2
    assert looptime == 40  # only the consumer's sleeps, no inactivity timeout


async def test_stream_is_closed_when_consumer_stops(
        monkeypatch, settings, resource, namespace):
    closed = asyncio.Event()

    async def stream(**kwargs):
        try:
            yield {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '101'}}}
            await asyncio.Event().wait()
        finally:
            closed.set()

    monkeypatch.setattr(watching.api, 'stream', stream)
    events = watch_objs(
        settings=settings,
        resource=resource,
        namespace=namespace,
        since='100',
        operator_pause_waiter=asyncio.Future(),
    )

    await anext(events)
    await events.aclose()

    assert closed.is_set()


async def test_abandoned_stream_does_not_cancel_the_next_one(
        settings, resource, namespace, kmock, looptime):
    """
    The inactivity timer of an abandoned stream must not cancel a later, healthy stream.

    The first stream is started, so its timer is armed, and it delivers one event. Then nobody
    reads it any more: its generator stays suspended at that event and is not closed. The timer
    would elapse at 70s. The second stream is healthy and runs for 100s, so it is alive then.

    Before the fix, the timer of the first stream stayed armed while its generator was suspended.
    At 70s it cancelled the task of the consumer, which was then reading the second stream.
    """
    settings.watching.inactivity_timeout = 70.0
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion='100')] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '101'}}},
    )
    kmock['watch', resource, kmock.namespace(namespace), kmock.params(resourceVersion='101')] << (
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '201'}}},
        lambda: asyncio.sleep(50),
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '202'}}},
        lambda: asyncio.sleep(50),
        {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '203'}}},
    )

    abandoned = watch_objs(settings=settings, resource=resource, namespace=namespace,
                           since='100', operator_pause_waiter=asyncio.Future())
    try:
        first = await anext(abandoned)  # suspended at this event; nobody reads it any further
        assert first['object']['metadata']['resourceVersion'] == '101'
        healthy = [event async for event in watch_objs(
            settings=settings, resource=resource, namespace=namespace,
            since='101', operator_pause_waiter=asyncio.Future())]
    except asyncio.CancelledError:
        pytest.fail("The consumer was cancelled while it was reading the healthy stream.")
    finally:
        await abandoned.aclose()  # closes the stream and disarms its timer; no leftovers

    assert [e['object']['metadata']['resourceVersion'] for e in healthy] == ['201', '202', '203']
    assert looptime == 100


async def test_restarted_stream_is_closed(monkeypatch, settings, resource, namespace, kmock):
    """When continuous_watch() restarts on "410 Gone", the abandoned stream is closed at once."""
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }
    closed = asyncio.Event()

    async def stream(**kwargs):
        try:
            yield {'type': 'ERROR', 'object': {'code': 410}}
            await asyncio.Event().wait()
        finally:
            closed.set()

    monkeypatch.setattr(watching.api, 'stream', stream)
    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert events == [Bookmark.LISTED]
    assert closed.is_set()


async def test_restarted_stream_does_not_cancel_the_next_one(
        monkeypatch, settings, resource, namespace, kmock, looptime):
    """
    An abandoned stream must not cancel the consumer when its inactivity timeout elapses.

    A server-side restart of the API server's storage closes the watch-streams. The next stream
    gets "410 Gone" at once and is abandoned. The following stream is healthy. If the abandoned
    stream's inactivity timeout were still armed, it would cancel the healthy one silently later.
    """
    settings.watching.inactivity_timeout = 70.0
    original_watch_objs = watching.watch_objs
    streams = []

    def watch_objs(**kwargs):
        streams.append(original_watch_objs(**kwargs))  # hold it as a delayed garbage collection
        return streams[-1]

    monkeypatch.setattr(watching, 'watch_objs', watch_objs)
    kmock['watch', resource, kmock.namespace(namespace)] << iter((
        {'type': 'ERROR', 'object': {'code': 410}},
    ))
    kmock['watch', resource, kmock.namespace(namespace)] << iter((
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '101'}}},
        lambda: asyncio.sleep(50),
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '102'}}},
        lambda: asyncio.sleep(50),
        {'type': 'ERROR', 'object': {'code': 410}},
    ))

    events = []
    async for event in watching.infinite_watch(settings=settings,
                                               resource=resource,
                                               namespace=namespace,
                                               _iterations=2):
        events.append(event)

    # The healthy stream is served fully, beyond the abandoned timeout; one backoff per restart.
    assert looptime == 100 + 2 * settings.watching.reconnect_backoff
    assert len(events) == 4
    assert events[0] == Bookmark.LISTED
    assert events[1] == Bookmark.LISTED
    assert events[2]['object']['metadata']['resourceVersion'] == '101'
    assert events[3]['object']['metadata']['resourceVersion'] == '102'


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
