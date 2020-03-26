"""
Only the tests from the K8s client library (simulated) to the event stream.

Excluded: the watch-stream consumption and queue multiplexing routines
(see ``tests_queueing.py``).

Used for internal control that the event multiplexing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.
"""
import asyncio
import logging

import aiohttp
import pytest

from kopf.clients.watching import streaming_watch, infinite_watch, WatchingError
from kopf.structs.primitives import Toggle

STREAM_WITH_NORMAL_EVENTS = [
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
]
STREAM_WITH_UNKNOWN_EVENT = [
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'UNKNOWN', 'object': {}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
]
STREAM_WITH_ERROR_410GONE = [
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ERROR', 'object': {'code': 410}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
]
STREAM_WITH_ERROR_CODE = [
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ERROR', 'object': {'code': 666}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
]


class SampleException(Exception):
    pass


@pytest.fixture(params=[
    pytest.param('something', id='namespace'),
    pytest.param(None, id='cluster'),
])
def namespace(request):
    return request.param


async def test_empty_stream_yields_nothing(settings, resource, stream, namespace):
    
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(settings=settings,
                                       resource=resource,
                                       namespace=namespace):
        events.append(event)

    assert len(events) == 0


async def test_event_stream_yields_everything(
        settings, resource, stream, namespace):

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(settings=settings,
                                       resource=resource,
                                       namespace=namespace):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'


async def test_unknown_event_type_ignored(
        settings, resource, stream, namespace, caplog):

    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_UNKNOWN_EVENT, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(settings=settings,
                                       resource=resource,
                                       namespace=namespace):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'
    assert "Ignoring an unsupported event type" in caplog.text
    assert "UNKNOWN" in caplog.text


async def test_error_410gone_exits_normally(
        settings, resource, stream, namespace, caplog):

    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_ERROR_410GONE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(settings=settings,
                                       resource=resource,
                                       namespace=namespace):
        events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert "Restarting the watch-stream" in caplog.text


async def test_unknown_error_raises_exception(
        settings, resource, stream, namespace):

    stream.feed(STREAM_WITH_ERROR_CODE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(WatchingError) as e:
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace):
            events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert '666' in str(e.value)


async def test_exception_escalates(
        settings, resource, stream, namespace, enforced_session, mocker):

    enforced_session.get = mocker.Mock(side_effect=SampleException())
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(SampleException):
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace):
            events.append(event)

    assert len(events) == 0


async def test_freezing_is_ignored_if_turned_off(
        settings, resource, stream, namespace, timer, caplog, assert_logs):

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(False)
    events = []

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    caplog.set_level(logging.DEBUG)
    with timer:
        await asyncio.wait_for(read_stream(), timeout=0.5)

    assert len(events) == 2
    assert timer.seconds < 0.2  # no waits, exits as soon as possible
    assert_logs([], prohibited=[
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_forever_if_not_resumed(
        settings, resource, stream, namespace, timer, caplog, assert_logs):

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(True)
    events = []

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    caplog.set_level(logging.DEBUG)
    with timer:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(read_stream(), timeout=0.5)

    assert len(events) == 0
    assert timer.seconds >= 0.5
    assert_logs([
        r"Freezing the watch-stream for",
    ], prohibited=[
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_until_resumed(
        settings, resource, stream, namespace, timer, caplog, assert_logs):

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(True)
    events = []

    async def delayed_resuming(delay: float):
        await asyncio.sleep(delay)
        await freeze_mode.turn_off()

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    caplog.set_level(logging.DEBUG)
    with timer:
        asyncio.create_task(delayed_resuming(0.2))
        await asyncio.wait_for(read_stream(), timeout=1.0)

    assert len(events) == 2
    assert timer.seconds >= 0.2
    assert timer.seconds <= 0.5
    assert_logs([
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])


async def test_infinite_watch_never_exits_normally(
        settings, resource, stream, namespace, aresponses):

    error = aresponses.Response(status=555, reason='stop-infinite-cycle')
    stream.feed(
        STREAM_WITH_ERROR_410GONE,          # watching restarted
        STREAM_WITH_UNKNOWN_EVENT,          # event ignored
        error,                              # to finally exit it somehow
        namespace=namespace,
    )
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(aiohttp.ClientResponseError) as e:
        async for event in infinite_watch(settings=settings,
                                          resource=resource,
                                          namespace=namespace):
            events.append(event)

    assert e.value.status == 555
    assert e.value.message == 'stop-infinite-cycle'

    assert len(events) == 3
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'


# See: See: https://github.com/zalando-incubator/kopf/issues/275
async def test_long_line_parsing(
        settings, resource, stream, namespace, aresponses):

    content = [
        {'type': 'ADDED', 'object': {'spec': {'field': 'x'}}},
        {'type': 'ADDED', 'object': {'spec': {'field': 'y' * (2 * 1024 * 1024)}}},
        {'type': 'ADDED', 'object': {'spec': {'field': 'z' * (4 * 1024 * 1024)}}},
    ]
    stream.feed(content, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(settings=settings,
                                       resource=resource,
                                       namespace=namespace):
        events.append(event)

    assert len(events) == 3
    assert len(events[0]['object']['spec']['field']) == 1
    assert len(events[1]['object']['spec']['field']) == 2 * 1024 * 1024
    assert len(events[2]['object']['spec']['field']) == 4 * 1024 * 1024
