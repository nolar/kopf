"""
Only the tests from the K8s client library (simulated) to the event stream.

Excluded: the watch-stream consumption and queue multiplexing routines
(see ``tests_queueing.py``).

Used for internal control that the event multiplexing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.
"""
import logging

import aiohttp
import pytest

from kopf.clients.watching import streaming_watch, infinite_watch, WatchingError

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


async def test_empty_stream_yields_nothing(resource, stream, namespace):
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(resource=resource, namespace=namespace):
        events.append(event)

    assert len(events) == 0


async def test_event_stream_yields_everything(resource, stream, namespace):
    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(resource=resource, namespace=namespace):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'


async def test_unknown_event_type_ignored(resource, stream, namespace, caplog):
    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_UNKNOWN_EVENT, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(resource=resource, namespace=namespace):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'
    assert "Ignoring an unsupported event type" in caplog.text
    assert "UNKNOWN" in caplog.text


async def test_error_410gone_exits_normally(resource, stream, namespace, caplog):
    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_ERROR_410GONE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in streaming_watch(resource=resource, namespace=namespace):
        events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert "Restarting the watch-stream" in caplog.text


async def test_unknown_error_raises_exception(resource, stream, namespace):
    stream.feed(STREAM_WITH_ERROR_CODE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(WatchingError) as e:
        async for event in streaming_watch(resource=resource, namespace=namespace):
            events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert '666' in str(e.value)


async def test_exception_escalates(resource, stream, namespace, enforced_session, mocker):
    enforced_session.get = mocker.Mock(side_effect=SampleException())
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(SampleException):
        async for event in streaming_watch(resource=resource, namespace=namespace):
            events.append(event)

    assert len(events) == 0


async def test_infinite_watch_never_exits_normally(resource, stream, namespace, aresponses):
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
        async for event in infinite_watch(resource=resource, namespace=namespace):
            events.append(event)

    assert e.value.status == 555
    assert e.value.message == 'stop-infinite-cycle'

    assert len(events) == 3
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'
