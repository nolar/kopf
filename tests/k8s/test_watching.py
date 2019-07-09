"""
Only the tests from the K8s client library (simulated) to the event stream.

Excluded: the watch-stream consumption and queue multiplexing routines
(see ``tests_queueing.py``).

Used for internal control that the event multiplexing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.
"""
import logging

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


async def test_empty_stream_yields_nothing(resource, stream):
    stream.feed([])

    events = []
    async for event in streaming_watch(resource=resource, namespace=None):
        events.append(event)

    assert len(events) == 0


async def test_event_stream_yields_everything(resource, stream):
    stream.feed(STREAM_WITH_NORMAL_EVENTS)

    events = []
    async for event in streaming_watch(resource=resource, namespace=None):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'


async def test_unknown_event_type_ignored(resource, stream, caplog):
    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_UNKNOWN_EVENT)

    events = []
    async for event in streaming_watch(resource=resource, namespace=None):
        events.append(event)

    assert len(events) == 2
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'
    assert "Ignoring an unsupported event type" in caplog.text
    assert "UNKNOWN" in caplog.text


async def test_error_410gone_exits_normally(resource, stream, caplog):
    caplog.set_level(logging.DEBUG)
    stream.feed(STREAM_WITH_ERROR_410GONE)

    events = []
    async for event in streaming_watch(resource=resource, namespace=None):
        events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert "Restarting the watch-stream" in caplog.text


async def test_unknown_error_raises_exception(resource, stream):
    stream.feed(STREAM_WITH_ERROR_CODE)

    events = []
    with pytest.raises(WatchingError) as e:
        async for event in streaming_watch(resource=resource, namespace=None):
            events.append(event)

    assert len(events) == 1
    assert events[0]['object']['spec'] == 'a'
    assert '666' in str(e.value)


async def test_exception_escalates(resource, stream):
    stream.feed(SampleException())

    events = []
    with pytest.raises(SampleException):
        async for event in streaming_watch(resource=resource, namespace=None):
            events.append(event)

    assert len(events) == 0


async def test_infinite_watch_never_exits_normally(resource, stream):
    stream.feed(
        STREAM_WITH_ERROR_410GONE,          # watching restarted
        STREAM_WITH_UNKNOWN_EVENT,          # event ignored
        SampleException(),                  # to finally exit it somehow
    )

    events = []
    with pytest.raises(SampleException):
        async for event in infinite_watch(resource=resource, namespace=None):
            events.append(event)

    assert len(events) == 3
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'
