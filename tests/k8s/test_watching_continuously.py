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

import pytest

from kopf.clients.watching import Bookmark, WatchingError, continuous_watch

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


async def test_empty_stream_yields_nothing(
        settings, resource, stream, namespace):

    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 1
    assert events[0] == Bookmark.LISTED


async def test_event_stream_yields_everything(
        settings, resource, stream, namespace):

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 3
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'


async def test_unknown_event_type_ignored(
        settings, resource, stream, namespace, caplog):
    caplog.set_level(logging.DEBUG)

    stream.feed(STREAM_WITH_UNKNOWN_EVENT, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 3
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'
    assert "Ignoring an unsupported event type" in caplog.text
    assert "UNKNOWN" in caplog.text


async def test_error_410gone_exits_normally(
        settings, resource, stream, namespace, caplog):
    caplog.set_level(logging.DEBUG)

    stream.feed(STREAM_WITH_ERROR_410GONE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 2
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert "Restarting the watch-stream" in caplog.text


async def test_unknown_error_raises_exception(
        settings, resource, stream, namespace):

    stream.feed(STREAM_WITH_ERROR_CODE, namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(WatchingError) as e:
        async for event in continuous_watch(settings=settings,
                                            resource=resource,
                                            namespace=namespace,
                                            operator_pause_waiter=asyncio.Future()):
            events.append(event)

    assert len(events) == 2
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert '666' in str(e.value)


async def test_exception_escalates(
        settings, resource, stream, namespace, enforced_session, mocker):

    enforced_session.get = mocker.Mock(side_effect=SampleException())
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(SampleException):
        async for event in continuous_watch(settings=settings,
                                            resource=resource,
                                            namespace=namespace,
                                            operator_pause_waiter=asyncio.Future()):
            events.append(event)

    assert len(events) == 0


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
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 4
    assert events[0] == Bookmark.LISTED
    assert len(events[1]['object']['spec']['field']) == 1
    assert len(events[2]['object']['spec']['field']) == 2 * 1024 * 1024
    assert len(events[3]['object']['spec']['field']) == 4 * 1024 * 1024
