"""
Only the tests from the K8s client library (simulated) to the event stream.

Excluded: the watch-stream consumption and queue multiplexing routines
(see ``tests_queueing.py``).

Used for internal control that the event multiplexing works are intended.
If the intentions change, the tests should be rewritten.
They are NOT part of the public interface of the framework.
"""
import asyncio

import aiohttp
import pytest

from kopf._cogs.clients.watching import Bookmark, WatchingError, continuous_watch

STREAM_WITH_ERROR_410GONE_ONLY = (
    {'type': 'ERROR', 'object': {'code': 410}},
)
STREAM_WITH_NORMAL_EVENTS = (
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
)
STREAM_WITH_UNKNOWN_EVENT = (
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'UNKNOWN', 'object': {}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
)
STREAM_WITH_ERROR_410GONE = (
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ERROR', 'object': {'code': 410}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
)
STREAM_WITH_ERROR_CODE = (
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ERROR', 'object': {'code': 666}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
)
EOS = ({'type': 'ERROR', 'object': {'code': 410}},)


@pytest.fixture(autouse=True)
def _stubs(kmock, resource):
    # The watch-stream makes an initial listing, so ensure there is a dummy response instead of 404.
    # Also ensure the continuous watcher terminates in the end (one "410 Gone" guarantees this).
    (kmock['list', resource] ** -60) << {'items': []}
    (kmock['watch', resource] ** -60) << STREAM_WITH_ERROR_410GONE_ONLY


class SampleException(Exception):
    pass


async def test_empty_stream_yields_nothing(kmock, settings, resource, namespace):

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 1
    assert events[0] == Bookmark.LISTED


async def test_event_stream_yields_everything(kmock, settings, resource, namespace):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_NORMAL_EVENTS << EOS

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


async def test_unknown_event_type_ignored(kmock, settings, resource, namespace, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_UNKNOWN_EVENT << EOS

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
    assert_logs(["Ignoring an unsupported event type"])
    assert_logs(["UNKNOWN"])


async def test_error_410gone_exits_normally(kmock, settings, resource, namespace, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_410GONE << EOS

    events = []
    async for event in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 2
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert_logs(["Restarting the watch-stream"])


async def test_unknown_error_raises_exception(kmock, settings, resource, namespace):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_CODE << EOS

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


async def test_exception_escalates(kmock, settings, resource, namespace, enforced_session, mocker):
    enforced_session.request = mocker.Mock(side_effect=SampleException())
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    with pytest.raises(SampleException):
        async for event in continuous_watch(settings=settings,
                                            resource=resource,
                                            namespace=namespace,
                                            operator_pause_waiter=asyncio.Future()):
            events.append(event)

    assert len(events) == 0


# See: See: https://github.com/zalando-incubator/kopf/issues/275
async def test_long_line_parsing(kmock, settings, resource, namespace):
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'spec': {'field': 'x'}}},
        {'type': 'ADDED', 'object': {'spec': {'field': 'y' * (2 * 1024 * 1024)}}},
        {'type': 'ADDED', 'object': {'spec': {'field': 'z' * (4 * 1024 * 1024)}}},
    ) << EOS

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

@pytest.mark.parametrize("connection_error",
    [
        aiohttp.ClientConnectionError,
        aiohttp.ClientPayloadError,
        asyncio.TimeoutError
    ]
)
async def test_list_objs_connection_errors_are_caught(
        settings, resource, namespace, enforced_session, mocker, connection_error):
    enforced_session.request = mocker.Mock(side_effect=connection_error())

    events = []
    async for event in continuous_watch(settings=settings,
                                            resource=resource,
                                            namespace=namespace,
                                            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert len(events) == 0
