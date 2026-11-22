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

from kopf._cogs.clients import watching
from kopf._cogs.clients.errors import APINotFoundError, APIServerError, APITooManyRequestsError
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
    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]
    assert len(kmock['list']) == 1
    assert len(kmock['watch']) == 1
    assert len(events) == 1
    assert events[0] == Bookmark.LISTED


async def test_pause_during_initial_listing_yields_nothing(
        monkeypatch, settings, resource, namespace):
    operator_pause_waiter = asyncio.Future()

    async def fetch_objs(**kwargs):
        operator_pause_waiter.set_result(None)
        yield ([{'metadata': {'resourceVersion': '100'}}], '100')

    monkeypatch.setattr(watching.fetching, 'fetch_objs', fetch_objs)

    events = [event async for event in continuous_watch(
        settings=settings,
        resource=resource,
        namespace=namespace,
        operator_pause_waiter=operator_pause_waiter,
    )]

    assert events == []


async def test_pause_during_initial_streaming_resets_the_snapshot(
        monkeypatch, settings, resource, namespace):
    settings.watching.initial_streaming = True
    operator_pause_waiter = asyncio.Future()

    # The pause closes the response, so the stream ends without any further event.
    async def watch_objs(**kwargs):
        yield {'type': 'ADDED', 'object': {'metadata': {'resourceVersion': '100'}}}
        operator_pause_waiter.set_result(None)

    monkeypatch.setattr(watching, 'watch_objs', watch_objs)

    events = [event async for event in continuous_watch(
        settings=settings,
        resource=resource,
        namespace=namespace,
        operator_pause_waiter=operator_pause_waiter,
    )]

    assert events[-1] is Bookmark.RESET


async def test_initial_streaming_does_not_fetch(
        kmock, settings, resource, namespace):
    settings.watching.initial_streaming = True
    kmock['list', resource, kmock.namespace(namespace)] << {'items': [{'spec': 'unseen'}]}
    kmock['watch', resource, kmock.namespace(namespace)] << EOS

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    assert len(kmock['list']) == 0
    assert len(kmock['watch']) == 1
    assert len(events) == 0


async def test_event_stream_yields_everything(kmock, settings, resource, namespace):
    kmock['list', resource, kmock.namespace(namespace)] << {'items': [{'spec': 'x'}]}
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_NORMAL_EVENTS << EOS

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    assert len(kmock['list']) == 1
    assert len(kmock['watch']) == 1
    assert len(events) == 4
    assert events[1] == Bookmark.LISTED
    assert events[0]['type'] is None
    assert events[2]['type'] == 'ADDED'
    assert events[3]['type'] == 'ADDED'
    assert events[0]['object']['spec'] == 'x'
    assert events[2]['object']['spec'] == 'a'
    assert events[3]['object']['spec'] == 'b'


async def test_initial_streaming_resets_types_to_none(
        kmock, settings, resource, namespace):
    settings.watching.initial_streaming = True
    kmock['watch', resource, kmock.namespace(namespace)] << (
        STREAM_WITH_NORMAL_EVENTS,
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': 'xyz'}}},
        STREAM_WITH_NORMAL_EVENTS,
        EOS,
    )

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    # Same inputs in the stream, different types in yields, but the same payloads.
    assert len(events) == 6
    assert events[0]['type'] is None
    assert events[1]['type'] is None
    assert events[4]['type'] == 'ADDED'
    assert events[5]['type'] == 'ADDED'
    assert events[0]['object']['spec'] == 'a'
    assert events[1]['object']['spec'] == 'b'
    assert events[4]['object']['spec'] == 'a'
    assert events[5]['object']['spec'] == 'b'


async def test_unknown_event_type_ignored(kmock, settings, resource, namespace, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_UNKNOWN_EVENT << EOS

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    assert len(events) == 3
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert events[2]['object']['spec'] == 'b'
    assert_logs(["Ignoring an unsupported event type"])
    assert_logs(["UNKNOWN"])


async def test_error_410gone_exits_normally(kmock, settings, resource, namespace, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_410GONE << EOS

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    assert len(events) == 2
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert_logs(["Restarting the watch-stream"])


# Temporary errors escalate as API errors, so that infinite_watch() reconnects with a backoff.
@pytest.mark.parametrize('code, exctype', [
    (404, APINotFoundError),
    (429, APITooManyRequestsError),
    (500, APIServerError),
    (502, APIServerError),
    (503, APIServerError),
    (504, APIServerError),
    (599, APIServerError),
])
async def test_temporary_error_escalates(kmock, settings, resource, namespace, code, exctype):
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'spec': 'a'}},
        {'type': 'ERROR', 'object': {'code': code, 'details': {'retryAfterSeconds': 7}}},
        {'type': 'ADDED', 'object': {'spec': 'b'}},
    ) << EOS

    events = []
    with pytest.raises(exctype) as e:
        async for event in continuous_watch(settings=settings,
                                            resource=resource,
                                            namespace=namespace,
                                            operator_pause_waiter=asyncio.Future()):
            events.append(event)

    assert len(events) == 2
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert e.value.status == code
    assert e.value.retry_after == 7


# Authentication and authorization errors are never restarted in place: the operator must fail,
# so that it is restarted as a whole and reads its credentials and permissions afresh.
# 400 is a client-side request error; retrying it would not help.
@pytest.mark.parametrize('code', [400, 401, 403, 499, 600])
async def test_other_errors_raise_exception(kmock, settings, resource, namespace, code):
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': {'spec': 'a'}},
        {'type': 'ERROR', 'object': {'code': code}},
        {'type': 'ADDED', 'object': {'spec': 'b'}},
    ) << EOS

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
    assert str(code) in str(e.value)


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

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

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
async def test_list_objs_transport_errors_escalate(
        settings, resource, namespace, enforced_session, mocker, connection_error):
    enforced_session.request = mocker.Mock(side_effect=connection_error())

    with pytest.raises(connection_error):
        async for _ in continuous_watch(settings=settings,
                                        resource=resource,
                                        namespace=namespace,
                                        operator_pause_waiter=asyncio.Future()):
            pass
