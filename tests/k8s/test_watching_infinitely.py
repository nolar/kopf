import pytest

from kopf._cogs.clients.errors import APIClientError, APIError
from kopf._cogs.clients.watching import Bookmark, infinite_watch

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


class SampleException(Exception):
    pass


async def test_exception_escalates(kmock, settings, resource, namespace, enforced_session, mocker):
    enforced_session.request = mocker.Mock(side_effect=SampleException())
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    with pytest.raises(SampleException):
        async for event in infinite_watch(settings=settings,
                                          resource=resource,
                                          namespace=namespace,
                                          _iterations=1):
            events.append(event)

    assert len(events) == 0


async def test_infinite_watch_never_exits_normally(kmock, settings, resource, namespace):
    kmock['watch', resource, kmock.namespace(namespace)] << iter(STREAM_WITH_ERROR_410GONE)
    kmock['watch', resource, kmock.namespace(namespace)] << iter(STREAM_WITH_UNKNOWN_EVENT)
    kmock['watch', resource, kmock.namespace(namespace)] << 555

    events = []
    with pytest.raises(APIError) as e:
        async for event in infinite_watch(settings=settings,
                                          resource=resource,
                                          namespace=namespace):
            events.append(event)

    assert e.value.status == 555

    assert len(events) == 5
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert events[2] == Bookmark.LISTED
    assert events[3]['object']['spec'] == 'a'
    assert events[4]['object']['spec'] == 'b'


async def test_too_many_requests_exception(
        kmock, settings, resource, namespace, enforced_session, mocker):

    exc = APIClientError({
        "apiVersion": "v1",
        "code": 429,
        "status": "Failure",
        "details": {
            "retryAfterSeconds": 1,
        }
    }, status=429)
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    async for event in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
        events.append(event)

    assert len(events) == 0
