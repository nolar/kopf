import pytest

from kopf.clients.errors import APIError
from kopf.clients.watching import Bookmark, infinite_watch

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


class SampleException(Exception):
    pass


async def test_exception_escalates(
        settings, resource, stream, namespace, enforced_session, mocker):

    enforced_session.get = mocker.Mock(side_effect=SampleException())
    stream.feed([], namespace=namespace)
    stream.close(namespace=namespace)

    events = []
    with pytest.raises(SampleException):
        async for event in infinite_watch(settings=settings,
                                          resource=resource,
                                          namespace=namespace,
                                          _iterations=1):
            events.append(event)

    assert len(events) == 0


async def test_infinite_watch_never_exits_normally(
        settings, resource, stream, namespace, aresponses):
    error = aresponses.Response(status=555)
    stream.feed(
        STREAM_WITH_ERROR_410GONE,  # watching restarted
        STREAM_WITH_UNKNOWN_EVENT,  # event ignored
        error,  # to finally exit it somehow
        namespace=namespace,
    )
    stream.close(namespace=namespace)

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
