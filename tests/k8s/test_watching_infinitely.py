import aiohttp
import pytest

from kopf._cogs.clients.errors import APIError, APIForbiddenError, APINotFoundError, \
                                      APIServerError, APITooManyRequestsError
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
    kmock['watch', resource, kmock.namespace(namespace)] << 666

    events = []
    with pytest.raises(APIError) as e:
        async for event in infinite_watch(settings=settings,
                                          resource=resource,
                                          namespace=namespace):
            events.append(event)

    assert e.value.status == 666

    assert len(events) == 5
    assert events[0] == Bookmark.LISTED
    assert events[1]['object']['spec'] == 'a'
    assert events[2] == Bookmark.LISTED
    assert events[3]['object']['spec'] == 'a'
    assert events[4]['object']['spec'] == 'b'


async def test_too_many_requests_exception_old_style(
        kmock, settings, resource, namespace, enforced_session, mocker):

    exc = APITooManyRequestsError({
        "apiVersion": "v1",
        "code": 666,  # must be irrelevant, purely informational
        "status": "Failure",
        "details": {
            "retryAfterSeconds": 1,
        }
    }, status=429, headers={})
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    async for event in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
        events.append(event)

    assert len(events) == 0


async def test_too_many_requests_exception_new_style(
        settings, resource, kmock, namespace, enforced_session, mocker):

    headers = {'Retry-After': '1'}
    exc = APITooManyRequestsError("Too many requests.", status=429, headers=headers)
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    async for event in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
        events.append(event)

    assert len(events) == 0


async def test_server_error_reconnects(
        settings, resource, kmock, namespace, enforced_session, mocker):
    exc = APIServerError("Service unavailable.", status=503, headers={})
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    async for event in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
        events.append(event)

    assert len(events) == 0


async def test_not_found_error_reconnects(
        settings, resource, kmock, namespace, enforced_session, mocker):
    exc = APINotFoundError("Not found.", status=404, headers={})
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    events = []
    async for event in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
        events.append(event)

    assert len(events) == 0


# Authorization errors are not restarted in place: the operator must fail and be restarted as a whole.
async def test_forbidden_error_escalates(
        settings, resource, kmock, namespace, enforced_session, mocker):
    exc = APIForbiddenError("Forbidden.", status=403, headers={})
    enforced_session.request = mocker.Mock(side_effect=exc)
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    with pytest.raises(APIForbiddenError):
        async for _ in infinite_watch(settings=settings,
                                      resource=resource,
                                      namespace=namespace,
                                      _iterations=1):
            pass


STREAM_WITH_ERROR_503 = (
    {'type': 'ERROR', 'object': {'code': 503}},
)


async def test_reconnection_delays_grow_up_to_the_limit(
        kmock, settings, resource, namespace, looptime, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_503
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=7):
        pass

    assert looptime == 1 + 2 + 4 + 8 + 10 + 10 + 10
    assert_logs(["failed; reconnecting in 8s", "failed; reconnecting in 10s"])


async def test_reconnection_delays_never_go_below_the_configured_backoff(
        kmock, settings, resource, namespace, looptime):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_503
    settings.watching.reconnect_backoff = 30

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 30 + 30 + 30


async def test_reconnection_errors_have_a_minimum_delay(
        kmock, settings, resource, namespace, looptime):
    kmock['watch', resource, kmock.namespace(namespace)] << STREAM_WITH_ERROR_503
    settings.watching.reconnect_backoff = 0

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 0.1 + 0.2 + 0.4


@pytest.mark.parametrize('event_type', ['ADDED', 'BOOKMARK'])
async def test_reconnection_delays_reset_after_a_watched_event(
        kmock, settings, resource, namespace, looptime, event_type):
    kmock['watch', resource, kmock.namespace(namespace)] << iter(STREAM_WITH_ERROR_503)
    kmock['watch', resource, kmock.namespace(namespace)] << iter(STREAM_WITH_ERROR_503)
    kmock['watch', resource, kmock.namespace(namespace)] << iter((
        {'type': event_type, 'object': {
            'metadata': {'resourceVersion': '100'} if event_type == 'BOOKMARK' else {},
            'spec': 'a',
        }},
        {'type': 'ERROR', 'object': {'code': 503}},
    ))
    kmock['watch', resource, kmock.namespace(namespace)] << iter(STREAM_WITH_ERROR_503)
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=4):
        pass

    assert looptime == 1 + 2 + 1 + 2


async def test_reconnection_delays_grow_on_server_errors(
        settings, resource, kmock, namespace, enforced_session, mocker, looptime):
    exc = APIServerError("Service unavailable.", status=503, headers={})
    enforced_session.request = mocker.Mock(side_effect=exc)
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 1 + 2 + 4


@pytest.mark.parametrize('exception', [
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    TimeoutError,
])
async def test_reconnection_delays_grow_on_transport_errors(
        settings, resource, namespace, enforced_session, mocker, looptime, exception):
    enforced_session.request = mocker.Mock(side_effect=exception())
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 1 + 2 + 4


async def test_reconnection_delays_grow_on_disconnects_before_events(
        kmock, settings, resource, namespace, mocker, looptime, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << b''

    async def broken_lines(*args, **kwargs):
        raise aiohttp.ClientPayloadError("incomplete payload")
        yield

    mocker.patch('kopf._cogs.clients.api.iter_jsonlines', side_effect=broken_lines)
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 1 + 2 + 4
    assert len(kmock['watch', resource]) == 3
    assert_logs([r"Watch-stream.*failed; reconnecting in 1s"])


@pytest.mark.parametrize('failure', [
    pytest.param(503, id='http-503'),
    pytest.param(({'type': 'ERROR', 'object': {'code': 503}},), id='in-stream-503'),
    pytest.param(b'', id='clean-eof'),
])
async def test_temporary_failures_resume_from_the_last_resource_version(
        kmock, settings, resource, namespace, failure):
    settings.networking.error_backoffs = []
    settings.watching.reconnect_backoff = 0
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }
    kmock['watch', resource, kmock.namespace(namespace), :1] << failure
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=2):
        pass

    assert len(kmock['list', resource]) == 1
    assert [call.params.get('resourceVersion')
            for call in kmock['watch', resource]] == ['100', '100']


async def test_partial_initial_streaming_restarts_from_the_beginning(
        kmock, settings, resource, namespace):
    settings.watching.initial_streaming = True
    settings.watching.reconnect_backoff = 0
    obj1 = {'metadata': {'uid': 'uid1', 'resourceVersion': '100'}}
    obj2 = {'metadata': {'uid': 'uid2', 'resourceVersion': '101'}}
    kmock['watch', resource, kmock.namespace(namespace), :1] << (
        {'type': 'ADDED', 'object': obj1},
    )
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ADDED', 'object': obj1},
        {'type': 'ADDED', 'object': obj2},
        {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '200'}}},
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    events = [event async for event in infinite_watch(
        settings=settings,
        resource=resource,
        namespace=namespace,
        _iterations=2,
    )]

    assert len(kmock['watch', resource]) == 2
    assert all(call.params.get('sendInitialEvents') == 'true'
               for call in kmock['watch', resource])
    assert [event['object']['metadata']['uid']
            for event in events
            if isinstance(event, dict) and event['type'] is None] == ['uid1', 'uid1', 'uid2']
    assert Bookmark.RESET in events
    assert Bookmark.LISTED in events


async def test_reconnection_delays_grow_on_malformed_streams(
        kmock, settings, resource, namespace, looptime, assert_logs):
    kmock['watch', resource, kmock.namespace(namespace)] << b'not json\n'
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=3):
        pass

    assert looptime == 1 + 2 + 4
    assert len(kmock['watch', resource]) == 3
    assert_logs([r"Watch-stream.*failed; reconnecting in 1s"])


@pytest.mark.parametrize('code', [429, 503])
@pytest.mark.parametrize('enforce, retry_after, expected', [
    pytest.param(False, 2, 5, id='shorter-ignored'),
    pytest.param(True, 2, 2, id='shorter-enforced'),
    pytest.param(False, 7, 7, id='longer'),
    pytest.param(True, 7, 7, id='longer-enforced'),
    pytest.param(False, 30, 30, id='above-the-limit'),
    pytest.param(True, 0, 5, id='zero-enforced'),
    pytest.param(False, 999999, 300, id='huge-capped'),
    pytest.param(True, 999999, 300, id='huge-enforced-capped'),
])
async def test_retry_after_in_the_stream_is_respected(
        kmock, settings, resource, namespace, looptime, code,
        enforce, retry_after, expected):
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ERROR', 'object': {'code': code, 'details': {
            'retryAfterSeconds': retry_after,
        }}},
    )
    settings.watching.reconnect_backoff = 5
    settings.networking.enforce_retry_after = enforce

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=1):
        pass

    assert looptime == expected


async def test_retry_after_on_connection_is_respected(
        settings, resource, kmock, namespace, enforced_session, mocker, looptime):
    exc = APITooManyRequestsError("Too many requests.", status=429, headers={'Retry-After': '7'})
    enforced_session.request = mocker.Mock(side_effect=exc)
    settings.watching.reconnect_backoff = 1

    async for _ in infinite_watch(settings=settings,
                                  resource=resource,
                                  namespace=namespace,
                                  _iterations=1):
        pass

    assert looptime == 7
