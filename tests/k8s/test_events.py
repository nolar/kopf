import aiohttp.web
import pytest

from kopf.clients.events import post_event
from kopf.structs.bodies import build_object_reference
from kopf.structs.references import Resource

EVENTS = Resource('', 'v1', 'events', namespaced=True)


async def test_posting(
        resp_mocker, aresponses, hostname):

    post_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, '/api/v1/namespaces/ns/events', 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message', resource=EVENTS)

    assert post_mock.called
    assert post_mock.call_count == 1

    req = post_mock.call_args_list[0][0][0]  # [callidx][args/kwargs][argidx]
    assert req.method == 'POST'

    data = req.data
    assert data['type'] == 'type'
    assert data['reason'] == 'reason'
    assert data['message'] == 'message'
    assert data['source']['component'] == 'kopf'
    assert data['involvedObject']['apiVersion'] == 'group/version'
    assert data['involvedObject']['kind'] == 'kind'
    assert data['involvedObject']['namespace'] == 'ns'
    assert data['involvedObject']['name'] == 'name'
    assert data['involvedObject']['uid'] == 'uid'


async def test_no_events_for_events(
        resp_mocker, aresponses, hostname):

    post_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, '/api/v1/namespaces/ns/events', 'post', post_mock)

    obj = {'apiVersion': 'v1',
           'kind': 'Event',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message', resource=EVENTS)

    assert not post_mock.called


async def test_api_errors_logged_but_suppressed(
        resp_mocker, aresponses, hostname, assert_logs):

    post_mock = resp_mocker(return_value=aresponses.Response(status=555))
    aresponses.add(hostname, '/api/v1/namespaces/ns/events', 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message', resource=EVENTS)

    assert post_mock.called
    assert_logs(["Failed to post an event."])


async def test_regular_errors_escalate(
        resp_mocker, enforced_session, mocker):

    error = Exception('boo!')
    enforced_session.post = mocker.Mock(side_effect=error)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)

    with pytest.raises(Exception) as excinfo:
        await post_event(ref=ref, type='type', reason='reason', message='message', resource=EVENTS)

    assert excinfo.value is error


async def test_message_is_cut_to_max_length(
        resp_mocker, aresponses, hostname):

    post_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, '/api/v1/namespaces/ns/events', 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    message = 'start' + ('x' * 2048) + 'end'
    await post_event(ref=ref, type='type', reason='reason', message=message, resource=EVENTS)

    data = post_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert len(data['message']) <= 1024  # max supported API message length
    assert '...' in data['message']
    assert data['message'].startswith('start')
    assert data['message'].endswith('end')


@pytest.mark.parametrize('status', [555, 500, 404, 403, 401])
async def test_headers_are_not_leaked(
        resp_mocker, aresponses, hostname, assert_logs, status):

    post_mock = resp_mocker(return_value=aresponses.Response(status=status))
    aresponses.add(hostname, '/api/v1/namespaces/ns/events', 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message', resource=EVENTS)

    assert_logs([
        "Failed to post an event.",
    ], prohibited=[
        "ClientResponseError",
        "RequestInfo",
        "headers=",
    ])
