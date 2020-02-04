import aiohttp.web
import pytest

from kopf.structs.bodies import build_object_reference
from kopf.clients.events import post_event, EVENTS_V1BETA1_CRD, EVENTS_CORE_V1_CRD


async def test_posting(
        resp_mocker, aresponses, hostname):

    post_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, EVENTS_CORE_V1_CRD.get_url(namespace='ns'), 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message')

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


async def test_type_is_v1_not_v1beta1(
        resp_mocker, aresponses, hostname):

    core_v1_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    v1beta1_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, EVENTS_CORE_V1_CRD.get_url(namespace='ns'), 'post', core_v1_mock)
    aresponses.add(hostname, EVENTS_V1BETA1_CRD.get_url(namespace='ns'), 'post', v1beta1_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message')

    assert core_v1_mock.called
    assert not v1beta1_mock.called


async def test_api_errors_logged_but_suppressed(
        resp_mocker, aresponses, hostname, assert_logs):

    post_mock = resp_mocker(return_value=aresponses.Response(status=555, reason='boo!'))
    aresponses.add(hostname, EVENTS_CORE_V1_CRD.get_url(namespace='ns'), 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message')

    assert post_mock.called
    assert_logs([
        "Failed to post an event.*boo!",
    ])


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
        await post_event(ref=ref, type='type', reason='reason', message='message')

    assert excinfo.value is error


async def test_message_is_cut_to_max_length(
        resp_mocker, aresponses, hostname):

    post_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, EVENTS_CORE_V1_CRD.get_url(namespace='ns'), 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    message = 'start' + ('x' * 2048) + 'end'
    await post_event(ref=ref, type='type', reason='reason', message=message)

    data = post_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert len(data['message']) <= 1024  # max supported API message length
    assert '...' in data['message']
    assert data['message'].startswith('start')
    assert data['message'].endswith('end')


@pytest.mark.parametrize('status', [555, 500, 404, 403, 401])
async def test_headers_are_not_leaked(
        resp_mocker, aresponses, hostname, assert_logs, status):

    post_mock = resp_mocker(return_value=aresponses.Response(status=status, reason='boo!'))
    aresponses.add(hostname, EVENTS_CORE_V1_CRD.get_url(namespace='ns'), 'post', post_mock)

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    ref = build_object_reference(obj)
    await post_event(ref=ref, type='type', reason='reason', message='message')

    assert_logs([
        f"Failed to post an event. .* Status: {status}. Message: boo!",
    ], prohibited=[
        "ClientResponseError",
        "RequestInfo",
        "headers=",
    ])
