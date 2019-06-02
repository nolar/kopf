import json

import pytest
import requests

from kopf.clients.events import post_event


async def test_posting(req_mock):
    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    await post_event(obj=obj, type='type', reason='reason', message='message')

    assert req_mock.post.called
    assert req_mock.post.call_count == 1

    data = json.loads(req_mock.post.call_args_list[0][1]['data'])
    assert data['type'] == 'type'
    assert data['reason'] == 'reason'
    assert data['message'] == 'message'
    assert data['source']['component'] == 'kopf'
    assert data['involvedObject']['apiVersion'] == 'group/version'
    assert data['involvedObject']['kind'] == 'kind'
    assert data['involvedObject']['namespace'] == 'ns'
    assert data['involvedObject']['name'] == 'name'
    assert data['involvedObject']['uid'] == 'uid'


async def test_type_is_v1_not_v1beta1(req_mock):
    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    await post_event(obj=obj, type='type', reason='reason', message='message')

    assert req_mock.post.called

    url = req_mock.post.call_args_list[0][1]['url']
    assert 'v1beta1' not in url
    assert '/api/v1/namespaces/ns/events' in url


async def test_api_errors_logged_but_suppressed(req_mock, assert_logs):
    response = requests.Response()
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.post.side_effect = error

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    await post_event(obj=obj, type='type', reason='reason', message='message')

    assert req_mock.post.called
    assert_logs([
        "Failed to post an event.*boo!",
    ])


async def test_regular_errors_escalate(req_mock):
    error = Exception('boo!')
    req_mock.post.side_effect = error

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}

    with pytest.raises(Exception) as excinfo:
        await post_event(obj=obj, type='type', reason='reason', message='message')

    assert excinfo.value is error


async def test_message_is_cut_to_max_length(req_mock):
    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    message = 'start' + ('x' * 2048) + 'end'
    await post_event(obj=obj, type='type', reason='reason', message=message)

    data = json.loads(req_mock.post.call_args_list[0][1]['data'])
    assert len(data['message']) <= 1024  # max supported API message length
    assert '...' in data['message']
    assert data['message'].startswith('start')
    assert data['message'].endswith('end')
