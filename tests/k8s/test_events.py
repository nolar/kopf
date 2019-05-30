from asynctest import call, ANY

from kopf.k8s.events import post_event


def test_posting(client_mock):
    result = object()
    apicls_mock = client_mock.CoreV1Api
    apicls_mock.return_value.create_namespaced_event.return_value = result
    postfn_mock = apicls_mock.return_value.create_namespaced_event

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    post_event(obj=obj, type='type', reason='reason', message='message')

    assert postfn_mock.called
    assert postfn_mock.call_count == 1
    assert postfn_mock.call_args_list == [call(
        namespace='ns',  # same as the object's namespace
        body=ANY,
    )]

    event = postfn_mock.call_args_list[0][1]['body']
    assert event.type == 'type'
    assert event.reason == 'reason'
    assert event.message == 'message'
    assert event.source.component == 'kopf'
    assert event.involved_object['apiVersion'] == 'group/version'
    assert event.involved_object['kind'] == 'kind'
    assert event.involved_object['namespace'] == 'ns'
    assert event.involved_object['name'] == 'name'
    assert event.involved_object['uid'] == 'uid'


def test_type_is_v1_not_v1beta1(client_mock):
    apicls_mock = client_mock.CoreV1Api
    postfn_mock = apicls_mock.return_value.create_namespaced_event

    obj = {'apiVersion': 'group/version',
           'kind': 'kind',
           'metadata': {'namespace': 'ns',
                        'name': 'name',
                        'uid': 'uid'}}
    post_event(obj=obj, type='type', reason='reason', message='message')

    event = postfn_mock.call_args_list[0][1]['body']
    assert isinstance(event, client_mock.V1Event)
    assert not isinstance(event, client_mock.V1beta1Event)
