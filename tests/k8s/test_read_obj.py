import pykube
import pytest
import requests

from kopf.clients.fetching import read_obj


@pytest.mark.resource_clustered  # see `req_mock`
def test_when_present_clustered(req_mock, resource):
    result = {}
    req_mock.get.return_value.json.return_value = result

    crd = read_obj(resource=resource, namespace=None, name='name1')
    assert crd is result

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/kopfexamples/name1' in url
    assert 'namespaces/' not in url


def test_when_present_namespaced(req_mock, resource):
    result = {}
    req_mock.get.return_value.json.return_value = result

    crd = read_obj(resource=resource, namespace='ns1', name='name1')
    assert crd is result

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/namespaces/ns1/kopfexamples/name1' in url


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [403, 404])
def test_when_absent_with_no_default(req_mock, resource, namespace, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    with pytest.raises(requests.exceptions.HTTPError) as e:
        read_obj(resource=resource, namespace=namespace, name='name1')
    assert e.value.response.status_code == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [403, 404])
def test_when_absent_with_default(req_mock, resource, namespace, default, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    crd = read_obj(resource=resource, namespace=namespace, name='name1', default=default)
    assert crd is default


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 500, 666])
def test_raises_api_error_despite_default(req_mock, resource, namespace, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    with pytest.raises(requests.exceptions.HTTPError) as e:
        read_obj(resource=resource, namespace=namespace, name='name1', default=object())
    assert e.value.response.status_code == status
