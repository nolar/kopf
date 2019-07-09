import pytest
import requests

from kopf.clients.fetching import list_objs


def test_when_successful_clustered(req_mock, resource):
    result = {'items': []}
    req_mock.get.return_value.json.return_value = result

    lst = list_objs(resource=resource, namespace=None)
    assert lst is result

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/kopfexamples' in url
    assert 'namespaces/' not in url


def test_when_successful_namespaced(req_mock, resource):
    result = {'items': []}
    req_mock.get.return_value.json.return_value = result

    lst = list_objs(resource=resource, namespace='ns1')
    assert lst is result

    assert req_mock.get.called
    assert req_mock.get.call_count == 1

    url = req_mock.get.call_args_list[0][1]['url']
    assert 'apis/zalando.org/v1/namespaces/ns1/kopfexamples' in url


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 403, 500, 666])
def test_raises_api_error(req_mock, resource, namespace, status):
    response = requests.Response()
    response.status_code = status
    error = requests.exceptions.HTTPError("boo!", response=response)
    req_mock.get.side_effect = error

    with pytest.raises(requests.exceptions.HTTPError) as e:
        list_objs(resource=resource, namespace=namespace)
    assert e.value.response.status_code == status
