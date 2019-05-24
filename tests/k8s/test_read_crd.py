import kubernetes
import pytest
from asynctest import call

from kopf.k8s.fetching import read_crd


def test_when_present(client_mock, resource):
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    readfn_mock = apicls_mock.return_value.read_custom_resource_definition
    result_mock = readfn_mock.return_value

    crd = read_crd(resource=resource)
    assert crd is result_mock

    assert apicls_mock.called
    assert apicls_mock.call_count == 1
    assert readfn_mock.called
    assert readfn_mock.call_count == 1
    assert readfn_mock.call_args_list == [
        call(name=f'{resource.plural}.{resource.group}')
    ]


def test_when_absent_with_no_default(client_mock, resource):
    error = kubernetes.client.rest.ApiException(status=404)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_crd(resource=resource)
    assert e.value.status == 404


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
def test_when_absent_with_default(client_mock, resource, default):
    error = kubernetes.client.rest.ApiException(status=404)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    crd = read_crd(resource=resource, default=default)
    assert crd is default


def test_raises_api_error_despite_default(client_mock, resource):
    error = kubernetes.client.rest.ApiException(status=666)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_crd(resource=resource, default=object())
    assert e.value.status == 666
