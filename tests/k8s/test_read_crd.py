import kubernetes
import pytest
from asynctest import call

from kopf.clients.fetching import read_crd


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


@pytest.mark.parametrize('status', [403, 404])
def test_when_absent_with_no_default(client_mock, resource, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_crd(resource=resource)
    assert e.value.status == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('status', [403, 404])
def test_when_absent_with_default(client_mock, resource, default, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    crd = read_crd(resource=resource, default=default)
    assert crd is default


@pytest.mark.parametrize('status', [400, 401, 500, 666])
def test_raises_api_error_despite_default(client_mock, resource, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.ApiextensionsV1beta1Api
    apicls_mock.return_value.read_custom_resource_definition.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_crd(resource=resource, default=object())
    assert e.value.status == status
