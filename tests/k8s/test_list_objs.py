import kubernetes.client.rest
import pytest
from asynctest import call

from kopf.clients.fetching import list_objs


def test_when_successful_clustered(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.return_value = result
    apicls_mock.return_value.list_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.list_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.list_cluster_custom_object

    lst = list_objs(resource=resource, namespace=None)
    assert lst is result

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
    )]


def test_when_successful_namespaced(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.return_value = result
    apicls_mock.return_value.list_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.list_cluster_custom_object
    mainfn_mock = apicls_mock.return_value.list_namespaced_custom_object

    lst = list_objs(resource=resource, namespace='ns1')
    assert lst is result

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        namespace='ns1',
    )]


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 403, 500, 666])
def test_raises_api_error(client_mock, resource, namespace, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.side_effect = error
    apicls_mock.return_value.list_namespaced_custom_object.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        list_objs(resource=resource, namespace=namespace)
    assert e.value.status == status
