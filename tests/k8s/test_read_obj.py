import kubernetes.client.rest
import pytest
from asynctest import call

from kopf.clients.fetching import read_obj


def test_when_present_clustered(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.get_cluster_custom_object.return_value = result
    apicls_mock.return_value.get_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.get_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.get_cluster_custom_object

    crd = read_obj(resource=resource, namespace=None, name='name1')
    assert crd is result

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        name='name1',
    )]


def test_when_present_namespaced(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.get_cluster_custom_object.return_value = result
    apicls_mock.return_value.get_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.get_cluster_custom_object
    mainfn_mock = apicls_mock.return_value.get_namespaced_custom_object

    crd = read_obj(resource=resource, namespace='ns1', name='name1')
    assert crd is result

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        namespace='ns1',
        name='name1',
    )]


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [404])
def test_when_absent_with_no_default(client_mock, resource, namespace, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.get_cluster_custom_object.side_effect = error
    apicls_mock.return_value.get_namespaced_custom_object.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_obj(resource=resource, namespace=namespace, name='name1')
    assert e.value.status == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [404])
def test_when_absent_with_default(client_mock, resource, namespace, default, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.get_cluster_custom_object.side_effect = error
    apicls_mock.return_value.get_namespaced_custom_object.side_effect = error

    crd = read_obj(resource=resource, namespace=namespace, name='name1', default=default)
    assert crd is default


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 403, 500, 666])
def test_raises_api_error_despite_default(client_mock, resource, namespace, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.get_cluster_custom_object.side_effect = error
    apicls_mock.return_value.get_namespaced_custom_object.side_effect = error

    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        read_obj(resource=resource, namespace=namespace, name='name1', default=object())
    assert e.value.status == status
