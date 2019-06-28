import pydoc

import kubernetes.client.rest
import pytest
from asynctest import call

from kopf.clients.fetching import make_list_fn


def test_when_present_clustered(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.return_value = result
    apicls_mock.return_value.list_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.list_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.list_cluster_custom_object

    fn = make_list_fn(resource=resource, namespace=None)
    assert callable(fn)

    assert not sidefn_mock.called
    assert not mainfn_mock.called

    res = fn(opt1='val1', opt2=123)
    assert res is result

    assert sidefn_mock.call_count == 0
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        opt1='val1', opt2=123,
    )]


def test_when_present_namespaced(client_mock, resource):
    result = object()
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.return_value = result
    apicls_mock.return_value.list_namespaced_custom_object.return_value = result
    sidefn_mock = apicls_mock.return_value.list_cluster_custom_object
    mainfn_mock = apicls_mock.return_value.list_namespaced_custom_object

    fn = make_list_fn(resource=resource, namespace='ns1')
    assert callable(fn)

    assert not sidefn_mock.called
    assert not mainfn_mock.called

    res = fn(opt1='val1', opt2=123)
    assert res is result

    assert sidefn_mock.call_count == 0
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        namespace='ns1',
        opt1='val1', opt2=123,
    )]


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 403, 404, 500, 666])
def test_raises_api_error(client_mock, resource, namespace, status):
    error = kubernetes.client.rest.ApiException(status=status)
    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.side_effect = error
    apicls_mock.return_value.list_namespaced_custom_object.side_effect = error

    fn = make_list_fn(resource=resource, namespace=namespace)
    with pytest.raises(kubernetes.client.rest.ApiException) as e:
        fn(opt1='val1', opt2=123)
    assert e.value.status == status


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
def test_docstrings_are_preserved(client_mock, resource, namespace):
    # Docstrings are important! Kubernetes client uses them to guess
    # the returned object types and the parameters type.
    docstring = """some doc \n :return: sometype"""

    apicls_mock = client_mock.CustomObjectsApi
    apicls_mock.return_value.list_cluster_custom_object.__doc__ = docstring
    apicls_mock.return_value.list_namespaced_custom_object.__doc__ = docstring

    fn = make_list_fn(resource=resource, namespace=namespace)
    fn_docstring = pydoc.getdoc(fn)  # same as k8s client does this
    assert isinstance(fn_docstring, str)
    assert ':return: sometype' in docstring  # it will be reformatted
