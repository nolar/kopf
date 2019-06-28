import collections.abc

import pytest

from kopf.clients.watching import streaming_watch


class PreventedActualCallError(Exception):
    pass


async def test_watching_over_cluster(resource, client_mock):
    apicls_mock = client_mock.CustomObjectsApi
    cl_list_fn = apicls_mock.return_value.list_cluster_custom_object
    ns_list_fn = apicls_mock.return_value.list_namespaced_custom_object
    cl_list_fn.side_effect = PreventedActualCallError()
    ns_list_fn.side_effect = PreventedActualCallError()

    itr = streaming_watch(
        resource=resource,
        namespace=None,
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    with pytest.raises(PreventedActualCallError):
        async for _ in itr: pass  # fully deplete it

    assert apicls_mock.called
    assert apicls_mock.call_count == 1

    # Cluster-scoped listing function is used irrelevant of the resource.
    assert not ns_list_fn.called
    assert cl_list_fn.called
    assert cl_list_fn.call_count == 1
    assert cl_list_fn.call_args[1]['group'] == resource.group
    assert cl_list_fn.call_args[1]['version'] == resource.version
    assert cl_list_fn.call_args[1]['plural'] == resource.plural
    assert 'namespace' not in cl_list_fn.call_args[1]


async def test_watching_over_namespace(resource, client_mock):
    apicls_mock = client_mock.CustomObjectsApi
    cl_list_fn = apicls_mock.return_value.list_cluster_custom_object
    ns_list_fn = apicls_mock.return_value.list_namespaced_custom_object
    cl_list_fn.side_effect = PreventedActualCallError()
    ns_list_fn.side_effect = PreventedActualCallError()

    itr = streaming_watch(
        resource=resource,
        namespace='something',
    )

    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    with pytest.raises(PreventedActualCallError):
        async for _ in itr: pass  # fully deplete it

    assert apicls_mock.called
    assert apicls_mock.call_count == 1

    # The scope-relevant listing function is used, depending on the resource.
    assert not cl_list_fn.called
    assert ns_list_fn.called
    assert ns_list_fn.call_count == 1
    assert ns_list_fn.call_args[1]['group'] == resource.group
    assert ns_list_fn.call_args[1]['version'] == resource.version
    assert ns_list_fn.call_args[1]['plural'] == resource.plural
    assert ns_list_fn.call_args[1]['namespace'] == 'something'
