import asyncio

import pytest
from asynctest import call

from kopf.clients.patching import patch_obj


async def test_by_name_clustered(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.patch_cluster_custom_object

    res = await patch_obj(resource=resource, namespace=None, name='name1', patch=patch)
    assert res is None  # never return any k8s-client specific things

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        name='name1',
        body=patch,
    )]


async def test_by_name_namespaced(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_cluster_custom_object
    mainfn_mock = apicls_mock.return_value.patch_namespaced_custom_object

    res = await patch_obj(resource=resource, namespace='ns1', name='name1', patch=patch)
    assert res is None  # never return any k8s-client specific things

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        namespace='ns1',
        name='name1',
        body=patch,
    )]


async def test_by_body_clustered(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.patch_cluster_custom_object

    body = {'metadata': {'name': 'name1'}}
    res = await patch_obj(resource=resource, body=body, patch=patch)
    assert res is None  # never return any k8s-client specific things

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        name='name1',
        body=patch,
    )]


async def test_by_body_namespaced(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_cluster_custom_object
    mainfn_mock = apicls_mock.return_value.patch_namespaced_custom_object

    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    res = await patch_obj(resource=resource, body=body, patch=patch)
    assert res is None  # never return any k8s-client specific things

    assert not sidefn_mock.called
    assert mainfn_mock.call_count == 1
    assert mainfn_mock.call_args_list == [call(
        group=resource.group,
        version=resource.version,
        plural=resource.plural,
        namespace='ns1',
        name='name1',
        body=patch,
    )]


async def test_raises_when_body_conflicts_with_namespace(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.patch_cluster_custom_object

    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    with pytest.raises(TypeError):
        await patch_obj(resource=resource, body=body, namespace='ns1', patch=patch)

    assert not sidefn_mock.called
    assert not mainfn_mock.called


async def test_raises_when_body_conflicts_with_name(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.patch_cluster_custom_object

    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    with pytest.raises(TypeError):
        await patch_obj(resource=resource, body=body, name='name1', patch=patch)

    assert not sidefn_mock.called
    assert not mainfn_mock.called


async def test_raises_when_body_conflicts_with_ids(client_mock, resource):
    patch = object()
    apicls_mock = client_mock.CustomObjectsApi
    sidefn_mock = apicls_mock.return_value.patch_namespaced_custom_object
    mainfn_mock = apicls_mock.return_value.patch_cluster_custom_object

    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    with pytest.raises(TypeError):
        await patch_obj(resource=resource, body=body, namespace='ns1', name='name1', patch=patch)

    assert not sidefn_mock.called
    assert not mainfn_mock.called
