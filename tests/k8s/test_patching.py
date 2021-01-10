import dataclasses

import aiohttp.web
import pytest

from kopf.clients.errors import APIError
from kopf.clients.patching import patch_obj
from kopf.structs.patches import Patch


async def test_without_subresources(
        resp_mocker, aresponses, hostname, resource, namespace):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='name1'), 'patch', patch_mock)

    patch = Patch({'x': 'y'})
    await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert patch_mock.called
    assert patch_mock.call_count == 1

    data = patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'x': 'y'}


async def test_status_as_subresource_with_combined_payload(
        resp_mocker, aresponses, hostname, resource, namespace):
    resource = dataclasses.replace(resource, subresources=['status'])

    # Simulate Kopf's initial state and intention.
    patch = Patch({'spec': {'x': 'y'}, 'status': {'s': 't'}})

    # Simulate K8s API's behaviour. Assume something extra is added remotely.
    object_response = {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                       'spec': {'x': 'y', 'extra': '456'},
                       'status': '...'}
    status_response = {'status': {'s': 't', 'extra': '789'}}

    object_url = resource.get_url(namespace=namespace, name='name1')
    status_url = resource.get_url(namespace=namespace, name='name1', subresource='status')
    object_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(object_response))
    status_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(status_response))
    aresponses.add(hostname, object_url, 'patch', object_patch_mock)
    aresponses.add(hostname, status_url, 'patch', status_patch_mock)

    reconstructed = await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert status_patch_mock.called
    assert status_patch_mock.call_count == 1

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}}
    data = status_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'status': {'s': 't'}}

    assert reconstructed == {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': {'s': 't', 'extra': '789'}}


async def test_status_as_subresource_with_object_fields_only(
        resp_mocker, aresponses, hostname, resource, namespace):
    resource = dataclasses.replace(resource, subresources=['status'])

    # Simulate Kopf's initial state and intention.
    patch = Patch({'spec': {'x': 'y'}})

    # Simulate K8s API's behaviour. Assume something extra is added remotely.
    object_response = {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                       'spec': {'x': 'y', 'extra': '456'},
                       'status': '...'}
    status_response = {'status': {'s': 't', 'extra': '789'}}

    object_url = resource.get_url(namespace=namespace, name='name1')
    status_url = resource.get_url(namespace=namespace, name='name1', subresource='status')
    object_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(object_response))
    status_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(status_response))
    aresponses.add(hostname, object_url, 'patch', object_patch_mock)
    aresponses.add(hostname, status_url, 'patch', status_patch_mock)

    reconstructed = await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert not status_patch_mock.called

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}}

    assert reconstructed == {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': '...'}


async def test_status_as_subresource_with_status_fields_only(
        resp_mocker, aresponses, hostname, resource, namespace):
    resource = dataclasses.replace(resource, subresources=['status'])

    # Simulate Kopf's initial state and intention.
    patch = Patch({'status': {'s': 't'}})

    # Simulate K8s API's behaviour. Assume something extra is added remotely.
    object_response = {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                       'spec': {'x': 'y', 'extra': '456'},
                       'status': '...'}
    status_response = {'status': {'s': 't', 'extra': '789'}}

    object_url = resource.get_url(namespace=namespace, name='name1')
    status_url = resource.get_url(namespace=namespace, name='name1', subresource='status')
    object_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(object_response))
    status_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(status_response))
    aresponses.add(hostname, object_url, 'patch', object_patch_mock)
    aresponses.add(hostname, status_url, 'patch', status_patch_mock)

    reconstructed = await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert not object_patch_mock.called
    assert status_patch_mock.called
    assert status_patch_mock.call_count == 1

    data = status_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'status': {'s': 't'}}

    assert reconstructed == {'status': {'s': 't', 'extra': '789'}}


async def test_status_as_body_field_with_combined_payload(
        resp_mocker, aresponses, hostname, resource, namespace):

    # Simulate Kopf's initial state and intention.
    patch = Patch({'spec': {'x': 'y'}, 'status': {'s': 't'}})

    # Simulate K8s API's behaviour. Assume something extra is added remotely.
    object_response = {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                       'spec': {'x': 'y', 'extra': '456'},
                       'status': '...'}
    status_response = {'s': 't', 'extra': '789'}

    object_url = resource.get_url(namespace=namespace, name='name1')
    status_url = resource.get_url(namespace=namespace, name='name1', subresource='status')
    object_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(object_response))
    status_patch_mock = resp_mocker(return_value=aiohttp.web.json_response(status_response))
    aresponses.add(hostname, object_url, 'patch', object_patch_mock)
    aresponses.add(hostname, status_url, 'patch', status_patch_mock)

    reconstructed = await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert not status_patch_mock.called

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}, 'status': {'s': 't'}}

    assert reconstructed == {'metadata': {'namespace': 'ns1', 'name': 'name1', 'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': '...'}


@pytest.mark.parametrize('status', [404])
async def test_ignores_absent_objects(
        resp_mocker, aresponses, hostname, status, resource, namespace,
        cluster_resource, namespaced_resource):

    patch_mock = resp_mocker(return_value=aresponses.Response(status=status))
    cluster_url = cluster_resource.get_url(namespace=None, name='name1')
    namespaced_url = namespaced_resource.get_url(namespace='ns', name='name1')
    aresponses.add(hostname, cluster_url, 'patch', patch_mock)
    aresponses.add(hostname, namespaced_url, 'patch', patch_mock)

    patch = {'x': 'y'}
    result = await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)

    assert result is None


@pytest.mark.parametrize('status', [400, 401, 403, 500, 666])
async def test_raises_api_errors(
        resp_mocker, aresponses, hostname, status, resource, namespace,
        cluster_resource, namespaced_resource):

    patch_mock = resp_mocker(return_value=aresponses.Response(status=status))
    cluster_url = cluster_resource.get_url(namespace=None, name='name1')
    namespaced_url = namespaced_resource.get_url(namespace='ns', name='name1')
    aresponses.add(hostname, cluster_url, 'patch', patch_mock)
    aresponses.add(hostname, namespaced_url, 'patch', patch_mock)

    patch = {'x': 'y'}
    with pytest.raises(APIError) as e:
        await patch_obj(resource=resource, namespace=namespace, name='name1', patch=patch)
    assert e.value.status == status
