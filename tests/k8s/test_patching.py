import dataclasses

import aiohttp.web
import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.patching import patch_obj
from kopf._cogs.structs.patches import Patch

OBJECT_RESPONSE = {'metadata': {'resourceVersion': 'xyz123', 'extra': '123'},
                   'spec': {'x': 'y', 'extra': '123'},
                   'status': {'extra': '123'}}
STATUS_RESPONSE = {'metadata': {'resourceVersion': 'abc456', 'extra': '456'},
                   'spec': {'x': 'y', 'extra': '456'},
                   'status': {'extra': '456', 's': 't'}}


@pytest.fixture()
def object_patch_mock(hostname, resource, namespace, resp_mocker, aresponses):
    url = resource.get_url(namespace=namespace, name='name1')
    mock = resp_mocker(return_value=aiohttp.web.json_response(OBJECT_RESPONSE))
    aresponses.add(hostname, url, 'patch', mock)
    return mock


@pytest.fixture()
def status_patch_mock(hostname, resource, namespace, resp_mocker, aresponses):
    url = resource.get_url(namespace=namespace, name='name1', subresource='status')
    mock = resp_mocker(return_value=aiohttp.web.json_response(STATUS_RESPONSE))
    aresponses.add(hostname, url, 'patch', mock)
    return mock


async def test_without_subresources(
        resp_mocker, aresponses, hostname, settings, resource, namespace, logger):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='name1'), 'patch', patch_mock)

    patch = Patch({'x': 'y'})
    await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert patch_mock.called
    assert patch_mock.call_count == 1

    data = patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'x': 'y'}


async def test_status_as_subresource_with_combined_payload(
        settings, resource, namespace, logger, object_patch_mock, status_patch_mock):
    resource = dataclasses.replace(resource, subresources=['status'])
    patch = Patch({'spec': {'x': 'y'}, 'status': {'s': 't'}})
    reconstructed = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert status_patch_mock.called
    assert status_patch_mock.call_count == 1

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}}
    data = status_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'status': {'s': 't'}}

    assert reconstructed == STATUS_RESPONSE  # ignore the body response if status was patched


async def test_status_as_subresource_with_object_fields_only(
        settings, resource, namespace, logger, object_patch_mock, status_patch_mock):
    resource = dataclasses.replace(resource, subresources=['status'])
    patch = Patch({'spec': {'x': 'y'}})
    reconstructed = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert not status_patch_mock.called

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}}

    assert reconstructed == OBJECT_RESPONSE  # ignore the status response if status was not patched


async def test_status_as_subresource_with_status_fields_only(
        settings, resource, namespace, logger, object_patch_mock, status_patch_mock):
    resource = dataclasses.replace(resource, subresources=['status'])
    patch = Patch({'status': {'s': 't'}})
    reconstructed = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert not object_patch_mock.called
    assert status_patch_mock.called
    assert status_patch_mock.call_count == 1

    data = status_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'status': {'s': 't'}}

    assert reconstructed == STATUS_RESPONSE  # ignore the body response if status was patched


async def test_status_as_body_field_with_combined_payload(
        settings, resource, namespace, logger, object_patch_mock, status_patch_mock):
    patch = Patch({'spec': {'x': 'y'}, 'status': {'s': 't'}})
    reconstructed = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert object_patch_mock.called
    assert object_patch_mock.call_count == 1
    assert not status_patch_mock.called

    data = object_patch_mock.call_args_list[0][0][0].data  # [callidx][args/kwargs][argidx]
    assert data == {'spec': {'x': 'y'}, 'status': {'s': 't'}}

    assert reconstructed == OBJECT_RESPONSE  # ignore the status response if status was not patched


@pytest.mark.parametrize('status', [404])
async def test_ignores_absent_objects(
        resp_mocker, aresponses, hostname, settings, status, resource, namespace, logger,
        cluster_resource, namespaced_resource):

    patch_mock = resp_mocker(return_value=aresponses.Response(status=status, reason='oops'))
    cluster_url = cluster_resource.get_url(namespace=None, name='name1')
    namespaced_url = namespaced_resource.get_url(namespace='ns', name='name1')
    aresponses.add(hostname, cluster_url, 'patch', patch_mock)
    aresponses.add(hostname, namespaced_url, 'patch', patch_mock)

    patch = {'x': 'y'}
    result = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert result is None


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status', [400, 403, 500, 666])
async def test_raises_api_errors(
        resp_mocker, aresponses, hostname, settings, status, resource, namespace, logger,
        cluster_resource, namespaced_resource):

    patch_mock = resp_mocker(return_value=aresponses.Response(status=status, reason='oops'))
    cluster_url = cluster_resource.get_url(namespace=None, name='name1')
    namespaced_url = namespaced_resource.get_url(namespace='ns', name='name1')
    aresponses.add(hostname, cluster_url, 'patch', patch_mock)
    aresponses.add(hostname, namespaced_url, 'patch', patch_mock)

    patch = {'x': 'y'}
    with pytest.raises(APIError) as e:
        await patch_obj(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
            name='name1',
            patch=patch,
        )
    assert e.value.status == status
