import dataclasses

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


async def test_without_subresources(kmock, settings, resource, namespace, logger):
    kmock.objects[resource, namespace, 'name1'] = {}
    patch = Patch({'x': 'y'})
    await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert len(kmock) == 1
    assert kmock[0].data == {'x': 'y'}
    assert kmock.objects[resource, namespace, 'name1'] == {'x': 'y'}


async def test_status_as_subresource_with_combined_payload(
        kmock, settings, resource, namespace, logger):
    kmock.objects[resource, namespace, 'name1'] = {
        'metadata': {'extra': '123'},
        'spec': {'extra': '456'},
        'status': {'extra': '789'},
    }

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

    assert len(kmock) == 2
    assert kmock[0].subresource is None
    assert kmock[1].subresource == 'status'
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert kmock[1].data == {'status': {'s': 't'}}
    assert reconstructed == {'metadata': {'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': {'s': 't', 'extra': '789'}}
    assert kmock.objects[resource, namespace, 'name1'] == reconstructed


async def test_status_as_subresource_with_object_fields_only(
        kmock, settings, resource, namespace, logger):
    kmock.objects[resource, namespace, 'name1'] = {
        'metadata': {'extra': '123'},
        'spec': {'extra': '456'},
        'status': {'extra': '789'}
    }

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

    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data == {'spec': {'x': 'y'}}
    assert reconstructed == {'metadata': {'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': {'extra': '789'}}
    assert kmock.objects[resource, namespace, 'name1'] == reconstructed


async def test_status_as_subresource_with_status_fields_only(
        kmock, settings, resource, namespace, logger):
    kmock.objects[resource, namespace, 'name1'] = {
        'metadata': {'extra': '123'},
        'spec': {'extra': '456'},
        'status': {'extra': '789'},
    }

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

    assert len(kmock) == 1
    assert kmock[0].subresource == 'status'
    assert kmock[0].data == {'status': {'s': 't'}}
    assert reconstructed == {'metadata': {'extra': '123'},
                             'spec': {'extra': '456'},
                             'status': {'s': 't', 'extra': '789'}}
    assert kmock.objects[resource, namespace, 'name1'] == reconstructed


async def test_status_as_body_field_with_combined_payload(
        kmock, settings, resource, namespace, logger):
    kmock.objects[resource, namespace, 'name1'] = {
        'metadata': {'extra': '123'},
        'spec': {'extra': '456'},
        'status': {'extra': '789'},
    }

    patch = Patch({'spec': {'x': 'y'}, 'status': {'s': 't'}})
    reconstructed = await patch_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        patch=patch,
    )

    assert len(kmock) == 1
    assert kmock[0].subresource is None
    assert kmock[0].data == {'spec': {'x': 'y'}, 'status': {'s': 't'}}
    assert reconstructed == {'metadata': {'extra': '123'},
                             'spec': {'x': 'y', 'extra': '456'},
                             'status': {'s': 't', 'extra': '789'}}
    assert kmock.objects[resource, namespace, 'name1'] == reconstructed


@pytest.mark.parametrize('status', [404])
async def test_ignores_absent_objects(
        kmock, settings, status, resource, namespace, logger,
        cluster_resource, namespaced_resource):
    kmock['patch', resource, kmock.name('name1')] << status

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
        kmock, settings, status, resource, namespace, logger,
        cluster_resource, namespaced_resource):
    kmock.objects[resource, namespace, 'name1'] = {}  # suppress 404s
    kmock['patch', resource, kmock.name('name1')] << status

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
