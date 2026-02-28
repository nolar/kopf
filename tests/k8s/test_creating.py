import pytest

from kopf._cogs.clients.creating import create_obj
from kopf._cogs.clients.errors import APIError


async def test_simple_body_with_arguments(
        kmock, settings, resource, namespace, logger):
    kmock['post', resource] << {}

    body = {'x': 'y'}
    await create_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        name='name1',
        body=body,
    )

    assert len(kmock['post']) == 1
    assert kmock['post'][0].resource == resource
    assert kmock['post'][0].namespace == namespace
    if resource.namespaced:
        assert kmock['post'][0].data == {'x': 'y', 'metadata': {'name': 'name1', 'namespace': 'ns'}}
    else:
        assert kmock['post'][0].data == {'x': 'y', 'metadata': {'name': 'name1'}}


async def test_full_body_with_identifiers(
        kmock, settings, resource, namespace, logger):
    kmock['post', resource] << {}

    body = {'x': 'y', 'metadata': {'name': 'name1', 'namespace': namespace}}
    await create_obj(
        logger=logger,
        settings=settings,
        resource=resource,
        body=body,
    )

    assert len(kmock['post']) == 1
    assert kmock['post'][0].resource == resource
    assert kmock['post'][0].namespace == namespace
    assert kmock['post'][0].data == {'x': 'y', 'metadata': {'name': 'name1', 'namespace': namespace}}


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status', [400, 403, 404, 409, 500, 666])
async def test_raises_api_errors(
        kmock, settings, status, resource, namespace, logger,
        cluster_resource, namespaced_resource):
    kmock['post', resource, kmock.namespace(None)] << status
    kmock['post', resource, kmock.namespace('ns')] << status

    body = {'x': 'y'}
    with pytest.raises(APIError) as e:
        await create_obj(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
            name='name1',
            body=body,
        )

    assert e.value.status == status
    assert len(kmock) == 1
    if namespace is None:
        assert kmock[0].resource == cluster_resource
    else:
        assert kmock[0].resource == namespaced_resource
