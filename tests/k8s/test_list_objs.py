import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import list_objs


async def test_listing_works(kmock, settings, logger, resource, namespace):
    kmock[resource, kmock.namespace(namespace)] << {'items': [{}, {}]}
    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert items == [{}, {}]
    assert len(kmock['list']) == 1


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status', [400, 403, 500, 666])
async def test_raises_direct_api_errors(
        kmock, settings, logger, status, resource, namespace,
        cluster_resource, namespaced_resource):
    kmock[cluster_resource, kmock.namespace(None)] << status
    kmock[namespaced_resource, kmock.namespace('ns')] << status

    with pytest.raises(APIError) as e:
        await list_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        )
    assert e.value.status == status
