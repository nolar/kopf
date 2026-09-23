from typing import Any

import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import list_objs
from kopf._cogs.configs.configuration import OperatorSettings, WatchListSelector
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import references


async def test_listing_works(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock[resource, kmock.namespace(namespace)] << {'items': [{}, {}]}
    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert items == [{}, {}]
    assert len(kmock['list']) == 1


async def test_listing_omits_server_side_selectors_by_default(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock[resource, kmock.namespace(namespace)] << {'items': []}

    await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )

    assert 'labelSelector' not in kmock[0].url.query
    assert 'fieldSelector' not in kmock[0].url.query


async def test_listing_passes_server_side_selectors(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    label_selector = 'prefect.io/flow-run-id'
    field_selector = 'status.phase!=Succeeded,status.phase!=Failed'
    kmock[resource, kmock.namespace(namespace)] << {'items': []}

    await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
        server_side_selector=WatchListSelector(
            label_selector=label_selector,
            field_selector=field_selector,
        ),
    )

    assert kmock[0].url.query['labelSelector'] == label_selector
    assert kmock[0].url.query['fieldSelector'] == field_selector


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status', [400, 403, 500, 666])
async def test_raises_direct_api_errors(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        status: int,
        resource: references.Resource,
        namespace: references.Namespace,
        cluster_resource: references.Resource,
        namespaced_resource: references.Resource,
) -> None:
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
