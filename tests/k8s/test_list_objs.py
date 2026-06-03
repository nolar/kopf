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
    assert kmock['list'][0].params == {}


async def test_listing_is_paginated_when_batch_size_is_set(
        kmock, settings, logger, resource, namespace):
    settings.watching.batch_size = 2
    ns = kmock.namespace(namespace)
    kmock[resource, ns] << {
        'metadata': {'resourceVersion': '100', 'continue': 'TOKEN1'},
        'items': [{'spec': 1}, {'spec': 2}]}
    kmock[resource, ns, kmock.params({'continue': 'TOKEN1'})].override << {
        'metadata': {'resourceVersion': '100', 'continue': 'TOKEN2'},
        'items': [{'spec': 3}, {'spec': 4}]}
    kmock[resource, ns, kmock.params({'continue': 'TOKEN2'})].override << {
        'metadata': {'resourceVersion': '100', 'continue': ''},
        'items': [{'spec': 5}]}

    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )

    assert [item['spec'] for item in items] == [1, 2, 3, 4, 5]
    assert resource_version == '100'
    assert len(kmock['list']) == 3
    assert kmock['list'][0].params == {'limit': '2'}
    assert kmock['list'][1].params == {'limit': '2', 'continue': 'TOKEN1'}
    assert kmock['list'][2].params == {'limit': '2', 'continue': 'TOKEN2'}


async def test_listing_defaults_kind_and_version_across_pages(
        kmock, settings, logger, resource, namespace):
    settings.watching.batch_size = 1
    ns = kmock.namespace(namespace)
    kmock[resource, ns] << {
        'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'continue': 'NEXT'},
        'items': [{}]}
    kmock[resource, ns, kmock.params({'continue': 'NEXT'})].override << {
        'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {},
        'items': [{}]}
    items, _ = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert len(items) == 2
    assert all(item['kind'] == 'Pod' and item['apiVersion'] == 'v1' for item in items)


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
