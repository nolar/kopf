import urllib.parse

import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import list_objs


def _query(url):
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))


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


async def test_listing_is_a_single_unpaginated_request_by_default(
        mocker, settings, logger, resource, namespace):
    get = mocker.patch('kopf._cogs.clients.api.get', return_value={
        'metadata': {'resourceVersion': '123'},
        'items': [{'spec': 1}, {'spec': 2}],
    })
    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert [item['spec'] for item in items] == [1, 2]
    assert resource_version == '123'
    assert get.call_count == 1
    query = _query(get.call_args.kwargs['url'])
    assert 'limit' not in query
    assert 'continue' not in query


async def test_listing_is_paginated_when_batch_size_is_set(
        mocker, settings, logger, resource, namespace):
    settings.watching.batch_size = 2
    pages = [
        {'metadata': {'resourceVersion': '100', 'continue': 'TOKEN1'},
         'items': [{'spec': 1}, {'spec': 2}]},
        {'metadata': {'resourceVersion': '100', 'continue': 'TOKEN2'},
         'items': [{'spec': 3}, {'spec': 4}]},
        {'metadata': {'resourceVersion': '100'},
         'items': [{'spec': 5}]},
    ]
    get = mocker.patch('kopf._cogs.clients.api.get', side_effect=pages)
    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )

    # All chunks are aggregated into a single collection.
    assert [item['spec'] for item in items] == [1, 2, 3, 4, 5]

    # The snapshot resource version (the same across all chunks) is returned.
    assert resource_version == '100'

    # Each chunk carries the page size; the follow-ups carry the continue token.
    assert get.call_count == 3
    assert _query(get.call_args_list[0].kwargs['url']) == {'limit': '2'}
    assert _query(get.call_args_list[1].kwargs['url']) == {'limit': '2', 'continue': 'TOKEN1'}
    assert _query(get.call_args_list[2].kwargs['url']) == {'limit': '2', 'continue': 'TOKEN2'}


async def test_listing_stops_at_an_empty_continue_token(
        mocker, settings, logger, resource, namespace):
    settings.watching.batch_size = 100
    get = mocker.patch('kopf._cogs.clients.api.get', side_effect=[
        {'metadata': {'resourceVersion': '7', 'continue': ''},
         'items': [{'spec': 1}]},
    ])
    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert [item['spec'] for item in items] == [1]
    assert resource_version == '7'
    assert get.call_count == 1


async def test_listing_defaults_kind_and_version_across_pages(
        mocker, settings, logger, resource, namespace):
    settings.watching.batch_size = 1
    pages = [
        {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'continue': 'NEXT'},
         'items': [{'metadata': {'name': 'a'}}]},
        {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {},
         'items': [{'metadata': {'name': 'b'}}]},
    ]
    mocker.patch('kopf._cogs.clients.api.get', side_effect=pages)
    items, _ = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert all(item['kind'] == 'Pod' and item['apiVersion'] == 'v1' for item in items)
    assert [item['metadata']['name'] for item in items] == ['a', 'b']


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
