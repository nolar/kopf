from typing import Any

import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import fetch_objs
from kopf._cogs.configs.configuration import OperatorSettings
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import references
from kopf._cogs.structs.references import EVERYTHING


async def test_fetching_works(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.chunk_size = None
    kmock[resource, kmock.namespace(namespace)] << {'items': [{}, {}],
                                                    'metadata': {'resourceVersion': 'v1'}}

    chunks = []
    versions = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
        versions.append(resource_version)

    assert chunks == [[{}, {}]]
    assert versions == ['v1']
    assert len(kmock['list']) == 1
    assert kmock['list', 0].params == {}  # no chunking was requested


async def test_fetching_omits_server_side_selectors_by_default(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock[resource, kmock.namespace(namespace)] << {'items': []}

    async for _, _ in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        pass

    assert 'labelSelector' not in kmock[0].params
    assert 'fieldSelector' not in kmock[0].params
    assert 'shardSelector' not in kmock[0].params


async def test_fetching_passes_server_side_selectors(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    # Also check how several selectors behave (ignored or joined or picked).
    # The exact picking logic is tested elsewhere; here, just the final result.
    settings.watching.label_selectors['unrelated'] = 'kopf.dev/mustbeabsent'
    settings.watching.field_selectors['unrelated'] = 'mustbeabsent=value'
    settings.watching.shard_selectors['unrelated'] = 'shardRange(must-be-absent)'
    settings.watching.label_selectors[resource.plural] = label_selector1 = 'kopf.dev/label=value'
    settings.watching.field_selectors[resource.plural] = field_selector1 = 'spec.field=value'
    settings.watching.shard_selectors[resource.plural] = shard_selector1 = 'shardRange(whatever1)'
    settings.watching.label_selectors[EVERYTHING] = label_selector2 = 'prefect.io/flow-run-id'
    settings.watching.field_selectors[EVERYTHING] = field_selector2 = 'status.phase!=Succeeded,status.phase!=Failed'
    settings.watching.shard_selectors[EVERYTHING] = shard_selector2 = 'shardRange(whatever2)'
    kmock[resource, kmock.namespace(namespace)] << {'items': []}

    async for _, _ in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        pass

    # Alphabetically sorted for predictability.
    assert kmock[0].params['labelSelector'] == f"{label_selector1},{label_selector2}"
    assert kmock[0].params['fieldSelector'] == f"{field_selector1},{field_selector2}"
    assert kmock[0].params['shardSelector'] == shard_selector1  # the 1st matching is used


async def test_chunking_continues_with_all_selectors(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.label_selectors[EVERYTHING] = label_selector1 = 'prefect.io/flow-run-id'
    settings.watching.field_selectors[EVERYTHING] = field_selector1 = 'status.phase!=Succeeded,status.phase!=Failed'
    settings.watching.shard_selectors[EVERYTHING] = shard_selector1 = 'shardRange(whatever2)'

    settings.watching.chunk_size = 2
    kmock[resource, kmock.namespace(namespace), :1] << {'items': [{'spec': 1}, {'spec': 2}],
                                                        'metadata': {'resourceVersion': 'v1',
                                                                     'continue': 'token1'}}
    kmock[resource, kmock.namespace(namespace), :2] << {'items': [{'spec': 3}, {'spec': 4}],
                                                        'metadata': {'resourceVersion': 'v2',
                                                                     'continue': ''}}
    kmock[resource, kmock.namespace(namespace)] << {'items': [{'spec': 'unreachable'}]}

    chunks = []
    versions = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
        versions.append(resource_version)

    # NB: versions are usually stable in K8s, but we simulate the case with different ones.
    assert versions == ['v1', 'v2']
    assert chunks == [
        [{'spec': 1}, {'spec': 2}],
        [{'spec': 3}, {'spec': 4}],
    ]

    expected_selectors = {'labelSelector': label_selector1,
                          'fieldSelector': field_selector1,
                          'shardSelector': shard_selector1}
    assert len(kmock['list']) == 2
    assert kmock['list', 0].params == {'limit': '2', **expected_selectors}
    assert kmock['list', 1].params == {'limit': '2', 'continue': 'token1', **expected_selectors}


async def test_fetching_populates_the_missing_metadata(
        kmock: Any,
        settings: OperatorSettings,
        logger: typedefs.Logger,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock[resource, kmock.namespace(namespace)] << {'items': [{'spec': 'x'}],
                                                    'metadata': {'resourceVersion': 'v1'},
                                                    'apiVersion': 'example/v1',
                                                    'kind': 'ExampleList',
                                                    }
    chunks = []
    async for chunk, resource_version in fetch_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    ):
        chunks.append(chunk)
    assert chunks == [[{'kind': 'Example', 'apiVersion': 'example/v1', 'spec': 'x'},]]
    assert len(kmock['list']) == 1


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
# 410 comes from the expiration of the chunk continuation token (5 mins by default).
@pytest.mark.parametrize('status', [400, 403, 410, 500, 666])
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
        async for _, _ in fetch_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        ):
            pass
    assert e.value.status == status
