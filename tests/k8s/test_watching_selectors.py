import asyncio
from typing import Any

from kopf._cogs.clients.watching import Bookmark, continuous_watch, watch_objs
from kopf._cogs.configs.configuration import OperatorSettings
from kopf._cogs.structs import references
from kopf._cogs.structs.references import EVERYTHING

EOS = ({'type': 'ERROR', 'object': {'code': 410}},)


async def test_watch_omits_server_side_selectors_by_default(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock['watch', resource, kmock.namespace(namespace)] << EOS

    async for _ in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='123',
            operator_pause_waiter=asyncio.Future()):
        pass

    assert kmock[0].params['watch'] == 'true'
    assert kmock[0].params['allowWatchBookmarks'] == 'true'
    assert kmock[0].params['resourceVersion'] == '123'
    assert 'sendInitialEvents' not in kmock[0].params
    assert 'labelSelector' not in kmock[0].params
    assert 'fieldSelector' not in kmock[0].params
    assert 'shardSelector' not in kmock[0].params


async def test_watch_passes_server_side_selectors_with_watch_params(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.server_timeout = 42

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
    kmock['watch', resource, kmock.namespace(namespace)] << EOS

    async for _ in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='123',
            operator_pause_waiter=asyncio.Future()):
        pass

    # Alphabetically sorted for predictability.
    assert kmock[0].params['watch'] == 'true'
    assert kmock[0].params['allowWatchBookmarks'] == 'true'
    assert kmock[0].params['resourceVersion'] == '123'
    assert kmock[0].params['timeoutSeconds'] == '42'
    assert kmock[0].params['labelSelector'] == f"{label_selector1},{label_selector2}"
    assert kmock[0].params['fieldSelector'] == f"{field_selector1},{field_selector2}"
    assert kmock[0].params['shardSelector'] == shard_selector1  # the 1st matching is used


async def test_watch_passes_initial_streaming_params(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.initial_streaming = True
    kmock['watch', resource, kmock.namespace(namespace)] << EOS

    async for _ in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            operator_pause_waiter=asyncio.Future()):
        pass

    assert len(kmock['list']) == 0
    assert kmock[0].params['watch'] == 'true'
    assert kmock[0].params['allowWatchBookmarks'] == 'true'
    assert kmock[0].params['sendInitialEvents'] == 'true'
    assert kmock[0].params['resourceVersionMatch'] == 'NotOlderThan'


async def test_continuous_watch_uses_same_selectors_for_list_and_watch(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    label_selector = settings.watching.label_selectors[EVERYTHING] = 'prefect.io/flow-run-id'
    field_selector = settings.watching.field_selectors[EVERYTHING] = 'status.phase!=Succeeded,status.phase!=Failed'
    shard_selector = settings.watching.shard_selectors[EVERYTHING] = 'shardRange(whatever)'
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }
    kmock['watch', resource, kmock.namespace(namespace)] << EOS

    events = [event async for event in continuous_watch(settings=settings,
                                                        resource=resource,
                                                        namespace=namespace,
                                                        operator_pause_waiter=asyncio.Future())]

    assert events == [Bookmark.LISTED]
    assert kmock[0].params['labelSelector'] == label_selector
    assert kmock[0].params['fieldSelector'] == field_selector
    assert kmock[0].params['shardSelector'] == shard_selector
    assert kmock[1].params['labelSelector'] == label_selector
    assert kmock[1].params['fieldSelector'] == field_selector
    assert kmock[1].params['shardSelector'] == shard_selector
    assert kmock[1].params['resourceVersion'] == '100'
