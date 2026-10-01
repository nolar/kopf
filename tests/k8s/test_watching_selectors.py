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

    assert kmock[0].url.query['watch'] == 'true'
    assert kmock[0].url.query['allowWatchBookmarks'] == 'true'
    assert kmock[0].url.query['resourceVersion'] == '123'
    assert 'labelSelector' not in kmock[0].url.query
    assert 'fieldSelector' not in kmock[0].url.query
    assert 'shardSelector' not in kmock[0].url.query


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
    assert kmock[0].url.query['watch'] == 'true'
    assert kmock[0].url.query['allowWatchBookmarks'] == 'true'
    assert kmock[0].url.query['resourceVersion'] == '123'
    assert kmock[0].url.query['timeoutSeconds'] == '42'
    assert kmock[0].url.query['labelSelector'] == f"{label_selector1},{label_selector2}"
    assert kmock[0].url.query['fieldSelector'] == f"{field_selector1},{field_selector2}"
    assert kmock[0].url.query['shardSelector'] == shard_selector1  # the 1st matching is used


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

    events = []
    async for event in continuous_watch(
            settings=settings,
            resource=resource,
            namespace=namespace,
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert events == [Bookmark.LISTED]
    assert kmock[0].url.query['labelSelector'] == label_selector
    assert kmock[0].url.query['fieldSelector'] == field_selector
    assert kmock[0].url.query['shardSelector'] == shard_selector
    assert kmock[1].url.query['labelSelector'] == label_selector
    assert kmock[1].url.query['fieldSelector'] == field_selector
    assert kmock[1].url.query['shardSelector'] == shard_selector
    assert kmock[1].url.query['resourceVersion'] == '100'
