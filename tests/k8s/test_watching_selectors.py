import asyncio
from typing import Any

from kopf._cogs.clients.watching import Bookmark, continuous_watch, watch_objs
from kopf._cogs.configs.configuration import OperatorSettings, WatchListSelector
from kopf._cogs.structs import references


async def test_watch_omits_server_side_selectors_by_default(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    kmock['watch', resource, kmock.namespace(namespace)] << ()

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


async def test_watch_passes_server_side_selectors_with_watch_params(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.server_timeout = 42
    label_selector = 'prefect.io/flow-run-id'
    field_selector = 'status.phase!=Succeeded,status.phase!=Failed'
    kmock['watch', resource, kmock.namespace(namespace)] << ()

    async for _ in watch_objs(
            settings=settings,
            resource=resource,
            namespace=namespace,
            since='123',
            server_side_selector=WatchListSelector(
                label_selector=label_selector,
                field_selector=field_selector,
            ),
            operator_pause_waiter=asyncio.Future()):
        pass

    assert kmock[0].url.query['watch'] == 'true'
    assert kmock[0].url.query['allowWatchBookmarks'] == 'true'
    assert kmock[0].url.query['resourceVersion'] == '123'
    assert kmock[0].url.query['timeoutSeconds'] == '42'
    assert kmock[0].url.query['labelSelector'] == label_selector
    assert kmock[0].url.query['fieldSelector'] == field_selector


async def test_continuous_watch_uses_same_selectors_for_list_and_watch(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    label_selector = 'prefect.io/flow-run-id'
    field_selector = 'status.phase!=Succeeded,status.phase!=Failed'
    selector = WatchListSelector(
        label_selector=label_selector,
        field_selector=field_selector,
    )
    kmock['list', resource, kmock.namespace(namespace)] << {
        'metadata': {'resourceVersion': '100'},
        'items': [],
    }
    kmock['watch', resource, kmock.namespace(namespace)] << (
        {'type': 'ERROR', 'object': {'code': 410}},
    )

    events = []
    async for event in continuous_watch(
            settings=settings,
            resource=resource,
            namespace=namespace,
            server_side_selector=selector,
            operator_pause_waiter=asyncio.Future()):
        events.append(event)

    assert events == [Bookmark.LISTED]
    assert kmock[0].url.query['labelSelector'] == label_selector
    assert kmock[0].url.query['fieldSelector'] == field_selector
    assert kmock[1].url.query['labelSelector'] == label_selector
    assert kmock[1].url.query['fieldSelector'] == field_selector
    assert kmock[1].url.query['resourceVersion'] == '100'
