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


async def test_watch_passes_server_side_selectors_with_watch_params(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    settings.watching.server_timeout = 42
    label_selector = settings.watching.label_selectors[EVERYTHING] = 'prefect.io/flow-run-id'
    field_selector = settings.watching.field_selectors[EVERYTHING] = 'status.phase!=Succeeded,status.phase!=Failed'
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
    assert kmock[0].url.query['timeoutSeconds'] == '42'
    assert kmock[0].url.query['labelSelector'] == label_selector
    assert kmock[0].url.query['fieldSelector'] == field_selector


async def test_continuous_watch_uses_same_selectors_for_list_and_watch(
        kmock: Any,
        settings: OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    label_selector = settings.watching.label_selectors[EVERYTHING] = 'prefect.io/flow-run-id'
    field_selector = settings.watching.field_selectors[EVERYTHING] = 'status.phase!=Succeeded,status.phase!=Failed'
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
    assert kmock[1].url.query['labelSelector'] == label_selector
    assert kmock[1].url.query['fieldSelector'] == field_selector
    assert kmock[1].url.query['resourceVersion'] == '100'
