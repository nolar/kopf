import asyncio

import pytest

from kopf._cogs.aiokits.aiotoggles import Toggle
from kopf._cogs.structs.ephemera import Memo
from kopf._core.actions.lifecycles import all_at_once
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']
EVENT_TYPES_WHEN_GONE = ['DELETED']
EVENT_TYPES = EVENT_TYPES_WHEN_EXISTS + EVENT_TYPES_WHEN_GONE


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_initially_stored(
        resource, namespace, settings, registry, indexers, index, event_type, handlers):
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    handlers.index_mock.return_value = 123
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == {None}
    assert set(index[None]) == {123}


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_overwritten(
        resource, namespace, settings, registry, indexers, index, event_type, handlers):
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    handlers.index_mock.return_value = 456
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == {None}
    assert set(index[None]) == {456}


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_preserved_on_logical_deletion(
        resource, namespace, settings, registry, indexers, index, event_type, handlers):
    body = {'metadata': {'namespace': namespace, 'name': 'name1',
                         'deletionTimestamp': '...'}}
    handlers.index_mock.return_value = 456
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == {None}
    assert set(index[None]) == {456}


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_GONE)
async def test_removed_on_physical_deletion(
        resource, namespace, settings, registry, indexers, index, event_type, handlers):
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    handlers.index_mock.return_value = 456
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == set()


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_removed_on_filters_mismatch(
        resource, namespace, settings, registry, indexers, index,
        event_type, handlers, mocker):

    # Simulate the indexing handler is gone out of scope (this is only one of the ways to do it):
    mocker.patch.object(registry._indexing, 'get_handlers', return_value=[])

    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    handlers.index_mock.return_value = 123
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == set()
