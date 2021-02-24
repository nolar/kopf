import asyncio
import logging

import pytest

from kopf.reactor.lifecycles import all_at_once
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.primitives import Toggle

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']
EVENT_TYPES_WHEN_GONE = ['DELETED']
EVENT_TYPES = EVENT_TYPES_WHEN_EXISTS + EVENT_TYPES_WHEN_GONE


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_initially_stored(
        resource, settings, registry, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
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
        resource, settings, registry, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
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
        resource, settings, registry, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1',
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
        resource, settings, registry, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
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
        resource, settings, registry, indexers, index, caplog, event_type, handlers, mocker):

    # Simulate the indexing handler is gone out of scope (this is only one of the ways to do it):
    mocker.patch.object(registry._resource_indexing, 'get_handlers', return_value=[])

    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
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
