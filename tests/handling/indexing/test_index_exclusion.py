import asyncio
import datetime
import logging

import freezegun
import pytest

from kopf._cogs.aiokits.aiotoggles import Toggle
from kopf._cogs.configs.progress import ProgressRecord
from kopf._cogs.structs.ephemera import Memo
from kopf._cogs.structs.ids import HandlerId
from kopf._core.actions.execution import PermanentError, TemporaryError
from kopf._core.actions.lifecycles import all_at_once
from kopf._core.actions.progression import HandlerState, State
from kopf._core.reactor.processing import process_resource_event

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']
EVENT_TYPES_WHEN_GONE = ['DELETED']
EVENT_TYPES = EVENT_TYPES_WHEN_EXISTS + EVENT_TYPES_WHEN_GONE


#
# PART 1/2:
# First, test that the initial indexing state is interpreted properly
# and that it affects the indexing decision (to do or not to do).
#


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_successes_are_removed_from_the_indexing_state(
        resource, namespace, settings, registry, memories, indexers, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    basetime = datetime.datetime.utcnow()  # any "future" time works and affects nothing as long as it is the same
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    record = ProgressRecord(success=True)
    state = State({HandlerId('unrelated'): HandlerState.from_storage(record, basetime=basetime)}, basetime=basetime)
    memory = await memories.recall(raw_body=body)
    memory.indexing_memory.indexing_state = state
    handlers.index_mock.side_effect = 123
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert handlers.index_mock.call_count == 1
    assert memory.indexing_memory.indexing_state is None


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_temporary_failures_with_no_delays_are_reindexed(
        resource, namespace, settings, registry, memories, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    basetime = datetime.datetime.utcnow()  # any "future" time works and affects nothing as long as it is the same
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    record = ProgressRecord(delayed=None)
    state = State({HandlerId('index_fn'): HandlerState.from_storage(record, basetime=basetime)}, basetime=basetime)
    memory = await memories.recall(raw_body=body)
    memory.indexing_memory.indexing_state = state
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert handlers.index_mock.call_count == 1


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_temporary_failures_with_expired_delays_are_reindexed(
        resource, namespace, settings, registry, memories, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    basetime = datetime.datetime.utcnow()  # any "future" time works and affects nothing as long as it is the same
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    record = ProgressRecord(delayed='2020-12-31T23:59:59.000000')
    state = State({HandlerId('index_fn'): HandlerState.from_storage(record, basetime=basetime)}, basetime=basetime)
    memory = await memories.recall(raw_body=body)
    memory.indexing_memory.indexing_state = state
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert handlers.index_mock.call_count == 1


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_permanent_failures_are_not_reindexed(
        resource, namespace, settings, registry, memories, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    basetime = datetime.datetime.utcnow()  # any "future" time works and affects nothing as long as it is the same
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    record = ProgressRecord(failure=True)
    state = State({HandlerId('index_fn'): HandlerState.from_storage(record, basetime=basetime)}, basetime=basetime)
    memory = await memories.recall(raw_body=body)
    memory.indexing_memory.indexing_state = state
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert handlers.index_mock.call_count == 0


#
# PART 2/2:
# Once the resulting indexing state's and its effect on reindexing is tested,
# we can assert that some specific state is reached without actually reindexing.
#


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_removed_and_remembered_on_permanent_errors(
        resource, namespace, settings, registry, memories, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    memory = await memories.recall(raw_body=body)
    handlers.index_mock.side_effect = PermanentError("boo!")
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == set()
    assert memory.indexing_memory.indexing_state['index_fn'].finished == True
    assert memory.indexing_memory.indexing_state['index_fn'].failure == True
    assert memory.indexing_memory.indexing_state['index_fn'].success == False
    assert memory.indexing_memory.indexing_state['index_fn'].message == 'boo!'
    assert memory.indexing_memory.indexing_state['index_fn'].delayed == None


@freezegun.freeze_time('2020-12-31T00:00:00')
@pytest.mark.parametrize('delay_kwargs, expected_delayed', [
    (dict(), datetime.datetime(2020, 12, 31, 0, 1, 0)),
    (dict(delay=0), datetime.datetime(2020, 12, 31, 0, 0, 0)),
    (dict(delay=9), datetime.datetime(2020, 12, 31, 0, 0, 9)),
    (dict(delay=None), None),
])
@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_removed_and_remembered_on_temporary_errors(
        resource, namespace, settings, registry, memories, indexers, index, handlers,
        caplog, event_type, delay_kwargs, expected_delayed):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    memory = await memories.recall(raw_body=body)
    handlers.index_mock.side_effect = TemporaryError("boo!", **delay_kwargs)
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == set()
    assert memory.indexing_memory.indexing_state['index_fn'].finished == False
    assert memory.indexing_memory.indexing_state['index_fn'].failure == False
    assert memory.indexing_memory.indexing_state['index_fn'].success == False
    assert memory.indexing_memory.indexing_state['index_fn'].message == 'boo!'
    assert memory.indexing_memory.indexing_state['index_fn'].delayed == expected_delayed


@pytest.mark.usefixtures('indexed_123')
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_preserved_on_ignored_errors(
        resource, namespace, settings, registry, memories, indexers, index, caplog, event_type, handlers):
    caplog.set_level(logging.DEBUG)
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    memory = await memories.recall(raw_body=body)
    handlers.index_mock.side_effect = Exception("boo!")
    await process_resource_event(
        lifecycle=all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=indexers,
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=asyncio.Queue(),
        resource_indexed=Toggle(),  # used! only to enable indexing.
    )
    assert set(index) == {None}
    assert set(index[None]) == {123}
    assert memory.indexing_memory.indexing_state is None
