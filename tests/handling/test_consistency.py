"""
Ensure that the framework properly invokes or ignores the handlers
depending on the consistency or inconsistency of the incoming stream of events.
"""
import asyncio
import contextlib

import pytest

import kopf
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import HANDLER_REASONS, Reason
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event


@pytest.fixture(autouse=True)
def _no_throttling(mocker, resource, registry):
    @contextlib.asynccontextmanager
    async def not_throttled(*_, **__):
        yield True

    mocker.patch('kopf._core.actions.throttlers.throttled', not_throttled)


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_preexisting_consistency(
        resource, registry, settings, handlers, cause_mock, cause_reason, k8s_mocked, looptime):
    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body, noticed_by_listing=event_type is None)
    memory.expected_version = None  # assume the pre-existing consistency
    memory.consistency_time = None  # assume the pre-existing consistency

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=event_queue,
    )

    # The state remains unaffected.
    assert memory.consistency_time is None
    assert memory.expected_version is None

    assert looptime == 0
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)

    changing_mock = (
        handlers.create_mock if cause_reason == Reason.CREATE else
        handlers.update_mock if cause_reason == Reason.UPDATE else
        handlers.delete_mock if cause_reason == Reason.DELETE else
        handlers.resume_mock if cause_reason == Reason.RESUME else
        None  # and fail
    )
    assert handlers.event_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert handlers.index_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert changing_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_past_consistency(
        resource, registry, settings, handlers, cause_mock, cause_reason, k8s_mocked, looptime):
    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body, noticed_by_listing=event_type is None)
    memory.expected_version = 'never-arrives'
    memory.consistency_time = 50  # expect the consistency in the past

    await asyncio.sleep(100)  # fast-forward to the future
    assert looptime == 100

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=event_queue,
    )

    # The expired state was reset, regardless of the versions.
    assert memory.consistency_time is None
    assert memory.expected_version is None

    assert looptime == 100
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)

    changing_mock = (
        handlers.create_mock if cause_reason == Reason.CREATE else
        handlers.update_mock if cause_reason == Reason.UPDATE else
        handlers.delete_mock if cause_reason == Reason.DELETE else
        handlers.resume_mock if cause_reason == Reason.RESUME else
        None  # and fail
    )
    assert handlers.event_mock.call_args_list[0].kwargs['_time'] == 100  # called instantly
    assert handlers.index_mock.call_args_list[0].kwargs['_time'] == 100  # called instantly
    assert changing_mock.call_args_list[0].kwargs['_time'] == 100  # called instantly


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_future_consistency(
        resource, registry, settings, handlers, cause_mock, cause_reason, k8s_mocked, looptime):
    settings.persistence.consistency_timeout = 55

    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body, noticed_by_listing=event_type is None)
    memory.expected_version = 'never-arrives'
    memory.consistency_time = 123  # expect the consistency in the future

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=event_queue,
    )

    # The state remains unaffected, not prolonged.
    assert memory.consistency_time == 123
    assert memory.expected_version == 'never-arrives'

    assert looptime == 123  # non-zero means it has slept for some time
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)

    changing_mock = (
        handlers.create_mock if cause_reason == Reason.CREATE else
        handlers.update_mock if cause_reason == Reason.UPDATE else
        handlers.delete_mock if cause_reason == Reason.DELETE else
        handlers.resume_mock if cause_reason == Reason.RESUME else
        None  # and fail
    )
    assert handlers.event_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert handlers.index_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert changing_mock.call_args_list[0].kwargs['_time'] == 123  # called after the deemed consistency


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_no_consistency_wait_without_changing_handlers(
        resource, registry, settings, cause_mock, cause_reason, watching_handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body)
    memory.expected_version = 'never-arrives'
    memory.consistency_time = 456  # expect the consistency in the future

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': '...', 'object': body},
        event_queue=event_queue,
    )

    # The state remain unaffected.
    assert memory.consistency_time == 456
    assert memory.expected_version == 'never-arrives'

    assert looptime == 0  # zero means it did not sleep, as expected
    assert watching_handlers.event_mock.call_count == 1
    assert watching_handlers.index_mock.call_count == 1
    assert watching_handlers.event_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert watching_handlers.index_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly


async def test_no_consistency_wait_on_deletion_event_and_gone_cause(
        resource, registry, settings, handlers, cause_mock, assert_logs,
        k8s_mocked, looptime):
    cause_mock.reason = Reason.GONE

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body)
    memory.expected_version = 'never-arrives'
    memory.consistency_time = 456  # expect the consistency in the future

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': 'DELETED', 'object': body},
        event_queue=event_queue,
    )

    # The state remain unaffected.
    assert memory.consistency_time == 456
    assert memory.expected_version == 'never-arrives'

    assert looptime == 0  # zero means it did not sleep: GONE bypasses consistency wait
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 0
    assert handlers.create_mock.call_count == 0
    assert handlers.update_mock.call_count == 0
    assert handlers.delete_mock.call_count == 0
    assert handlers.resume_mock.call_count == 0
    assert_logs(["Deleted, really deleted"])


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_stream_pressure_awakening_prevents_change_handlers(
        resource, registry, settings, cause_mock, cause_reason, handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body)
    memory.expected_version = 'never-arrives'
    memory.consistency_time = 456  # expect the consistency in the future

    stream_pressure = asyncio.Event()
    asyncio.get_running_loop().call_later(123, stream_pressure.set)

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': '...', 'object': body},
        event_queue=event_queue,
        stream_pressure=stream_pressure,
    )

    # The state remain unaffected.
    assert memory.consistency_time == 456
    assert memory.expected_version == 'never-arrives'

    assert looptime == 123  # means: it has slept until awakened, but not until consistency
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.event_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly
    assert handlers.index_mock.call_args_list[0].kwargs['_time'] == 0  # called instantly

    changing_mock = (
        handlers.create_mock if cause_reason == Reason.CREATE else
        handlers.update_mock if cause_reason == Reason.UPDATE else
        handlers.delete_mock if cause_reason == Reason.DELETE else
        handlers.resume_mock if cause_reason == Reason.RESUME else
        None  # and fail
    )
    assert changing_mock.call_count == 0  # zero means the state-dependent handler was skipped


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_patching_with_real_timeout_arms_the_resource_version_expectation(
        resource, registry, settings, cause_mock, cause_reason, watching_handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason

    settings.persistence.consistency_timeout = 3.45
    await asyncio.sleep(5)
    assert looptime == 5  # to distinguish just the timeout from now+timeout

    k8s_mocked.patch.return_value = {'metadata': {'resourceVersion': 'some-rv'}}
    watching_handlers.event_mock.return_value = 'something-to-provoke-patching'

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body)

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': '...', 'object': body},
        event_queue=event_queue,
    )
    assert memory.expected_version == 'some-rv'
    assert memory.consistency_time == 8.45
    assert looptime == 5  # no time passed since the start means everything has happened instantly


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_patching_with_zero_timeout_skips_the_resource_version_expectation(
        resource, registry, settings, cause_mock, cause_reason, watching_handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason

    settings.persistence.consistency_timeout = 0

    k8s_mocked.patch.return_value = {'metadata': {'resourceVersion': 'some-rv'}}
    watching_handlers.event_mock.return_value = 'something-to-provoke-patching'

    body = {'metadata': {'uid': 'uid1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body)

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': '...', 'object': body},
        event_queue=event_queue,
    )
    assert memory.expected_version is None
    assert memory.consistency_time is None
    assert looptime == 0  # zero means everything has happened instantly


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_matching_resource_version_resets_the_expectation(
        resource, registry, settings, cause_mock, cause_reason, handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason
    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'

    body = {'metadata': {'uid': 'uid1', 'resourceVersion': 'rv1'}}
    memories = ResourceMemories()
    memory = await memories.recall(body, noticed_by_listing=event_type is None)
    memory.consistency_time = 9.99
    memory.expected_version = 'rv1'

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=event_queue,
    )
    assert memory.expected_version is None
    assert memory.consistency_time is None
    assert looptime == 0  # zero means everything has happened instantly

    # The matching resource version must call something instantly, too.
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_mismatching_resource_version_keeps_the_expectation(
        resource, registry, settings, cause_mock, cause_reason, handlers,
        k8s_mocked, looptime):
    cause_mock.reason = cause_reason
    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'

    body = {'metadata': {'uid': 'uid1', 'resourceVersion': 'arrived-rv'}}
    memories = ResourceMemories()
    memory = await memories.recall(body, noticed_by_listing=event_type is None)
    memory.expected_version = 'rv-that-does-not-arrive'
    memory.consistency_time = 3.45

    stream_pressure = asyncio.Event()
    stream_pressure.set()
    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=memories,
        memobase=Memo(),
        raw_event={'type': event_type, 'object': body},
        event_queue=event_queue,
        stream_pressure=stream_pressure,
    )
    assert memory.expected_version == 'rv-that-does-not-arrive'
    assert memory.consistency_time == 3.45
    assert looptime == 0  # zero means everything has happened instantly

    # The mismatching resource version does call the watching handlers, but not the stateful ones.
    assert handlers.event_mock.call_count == 1
    assert handlers.index_mock.call_count == 1
    assert handlers.create_mock.call_count == 0
    assert handlers.update_mock.call_count == 0
    assert handlers.delete_mock.call_count == 0
    assert handlers.resume_mock.call_count == 0
