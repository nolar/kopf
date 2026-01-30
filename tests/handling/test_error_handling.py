import asyncio

import pytest

import kopf
from kopf._cogs.structs.ephemera import Memo
from kopf._core.actions.execution import PermanentError, TemporaryError
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import HANDLER_REASONS, Reason
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_fatal_error_stops_handler(
        registry, settings, handlers, extrahandlers, resource, cause_mock, cause_type,
        assert_logs, k8s_mocked, looptime):
    name1 = f'{cause_type}_fn'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_type
    handlers.create_mock.side_effect = PermanentError("oops")
    handlers.update_mock.side_effect = PermanentError("oops")
    handlers.delete_mock.side_effect = PermanentError("oops")
    handlers.resume_mock.side_effect = PermanentError("oops")

    await process_resource_event(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == Reason.RESUME else 0)

    assert looptime == 0
    assert k8s_mocked.patch.called

    patch = k8s_mocked.patch.call_args_list[0][1]['payload']
    assert patch['status']['kopf']['progress'] is not None
    assert patch['status']['kopf']['progress'][name1]['failure'] is True
    assert patch['status']['kopf']['progress'][name1]['message'] == 'oops'

    assert_logs([
        "Handler .+ failed permanently: oops",
    ])


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_retry_error_delays_handler(
        registry, settings, handlers, extrahandlers, resource, cause_mock, cause_type,
        assert_logs, k8s_mocked, looptime):
    name1 = f'{cause_type}_fn'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_type
    handlers.create_mock.side_effect = TemporaryError("oops")
    handlers.update_mock.side_effect = TemporaryError("oops")
    handlers.delete_mock.side_effect = TemporaryError("oops")
    handlers.resume_mock.side_effect = TemporaryError("oops")

    await process_resource_event(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == Reason.RESUME else 0)

    assert looptime == 0
    assert k8s_mocked.patch.called

    patch = k8s_mocked.patch.call_args_list[0][1]['payload']
    assert patch['status']['kopf']['progress'] is not None
    assert patch['status']['kopf']['progress'][name1]['failure'] is False
    assert patch['status']['kopf']['progress'][name1]['success'] is False
    assert patch['status']['kopf']['progress'][name1]['delayed']

    assert_logs([
        "Handler .+ failed temporarily: oops",
    ])


# The extrahandlers are needed to prevent the cycle ending and status purging.
@pytest.mark.parametrize('cause_type', HANDLER_REASONS)
async def test_arbitrary_error_delays_handler(
        registry, settings, handlers, extrahandlers, resource, cause_mock, cause_type,
        assert_logs, k8s_mocked, looptime):
    name1 = f'{cause_type}_fn'

    event_type = None if cause_type == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_type
    handlers.create_mock.side_effect = Exception("oops")
    handlers.update_mock.side_effect = Exception("oops")
    handlers.delete_mock.side_effect = Exception("oops")
    handlers.resume_mock.side_effect = Exception("oops")

    await process_resource_event(
        lifecycle=kopf.lifecycles.one_by_one,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=asyncio.Queue(),
    )

    assert handlers.create_mock.call_count == (1 if cause_type == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_type == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_type == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_type == Reason.RESUME else 0)

    assert looptime == 0
    assert k8s_mocked.patch.called

    patch = k8s_mocked.patch.call_args_list[0][1]['payload']
    assert patch['status']['kopf']['progress'] is not None
    assert patch['status']['kopf']['progress'][name1]['failure'] is False
    assert patch['status']['kopf']['progress'][name1]['success'] is False
    assert patch['status']['kopf']['progress'][name1]['delayed']

    assert_logs([
        "Handler .+ failed with an exception and will try again in 60 seconds: oops",
    ])
