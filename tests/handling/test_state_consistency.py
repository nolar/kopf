"""
Ensure that the framework properly invokes or ignores the handlers
depending on the consistency of the incoming stream of events.
"""
import asyncio
import logging

import pytest

import kopf
from kopf._core.intents.causes import Reason, HANDLER_REASONS
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event
from kopf._cogs.structs.ephemera import Memo


# TODO: `consistency_time` is the target of the tests:
#       consistency_time=None,              => it processes immediately
#       consistency_time=in the past,       => it processes immediately (as if None)
#       consistency_time=within the window, => it sleeps until the time, then processes
#       consistency_time=after the window,  => it assumes the consistency, then processes
#       UPD: no need for within/outside window. Just in the future is enough.
#            the window limiting is a responsibility of another unit (queueing/worker).
#   ALSO:
#       with/without change-detecting handlers.
#       with/without event-watching handlers.
#   ALSO:
#       when awakened by a new event (stream pressure).

# TODO: Test that the on-event() and on-creation/update/deletion happen in different times,
#       that the sleep is between them, and that the latter ones are executed STRICTLY after consistency.
#       NOW, the timing of the handlers is not tested.

@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_implicit_consistency(
        resource, registry, settings, handlers, caplog, cause_mock, cause_reason, k8s_mocked, timer):
    caplog.set_level(logging.DEBUG)

    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    with timer:
        event_queue = asyncio.Queue()
        await process_resource_event(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=OperatorIndexers(),
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': event_type, 'object': {}},
            event_queue=event_queue,
            consistency_time=None,
        )

    assert k8s_mocked.sleep.call_count == 0
    # assert timer.seconds < 0.01

    assert handlers.event_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_past_consistency(
        resource, registry, settings, handlers, caplog, cause_mock, cause_reason, k8s_mocked, timer):
    caplog.set_level(logging.DEBUG)
    loop = asyncio.get_running_loop()

    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    with timer:
        event_queue = asyncio.Queue()
        await process_resource_event(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=OperatorIndexers(),
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': event_type, 'object': {}},
            event_queue=event_queue,
            consistency_time=loop.time() - 10,
        )

    assert k8s_mocked.sleep.call_count == 1
    delay = k8s_mocked.sleep.call_args[0][0]
    assert delay < 0

    # assert timer.seconds < 0.01

    assert handlers.event_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
async def test_future_consistency(
        resource, registry, settings, handlers, caplog, cause_mock, cause_reason, k8s_mocked, timer):
    caplog.set_level(logging.DEBUG)
    loop = asyncio.get_running_loop()

    settings.persistence.consistency_timeout = 5

    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    with timer:
        event_queue = asyncio.Queue()
        await process_resource_event(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=OperatorIndexers(),
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': event_type, 'object': {}},
            event_queue=event_queue,
            consistency_time=loop.time() + 3,
        )

    assert k8s_mocked.sleep.call_count == 1
    delay = k8s_mocked.sleep.call_args[0][0]
    assert 2.9 < delay < 3.0
    # assert timer.seconds < 0.01

    assert handlers.event_mock.call_count == 1
    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)


# TODO: we definitely need a loop with a fake time!
#           and the time should start with 0.
#           and it should have zero-waste on code overhead, only on sleeps.


# TODO:
#   And then, there will be separate splitting for:
#       - watcher() -> processor() with proper/expected consistency_time,
#       - processor() -> handlers() properly according to various consistency times.
#   This leaks some abstractions of how consistency works to the tests, but can be tolerated
#   due to complexity of units, with "consistency time" treated as a unit contract.
#   In addition, the whole bundle can be tested:
#       - watcher() -> handlers() -- i.e. a full simulation of the watch-stream.
