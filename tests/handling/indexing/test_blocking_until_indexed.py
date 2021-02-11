import asyncio
import logging

import async_timeout
import pytest

from kopf.reactor.lifecycles import all_at_once
from kopf.reactor.processing import process_resource_event
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.primitives import ToggleSet

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']
EVENT_TYPES_WHEN_GONE = ['DELETED']
EVENT_TYPES = EVENT_TYPES_WHEN_EXISTS + EVENT_TYPES_WHEN_GONE


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_reporting_on_resource_readiness(
        resource, settings, registry, indexers, caplog, event_type, handlers, timer):
    caplog.set_level(logging.DEBUG)

    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
    async with timer, async_timeout.timeout(0.5) as timeout:
        await process_resource_event(
            lifecycle=all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=indexers,
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': event_type, 'object': {}},
            event_queue=asyncio.Queue(),
            operator_indexed=operator_indexed,
            resource_indexed=resource_indexed,
        )
    assert not timeout.expired
    assert timer.seconds < 0.2  # asap, nowait
    assert operator_indexed.is_on()
    assert set(operator_indexed) == set()  # save RAM
    assert handlers.event_mock.called


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_blocking_when_operator_is_not_ready(
        resource, settings, registry, indexers, caplog, event_type, handlers, timer):
    caplog.set_level(logging.DEBUG)

    operator_indexed = ToggleSet(all)
    resource_listed = await operator_indexed.make_toggle()
    resource_indexed = await operator_indexed.make_toggle()
    with pytest.raises(asyncio.TimeoutError):
        async with timer, async_timeout.timeout(0.2) as timeout:
            await process_resource_event(
                lifecycle=all_at_once,
                registry=registry,
                settings=settings,
                resource=resource,
                indexers=indexers,
                memories=ResourceMemories(),
                memobase=Memo(),
                raw_event={'type': event_type, 'object': {}},
                event_queue=asyncio.Queue(),
                operator_indexed=operator_indexed,
                resource_indexed=resource_indexed,
            )
    assert timeout.expired
    assert 0.2 < timer.seconds < 0.4
    assert operator_indexed.is_off()
    assert set(operator_indexed) == {resource_listed}
    assert not handlers.event_mock.called


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_unblocking_once_operator_is_ready(
        resource, settings, registry, indexers, caplog, event_type, handlers, timer):
    caplog.set_level(logging.DEBUG)

    async def delayed_readiness(delay: float):
        await asyncio.sleep(delay)
        await resource_listed.turn_to(True)

    operator_indexed = ToggleSet(all)
    resource_listed = await operator_indexed.make_toggle()
    resource_indexed = await operator_indexed.make_toggle()
    async with timer, async_timeout.timeout(1.0) as timeout:
        asyncio.create_task(delayed_readiness(0.2))
        await process_resource_event(
            lifecycle=all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            indexers=indexers,
            memories=ResourceMemories(),
            memobase=Memo(),
            raw_event={'type': event_type, 'object': {}},
            event_queue=asyncio.Queue(),
            operator_indexed=operator_indexed,
            resource_indexed=resource_indexed,
        )
    assert not timeout.expired
    assert 0.2 < timer.seconds < 0.4
    assert operator_indexed.is_on()
    assert set(operator_indexed) == {resource_listed}
    assert handlers.event_mock.called
