import asyncio

import pytest

from kopf._cogs.aiokits.aiotoggles import ToggleSet
from kopf._cogs.structs.ephemera import Memo
from kopf._core.actions.lifecycles import all_at_once
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']
EVENT_TYPES_WHEN_GONE = ['DELETED']
EVENT_TYPES = EVENT_TYPES_WHEN_EXISTS + EVENT_TYPES_WHEN_GONE


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_reporting_on_resource_readiness(
        resource, settings, registry, indexers, event_type, handlers, looptime):
    operator_indexed = ToggleSet(all)
    resource_indexed = await operator_indexed.make_toggle()
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
    assert looptime == 0
    assert operator_indexed.is_on()
    assert set(operator_indexed) == set()  # save RAM
    assert handlers.event_mock.called


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_blocking_when_operator_is_not_ready(
        resource, settings, registry, indexers, event_type, handlers, looptime):
    operator_indexed = ToggleSet(all)
    resource_listed = await operator_indexed.make_toggle()
    resource_indexed = await operator_indexed.make_toggle()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(process_resource_event(
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
        ), timeout=1.23)
    assert looptime == 1.23
    assert operator_indexed.is_off()
    assert set(operator_indexed) == {resource_listed}
    assert not handlers.event_mock.called


@pytest.mark.parametrize('pressure_on', [False, True])
@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_unblocking_once_operator_is_ready(
        resource, settings, registry, indexers, event_type, pressure_on, handlers, looptime):

    async def delayed_readiness(delay: float):
        await asyncio.sleep(delay)
        await resource_listed.turn_to(True)

    # An extra check: no event is skipped regardless of the presence of newer events.
    stream_pressure = asyncio.Event()
    if pressure_on:
        stream_pressure.set()

    operator_indexed = ToggleSet(all)
    resource_listed = await operator_indexed.make_toggle()
    resource_indexed = await operator_indexed.make_toggle()
    asyncio.create_task(delayed_readiness(1.23))
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
        stream_pressure=stream_pressure,
        operator_indexed=operator_indexed,
        resource_indexed=resource_indexed,
    )
    assert looptime == 1.23
    assert operator_indexed.is_on()
    assert set(operator_indexed) == {resource_listed}
    assert handlers.event_mock.called
