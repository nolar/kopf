import asyncio

import pytest

from kopf._cogs.structs.bodies import RawBody, RawEvent
from kopf._cogs.structs.references import Insights
from kopf._core.reactor.observation import process_discovered_namespace_event


async def test_initial_listing_is_ignored(looptime):
    insights = Insights()
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(0))
    with pytest.raises(asyncio.TimeoutError):
        async with insights.revised:
            await asyncio.wait_for(insights.revised.wait(), timeout=1.23)
    await task

    assert looptime == 1.23
    assert not insights.namespaces


@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED'])
async def test_followups_for_addition(looptime, etype):
    insights = Insights()
    e1 = RawEvent(type=etype, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(9))
    async with insights.revised:
        await insights.revised.wait()
    await task
    assert looptime == 9
    assert insights.namespaces == {'ns1'}


@pytest.mark.parametrize('etype', ['DELETED'])
async def test_followups_for_deletion(looptime, etype):
    insights = Insights()
    insights.namespaces.add('ns1')
    e1 = RawEvent(type=etype, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(9))
    async with insights.revised:
        await insights.revised.wait()
    await task
    assert looptime == 9
    assert not insights.namespaces
