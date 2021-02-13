import asyncio

import async_timeout
import pytest

from kopf.reactor.observation import process_discovered_namespace_event
from kopf.structs.bodies import RawBody, RawEvent
from kopf.structs.references import Insights


async def test_initial_listing_is_ignored():
    insights = Insights()
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(0))
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.1) as timeout:
            async with insights.revised:
                await insights.revised.wait()
    await task
    assert timeout.expired
    assert not insights.namespaces


@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED'])
async def test_followups_for_addition(timer, etype):
    insights = Insights()
    e1 = RawEvent(type=etype, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 0.11
    assert insights.namespaces == {'ns1'}


@pytest.mark.parametrize('etype', ['DELETED'])
async def test_followups_for_deletion(timer, etype):
    insights = Insights()
    insights.namespaces.add('ns1')
    e1 = RawEvent(type=etype, object=RawBody(metadata={'name': 'ns1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_namespace_event(
            insights=insights, raw_event=e1, namespaces=['ns*'])

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 0.11
    assert not insights.namespaces
