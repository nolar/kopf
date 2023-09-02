import asyncio

import pytest

from kopf._cogs.aiokits.aiotime import sleep


async def test_the_only_delay_is_awaited(looptime):
    unslept = await sleep(123)
    assert looptime == 123
    assert unslept is None


async def test_the_shortest_delay_is_awaited(looptime):
    unslept = await sleep([123, 456])
    assert looptime == 123
    assert unslept is None


async def test_specific_delays_only_are_awaited(looptime):
    unslept = await sleep([123, None])
    assert looptime == 123
    assert unslept is None


@pytest.mark.parametrize('delays', [
    pytest.param([1000, -10], id='mixed-signs'),
    pytest.param([-100, -10], id='all-negative'),
    pytest.param(-10, id='alone'),
])
async def test_negative_delays_skip_sleeping(looptime, delays):
    unslept = await sleep(delays)
    assert looptime == 0
    assert unslept is None


@pytest.mark.parametrize('delays', [
    pytest.param([], id='empty-list'),
    pytest.param([None], id='list-of-none'),
])
async def test_no_delays_skip_sleeping(looptime, delays):
    unslept = await sleep(delays)
    assert looptime == 0
    assert unslept is None


async def test_by_event_set_before_time_comes(looptime):
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(7, event.set)
    unslept = await sleep(10, event)
    assert unslept is not None
    assert unslept == 3
    assert looptime == 7


async def test_with_zero_time_and_event_initially_cleared(looptime):
    event = asyncio.Event()
    event.clear()
    unslept = await sleep(0, event)
    assert looptime == 0
    assert unslept is None


async def test_with_zero_time_and_event_initially_set(looptime):
    event = asyncio.Event()
    event.set()
    unslept = await sleep(0, event)
    assert looptime == 0
    assert not unslept  # 0/None; undefined for such case: both goals reached.
