import asyncio
import pytest

from kopf.engines.sleeping import sleep_or_wait


async def test_the_only_delay_is_awaited(timer):
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0.10), timeout=1.0)
    assert 0.10 <= timer.seconds < 0.11
    assert unslept is None


async def test_the_shortest_delay_is_awaited(timer):
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait([0.10, 0.20]), timeout=1.0)
    assert 0.10 <= timer.seconds < 0.11
    assert unslept is None


async def test_specific_delays_only_are_awaited(timer):
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait([0.10, None]), timeout=1.0)
    assert 0.10 <= timer.seconds < 0.11
    assert unslept is None


async def test_passed_delays_skip_sleeping(timer):
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait([0.10, -10]), timeout=1.0)
    assert timer.seconds < 0.01
    assert unslept is None


@pytest.mark.parametrize('delays', [
    pytest.param([], id='empty-list'),
    pytest.param([None], id='list-of-none'),
])
async def test_no_delays_skip_sleeping(timer, delays):
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(delays), timeout=1.0)
    assert timer.seconds < 0.01
    assert unslept is None


async def test_by_event_set_before_time_comes(timer):
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(0.07, event.set)
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0.10, event), timeout=1.0)
    assert unslept is not None
    assert 0.02 <= unslept <= 0.04
    assert 0.06 <= timer.seconds <= 0.08


async def test_with_zero_time_and_event_initially_cleared(timer):
    event = asyncio.Event()
    event.clear()
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0, event), timeout=1.0)
    assert timer.seconds <= 0.01
    assert unslept is None


async def test_with_zero_time_and_event_initially_set(timer):
    event = asyncio.Event()
    event.set()
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0, event), timeout=1.0)
    assert timer.seconds <= 0.01
    assert not unslept  # 0/None; undefined for such case: both goals reached.
