import asyncio

from kopf.engines.sleeping import sleep_or_wait


async def test_sleep_or_wait_by_delay_reached(timer):
    event = asyncio.Event()
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0.10, event), timeout=1.0)
    assert 0.10 <= timer.seconds < 0.11
    assert unslept is None


async def test_sleep_or_wait_by_event_set(timer):
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(0.07, event.set)
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0.10, event), timeout=1.0)
    assert 0.06 <= timer.seconds <= 0.08
    assert 0.02 <= unslept  <= 0.04


async def test_sleep_or_wait_with_zero_time_and_event_cleared(timer):
    event = asyncio.Event()
    event.clear()
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0, event), timeout=1.0)
    assert timer.seconds <= 0.01
    assert unslept is None


async def test_sleep_or_wait_with_zero_time_and_event_preset(timer):
    event = asyncio.Event()
    event.set()
    with timer:
        unslept = await asyncio.wait_for(sleep_or_wait(0, event), timeout=1.0)
    assert timer.seconds <= 0.01
    assert not unslept  # 0/None; undefined for such case: both goals reached.
