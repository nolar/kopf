import asyncio

import async_timeout
import pytest

from kopf.structs.primitives import Toggle


async def test_created_as_off():
    toggle = Toggle()

    assert not toggle
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_initialised_as_off():
    toggle = Toggle(False)

    assert not toggle
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_initialised_as_on():
    toggle = Toggle(True)

    assert toggle
    assert toggle.is_on()
    assert not toggle.is_off()


async def test_turning_on():
    toggle = Toggle(False)
    await toggle.turn_on()

    assert toggle
    assert toggle.is_on()
    assert not toggle.is_off()


async def test_turning_off():
    toggle = Toggle(True)
    await toggle.turn_off()

    assert not toggle
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_waiting_until_on_fails_when_not_turned_on():
    toggle = Toggle(False)

    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.1) as timeout:
            await toggle.wait_for_on()

    assert not toggle
    assert timeout.expired


async def test_waiting_until_off_fails_when_not_turned_off():
    toggle = Toggle(True)

    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.1) as timeout:
            await toggle.wait_for_off()

    assert toggle
    assert timeout.expired


async def test_waiting_until_on_wakes_when_turned_on(timer):
    toggle = Toggle(False)

    async def delayed_turning_on(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_on()

    async with timer, async_timeout.timeout(1.0) as timeout:
        asyncio.create_task(delayed_turning_on(0.05))
        await toggle.wait_for_on()

    assert toggle
    assert not timeout.expired
    assert timer.seconds < 0.5  # approx. 0.05 plus some code overhead


async def test_waiting_until_off_wakes_when_turned_off(timer):
    toggle = Toggle(True)

    async def delayed_turning_off(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_off()

    async with timer, async_timeout.timeout(1.0) as timeout:
        asyncio.create_task(delayed_turning_off(0.05))
        await toggle.wait_for_off()

    assert not toggle
    assert not timeout.expired
    assert timer.seconds < 0.5  # approx. 0.05 plus some code overhead
