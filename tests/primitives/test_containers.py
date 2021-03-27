import asyncio

import async_timeout
import pytest

from kopf.structs.primitives import Container


async def test_empty_by_default():
    container = Container()
    with pytest.raises(asyncio.TimeoutError):
        with async_timeout.timeout(0.1) as timeout:
            await container.wait()
    assert timeout.expired


async def test_does_not_wake_up_when_reset(event_loop, timer):
    container = Container()

    async def reset_it():
        await container.reset()

    event_loop.call_later(0.05, asyncio.create_task, reset_it())

    with pytest.raises(asyncio.TimeoutError):
        with async_timeout.timeout(0.1) as timeout:
            await container.wait()

    assert timeout.expired


async def test_wakes_up_when_preset(event_loop, timer):
    container = Container()
    await container.set(123)

    with timer, async_timeout.timeout(10) as timeout:
        result = await container.wait()

    assert not timeout.expired
    assert timer.seconds <= 0.1
    assert result == 123


async def test_wakes_up_when_set(event_loop, timer):
    container = Container()

    async def set_it():
        await container.set(123)

    event_loop.call_later(0.1, asyncio.create_task, set_it())

    with timer, async_timeout.timeout(10) as timeout:
        result = await container.wait()

    assert not timeout.expired
    assert 0.1 <= timer.seconds <= 0.2
    assert result == 123


async def test_iterates_when_set(event_loop, timer):
    container = Container()

    async def set_it(v):
        await container.set(v)

    event_loop.call_later(0.1, asyncio.create_task, set_it(123))
    event_loop.call_later(0.2, asyncio.create_task, set_it(234))

    values = []
    with timer, async_timeout.timeout(10) as timeout:
        async for value in container.as_changed():
            values.append(value)
            if value == 234:
                break

    assert not timeout.expired
    assert 0.2 <= timer.seconds <= 0.3
    assert values == [123, 234]


async def test_iterates_when_preset(event_loop, timer):
    container = Container()
    await container.set(123)

    values = []
    with timer, async_timeout.timeout(10) as timeout:
        async for value in container.as_changed():
            values.append(value)
            break

    assert not timeout.expired
    assert timer.seconds <= 0.1
    assert values == [123]
