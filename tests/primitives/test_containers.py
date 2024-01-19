import asyncio

import pytest

from kopf._cogs.aiokits.aiovalues import Container


async def test_empty_by_default():
    container = Container()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(container.wait(), timeout=0.1)


async def test_does_not_wake_up_when_reset(timer):
    container = Container()

    async def reset_it():
        await container.reset()

    loop = asyncio.get_running_loop()
    loop.call_later(0.05, asyncio.create_task, reset_it())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(container.wait(), timeout=0.1)


async def test_wakes_up_when_preset(timer):
    container = Container()
    await container.set(123)

    with timer:
        result = await container.wait()

    assert timer.seconds <= 0.1
    assert result == 123


async def test_wakes_up_when_set(timer):
    container = Container()

    async def set_it():
        await container.set(123)

    loop = asyncio.get_running_loop()
    loop.call_later(0.1, asyncio.create_task, set_it())

    with timer:
        result = await container.wait()

    assert 0.1 <= timer.seconds <= 0.2
    assert result == 123


async def test_iterates_when_set(timer):
    container = Container()

    async def set_it(v):
        await container.set(v)

    loop = asyncio.get_running_loop()
    loop.call_later(0.1, asyncio.create_task, set_it(123))
    loop.call_later(0.2, asyncio.create_task, set_it(234))

    values = []
    with timer:
        async for value in container.as_changed():
            values.append(value)
            if value == 234:
                break

    assert 0.2 <= timer.seconds <= 0.3
    assert values == [123, 234]


async def test_iterates_when_preset(timer):
    container = Container()
    await container.set(123)

    values = []
    with timer:
        async for value in container.as_changed():
            values.append(value)
            break

    assert timer.seconds <= 0.1
    assert values == [123]
