import asyncio

import pytest

from kopf._cogs.aiokits.aiovalues import Container


async def test_empty_by_default(looptime):
    container = Container()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(container.wait(), timeout=9)
    assert looptime == 9


async def test_does_not_wake_up_when_reset(looptime):
    container = Container()

    async def reset_it():
        await container.reset()

    loop = asyncio.get_running_loop()
    loop.call_later(1, asyncio.create_task, reset_it())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(container.wait(), timeout=9)
    assert looptime == 9


async def test_wakes_up_when_preset(looptime):
    container = Container()
    await container.set(123)

    result = await container.wait()
    assert looptime == 0
    assert result == 123


async def test_wakes_up_when_set(looptime):
    container = Container()

    async def set_it():
        await container.set(123)

    loop = asyncio.get_running_loop()
    loop.call_later(9, asyncio.create_task, set_it())

    result = await container.wait()
    assert looptime == 9
    assert result == 123


async def test_iterates_when_set(looptime):
    container = Container()

    async def set_it(v):
        await container.set(v)

    loop = asyncio.get_running_loop()
    loop.call_later(6, asyncio.create_task, set_it(123))
    loop.call_later(9, asyncio.create_task, set_it(234))

    values = []
    async for value in container.as_changed():
        values.append(value)
        if value == 234:
            break

    assert looptime == 9
    assert values == [123, 234]


async def test_iterates_when_preset(looptime):
    container = Container()
    await container.set(123)

    values = []
    async for value in container.as_changed():
        values.append(value)
        break

    assert looptime == 0
    assert values == [123]
