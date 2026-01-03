import asyncio

import pytest

from kopf._cogs.aiokits.aiobindings import condition_chain


async def test_no_triggering(looptime):
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:
        with pytest.raises(asyncio.TimeoutError):
            async with target:
                await asyncio.wait_for(target.wait(), timeout=1.23)
        assert looptime == 1.23
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_triggering(looptime):
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:

        async def delayed_trigger():
            async with source:
                source.notify_all()

        loop = asyncio.get_running_loop()
        loop.call_later(1.23, asyncio.create_task, delayed_trigger())

        async with target:
            await target.wait()

        assert looptime == 1.23

    finally:
        task.cancel()
        await asyncio.wait([task])
