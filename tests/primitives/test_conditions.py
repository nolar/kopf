import asyncio

import pytest

from kopf._cogs.aiokits.aiobindings import condition_chain


async def test_no_triggering():
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:
        with pytest.raises(asyncio.TimeoutError):
            async with target:
                await asyncio.wait_for(target.wait(), timeout=0.1)
    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_triggering(timer):
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:

        async def delayed_trigger():
            async with source:
                source.notify_all()

        loop = asyncio.get_running_loop()
        loop.call_later(0.1, asyncio.create_task, delayed_trigger())

        with timer:
            async with target:
                await target.wait()

        assert 0.1 <= timer.seconds <= 0.2

    finally:
        task.cancel()
        await asyncio.wait([task])
