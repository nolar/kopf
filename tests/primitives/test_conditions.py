import asyncio

import async_timeout
import pytest

from kopf.structs.primitives import condition_chain


async def test_no_triggering():
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:

        with pytest.raises(asyncio.TimeoutError):
            with async_timeout.timeout(0.1) as timeout:
                async with target:
                    await target.wait()

        assert timeout.expired

    finally:
        task.cancel()
        await asyncio.wait([task])


async def test_triggering(event_loop, timer):
    source = asyncio.Condition()
    target = asyncio.Condition()
    task = asyncio.create_task(condition_chain(source, target))
    try:

        async def delayed_trigger():
            async with source:
                source.notify_all()

        event_loop.call_later(0.1, asyncio.create_task, delayed_trigger())

        with timer, async_timeout.timeout(10) as timeout:
            async with target:
                await target.wait()

        assert not timeout.expired
        assert 0.1 <= timer.seconds <= 0.2

    finally:
        task.cancel()
        await asyncio.wait([task])
