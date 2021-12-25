import asyncio

from kopf._cogs.aiokits.aiotasks import wait


async def test_wait_with_no_tasks(looptime):
    done, pending = await wait([])
    assert not done
    assert not pending
    assert looptime == 0


async def test_wait_with_timeout(looptime):
    flag = asyncio.Event()
    task = asyncio.create_task(flag.wait())
    done, pending = await wait([task], timeout=1.23)
    assert not done
    assert pending == {task}
    assert looptime == 1.23
    flag.set()
    await task
