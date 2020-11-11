import asyncio

from kopf.utilities.aiotasks import create_task, wait


async def test_wait_with_no_tasks():
    done, pending = await wait([])
    assert not done
    assert not pending


async def test_wait_with_timeout():
    flag = asyncio.Event()
    task = create_task(flag.wait())
    done, pending = await wait([task], timeout=0.01)
    assert not done
    assert pending == {task}
    flag.set()
    await task
