import asyncio

from kopf._cogs.aiokits.aiotasks import all_tasks


async def test_alltasks_exclusion():
    flag = asyncio.Event()
    task1 = asyncio.create_task(flag.wait())
    task2 = asyncio.create_task(flag.wait())
    done, pending = await asyncio.wait([task1, task2], timeout=0.01)  # let them start
    assert not done

    tasks = await all_tasks(ignored=[task2])
    assert task1 in tasks
    assert task2 not in tasks
    assert asyncio.current_task() not in tasks

    flag.set()
    await task1
    await task2
