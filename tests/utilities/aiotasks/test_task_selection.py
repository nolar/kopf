import asyncio

from kopf.utilities.aiotasks import all_tasks, create_task


async def test_alltasks_exclusion():
    flag = asyncio.Event()
    task1 = create_task(flag.wait())
    task2 = create_task(flag.wait())
    done, pending = await asyncio.wait([task1, task2], timeout=0.01)
    assert not done

    tasks = await all_tasks(ignored=[task2])
    assert task1 in tasks
    assert task2 not in tasks
    assert asyncio.current_task() not in tasks

    flag.set()
    await task1
    await task2
