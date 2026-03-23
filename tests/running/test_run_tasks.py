import asyncio

import pytest

from kopf._cogs.aiokits import aiotasks
from kopf._core.reactor.running import run_tasks


async def test_root_task_exit_cancels_remaining(assert_logs, looptime):
    async def finishes_soon():
        await asyncio.sleep(3)

    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(finishes_soon(), name="finishes_soon")
    task_b = asyncio.create_task(runs_forever(), name="runs_forever")
    await run_tasks([task_a, task_b], ignored=ignored)

    assert task_a.done()
    assert task_b.done()
    assert task_b.cancelled()
    assert looptime == 3
    assert_logs([
        r"Root tasks are stopped: finishing normally",
        r"Hung tasks stopping is skipped: no tasks given",
    ])


async def test_root_task_exception_is_reraised(assert_logs, looptime):
    async def fails_soon():
        await asyncio.sleep(3)
        raise RuntimeError("boom")

    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(fails_soon(), name="fails_soon")
    task_b = asyncio.create_task(runs_forever(), name="runs_forever")
    with pytest.raises(RuntimeError, match="boom"):
        await run_tasks([task_a, task_b], ignored=ignored)

    assert task_a.done()
    assert task_b.done()
    assert looptime == 3
    assert_logs([
        r"Root tasks are stopped: finishing normally",
    ])


async def test_cancellation_during_root_wait(assert_logs, looptime):
    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(runs_forever(), name="root_a")
    task_b = asyncio.create_task(runs_forever(), name="root_b")
    runner = asyncio.create_task(run_tasks([task_a, task_b], ignored=ignored))

    await asyncio.sleep(3)
    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner

    assert task_a.done()
    assert task_b.done()
    assert looptime == 3
    assert_logs([
        r"Root tasks are stopped: cancelling normally",
        r"Hung tasks stopping is skipped: no tasks given",
    ])


async def test_hung_subtask_cancelled_after_grace_period(assert_logs, looptime):
    hung_task: aiotasks.Task | None = None

    async def spawns_and_exits():
        nonlocal hung_task
        hung_task = asyncio.create_task(asyncio.sleep(10), name="hung_subtask")
        await asyncio.sleep(3)

    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(spawns_and_exits(), name="spawns_and_exits")
    task_b = asyncio.create_task(runs_forever(), name="runs_forever")
    await run_tasks([task_a, task_b], ignored=ignored)

    assert hung_task is not None
    assert hung_task.done()
    assert hung_task.cancelled()
    # 3s for root exit + 5s grace period for hung task timeout.
    assert looptime == 8
    assert_logs([
        r"Root tasks are stopped: finishing normally",
        r"Hung tasks are stopped: finishing normally",
    ])


async def test_hung_subtask_finishes_within_grace_period(assert_logs, looptime):
    hung_task: aiotasks.Task | None = None

    async def spawns_and_exits():
        nonlocal hung_task
        hung_task = asyncio.create_task(asyncio.sleep(2), name="hung_subtask")

    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(spawns_and_exits(), name="spawns_and_exits")
    task_b = asyncio.create_task(runs_forever(), name="runs_forever")
    await run_tasks([task_a, task_b], ignored=ignored)

    assert hung_task is not None
    assert hung_task.done()
    assert not hung_task.cancelled()
    # 0s for root exit + 2s for hung task finishing within grace period.
    assert looptime == 2
    assert_logs([
        r"Root tasks are stopped: finishing normally",
        r"Hung tasks stopping is skipped: no tasks given",
    ])


async def test_cancellation_during_hung_task_wait(assert_logs, looptime):
    hung_task: aiotasks.Task | None = None

    async def spawns_and_exits():
        nonlocal hung_task
        hung_task = asyncio.create_task(asyncio.sleep(10), name="hung_subtask")
        await asyncio.sleep(3)

    async def runs_forever():
        await asyncio.sleep(10)

    ignored = asyncio.all_tasks()
    task_a = asyncio.create_task(spawns_and_exits(), name="spawns_and_exits")
    task_b = asyncio.create_task(runs_forever(), name="runs_forever")
    runner = asyncio.create_task(run_tasks([task_a, task_b], ignored=ignored))

    # 3s for root exit, then cancel during the 5s hung-task grace period.
    await asyncio.sleep(5)
    runner.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runner

    assert hung_task is not None
    assert hung_task.done()
    assert looptime == 5
    assert_logs([
        r"Root tasks are stopped: finishing normally",
        r"Hung tasks are stopped: cancelling normally",
    ])
