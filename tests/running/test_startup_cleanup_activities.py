import asyncio
import contextlib

import pytest

import kopf
from kopf._cogs.aiokits import aiotasks
from kopf._cogs.structs.credentials import Vault
from kopf._cogs.structs.ephemera import AnyMemo, Memo
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.reactor.running import startup_cleanup_activities


async def test_startup_sets_flags(registry, settings, looptime):
    started_flag = asyncio.Event()
    ready_flag = asyncio.Event()
    root_tasks: list[aiotasks.Task] = []
    core_task = asyncio.create_task(asyncio.sleep(10), name="dummy_core")

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=[core_task],
        ready_flag=ready_flag,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))
    root_tasks.append(task)

    await started_flag.wait()
    assert ready_flag.is_set()

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert core_task.done()
    assert looptime == 0


async def test_cleanup_runs_only_after_root_tasks_exit(registry, settings, looptime):
    started_flag = asyncio.Event()
    root_tasks: list[aiotasks.Task] = []
    cleanup_result: dict[str, object] = {}
    core_task = asyncio.create_task(asyncio.sleep(10), name="dummy_core")

    @kopf.on.cleanup(registry=registry)
    async def cleanup_handler(**_):
        cleanup_result['called'] = True

    async def dummy_task():
        await asyncio.sleep(3)

    dummy = asyncio.create_task(dummy_task(), name="dummy_task")
    root_tasks.append(dummy)

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=[core_task],
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))
    root_tasks.append(task)

    await started_flag.wait()

    # Simulate operator shutdown: cancel the activities task (wakes it from sleep).
    task.cancel()
    await asyncio.sleep(0)

    # The activities task is now waiting for other root tasks to exit.
    # The dummy task sleeps for 3 seconds, then exits.
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert core_task.done()
    assert cleanup_result == {'called': True}
    assert looptime == 3


async def test_cancellation_during_startup(registry, settings, assert_logs, looptime):
    started_flag = asyncio.Event()
    core_task = asyncio.create_task(asyncio.sleep(10), name="dummy_core")

    @kopf.on.startup(registry=registry)
    async def blocking_startup(**_):
        await asyncio.sleep(10)

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=[],
        core_tasks=[core_task],
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))

    await asyncio.sleep(3)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert core_task.done()
    assert not started_flag.is_set()
    assert_logs(["Startup activity is only partially executed due to cancellation."])
    assert looptime == 3


async def test_cancellation_during_root_task_wait(registry, settings, assert_logs, looptime):
    started_flag = asyncio.Event()
    root_tasks: list[aiotasks.Task] = []
    core_task = asyncio.create_task(asyncio.sleep(10), name="dummy_core")

    async def dummy_task():
        await asyncio.sleep(10)

    dummy = asyncio.create_task(dummy_task(), name="dummy_task")
    root_tasks.append(dummy)

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=[core_task],
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))
    root_tasks.append(task)

    await started_flag.wait()

    # First cancel wakes from sleep; the task then waits for root tasks.
    task.cancel()
    await asyncio.sleep(0)

    # Wait a bit, then cancel again while it is waiting for the dummy task.
    await asyncio.sleep(3)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert core_task.done()
    assert_logs(["Cleanup activity is not executed at all due to cancellation."])
    assert looptime == 3

    dummy.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await dummy


async def test_cancellation_during_cleanup(registry, settings, assert_logs, looptime):
    started_flag = asyncio.Event()
    root_tasks: list[aiotasks.Task] = []
    core_task = asyncio.create_task(asyncio.sleep(10), name="dummy_core")

    @kopf.on.cleanup(registry=registry)
    async def blocking_cleanup(**_):
        await asyncio.sleep(10)

    async def dummy_task():
        await asyncio.sleep(3)

    dummy = asyncio.create_task(dummy_task(), name="dummy_task")
    root_tasks.append(dummy)

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=[core_task],
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))
    root_tasks.append(task)

    await started_flag.wait()

    # First cancel wakes from sleep; the task then waits for root tasks.
    task.cancel()
    await asyncio.sleep(0)

    # The dummy task finishes after 3 seconds, so the cleanup handler starts.
    # Wait a bit more, then cancel during the cleanup handler.
    await asyncio.sleep(6)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert core_task.done()
    assert_logs(["Cleanup activity is only partially executed due to cancellation."])
    assert looptime == 6


async def test_cancellation_during_core_task_stop(registry, settings, assert_logs, looptime):
    started_flag = asyncio.Event()
    root_tasks: list[aiotasks.Task] = []

    async def dummy_root_task():
        await asyncio.sleep(3)

    async def stubborn_core_task():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(10)

    dummy = asyncio.create_task(dummy_root_task(), name="dummy_root")
    root_tasks.append(dummy)

    core_tasks = [asyncio.create_task(stubborn_core_task(), name="stubborn_core")]

    task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=core_tasks,
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=AnyMemo(Memo()),
    ))
    root_tasks.append(task)

    await started_flag.wait()

    # First cancel wakes from sleep; the task then waits for root tasks.
    task.cancel()
    await asyncio.sleep(0)

    # The root task finishes after 3 seconds, then core task stopping begins.
    # Wait a bit more, then cancel during the core task stopping phase.
    await asyncio.sleep(5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert looptime == 5
    assert_logs(["Cleanup activity is not executed at all due to cancellation."])

    # Clean up the stubborn core task.
    core_tasks[0].cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await core_tasks[0]


async def test_core_tasks_are_stopped_after_root_tasks(settings, registry, looptime):
    timestamps: dict[str, float] = {}

    async def sample_task(duration: float, key: str) -> None:
        nonlocal timestamps
        try:
            await asyncio.sleep(duration)
        finally:
            timestamps[key] = float(looptime)

    root_tasks = [
        asyncio.create_task(sample_task(2, 'root1')),
        asyncio.create_task(sample_task(5, 'root2')),
    ]
    core_tasks = [
        asyncio.create_task(sample_task(9, 'core1')),
        asyncio.create_task(sample_task(9, 'core2')),
    ]

    started_flag = asyncio.Event()
    activities_task = asyncio.create_task(startup_cleanup_activities(
        root_tasks=root_tasks,
        core_tasks=core_tasks,
        ready_flag=None,
        started_flag=started_flag,
        registry=registry,
        settings=settings,
        indices=OperatorIndexers().indices,
        vault=Vault(),
        memo=Memo(),
    ))
    root_tasks.append(activities_task)

    # Simulate the behavior of the guarded task (briefly).
    await started_flag.wait()
    activities_task.cancel()  # interrupt its eternal sleep
    await activities_task

    assert timestamps['root1'] == 2
    assert timestamps['root2'] == 5
    assert timestamps['core1'] == 5  # the latest of root tasks, not 9
    assert timestamps['core2'] == 5  # the latest of root tasks, not 9
