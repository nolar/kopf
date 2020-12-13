import asyncio
import logging

import async_timeout
import pytest

from kopf.utilities.aiotasks import create_task, stop


async def simple() -> None:
    await asyncio.Event().wait()


async def stuck() -> None:
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await asyncio.Event().wait()


async def test_stop_with_no_tasks(assert_logs, caplog):
    logger = logging.getLogger()
    caplog.set_level(0)
    done, pending = await stop([], title='sample', logger=logger)
    assert not done
    assert not pending
    assert_logs(["Sample tasks stopping is skipped: no tasks given."])


async def test_stop_with_no_tasks_when_quiet(assert_logs, caplog):
    logger = logging.getLogger()
    caplog.set_level(0)
    done, pending = await stop([], title='sample', logger=logger, quiet=True)
    assert not done
    assert not pending
    assert not caplog.messages


async def test_stop_immediately_with_finishing(assert_logs, caplog):
    logger = logging.getLogger()
    caplog.set_level(0)
    task1 = create_task(simple())
    task2 = create_task(simple())
    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await stop([task1, task2], title='sample', logger=logger, cancelled=False)
    assert done
    assert not pending
    assert_logs(["Sample tasks are stopped: finishing normally"])
    assert task1.cancelled()
    assert task2.cancelled()


async def test_stop_immediately_with_cancelling(assert_logs, caplog):
    logger = logging.getLogger()
    caplog.set_level(0)
    task1 = create_task(simple())
    task2 = create_task(simple())
    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await stop([task1, task2], title='sample', logger=logger, cancelled=True)
    assert done
    assert not pending
    assert_logs(["Sample tasks are stopped: cancelling normally"])
    assert task1.cancelled()
    assert task2.cancelled()


@pytest.mark.parametrize('cancelled', [False, True])
async def test_stop_iteratively(assert_logs, caplog, cancelled):
    logger = logging.getLogger()
    caplog.set_level(0)
    task1 = create_task(simple())
    task2 = create_task(stuck())
    stask = create_task(stop([task1, task2], title='sample', logger=logger, interval=0.01, cancelled=cancelled))

    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await asyncio.wait({stask}, timeout=0.011)
    assert not done
    assert task1.done()
    assert not task2.done()

    task2.cancel()

    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await asyncio.wait({stask}, timeout=0.011)
    assert done
    assert task1.done()
    assert task2.done()

    assert_logs([
        r"Sample tasks are not stopped: (finishing|cancelling) normally; tasks left: \{<Task",
        r"Sample tasks are stopped: (finishing|cancelling) normally; tasks left: set\(\)",
    ])


@pytest.mark.parametrize('cancelled', [False, True])
async def test_stop_itself_is_cancelled(assert_logs, caplog, cancelled):
    logger = logging.getLogger()
    caplog.set_level(0)
    task1 = create_task(simple())
    task2 = create_task(stuck())
    stask = create_task(stop([task1, task2], title='sample', logger=logger, interval=0.01, cancelled=cancelled))

    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await asyncio.wait({stask}, timeout=0.011)
    assert not done
    assert task1.done()
    assert not task2.done()

    stask.cancel()

    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await asyncio.wait({stask}, timeout=0.011)
    assert done
    assert task1.done()
    assert not task2.done()

    assert_logs([
        r"Sample tasks are not stopped: (finishing|cancelling) normally; tasks left: \{<Task",
        r"Sample tasks are not stopped: (cancelling|double-cancelling) at stopping; tasks left: \{<Task",
    ], prohibited=[
        r"Sample tasks are stopped",
    ])

    task2.cancel()
    async with async_timeout.timeout(1):  # extra test safety
        done, pending = await asyncio.wait({task1, task2})
    assert done
    assert task1.done()
    assert task2.done()
