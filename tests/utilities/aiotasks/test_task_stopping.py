import asyncio
import logging

import pytest

from kopf._cogs.aiokits.aiotasks import stop


async def simple() -> None:
    await asyncio.Event().wait()


async def stuck() -> None:
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await asyncio.Event().wait()


async def test_stop_with_no_tasks(assert_logs, looptime):
    logger = logging.getLogger()
    done, pending = await stop([], title='sample', logger=logger)
    assert not done
    assert not pending
    assert_logs(["Sample tasks stopping is skipped: no tasks given."])
    assert looptime == 0


async def test_stop_with_no_tasks_when_quiet(assert_logs, looptime):
    logger = logging.getLogger()
    done, pending = await stop([], title='sample', logger=logger, quiet=True)
    assert not done
    assert not pending
    assert_logs(prohibited=['.*'])
    assert looptime == 0


async def test_stop_immediately_with_finishing(assert_logs, looptime):
    logger = logging.getLogger()
    task1 = asyncio.create_task(simple())
    task2 = asyncio.create_task(simple())
    done, pending = await stop([task1, task2], title='sample', logger=logger, cancelled=False)
    assert done
    assert not pending
    assert_logs(["Sample tasks are stopped: finishing normally"])
    assert task1.cancelled()
    assert task2.cancelled()
    assert looptime == 0


async def test_stop_immediately_with_cancelling(assert_logs, looptime):
    logger = logging.getLogger()
    task1 = asyncio.create_task(simple())
    task2 = asyncio.create_task(simple())
    done, pending = await stop([task1, task2], title='sample', logger=logger, cancelled=True)
    assert done
    assert not pending
    assert_logs(["Sample tasks are stopped: cancelling normally"])
    assert task1.cancelled()
    assert task2.cancelled()
    assert looptime == 0


@pytest.mark.parametrize('cancelled', [False, True])
async def test_stop_iteratively(assert_logs, cancelled, looptime):
    logger = logging.getLogger()
    task1 = asyncio.create_task(simple())
    task2 = asyncio.create_task(stuck())
    stask = asyncio.create_task(stop([task1, task2], title='sample', logger=logger, interval=1, cancelled=cancelled))
    assert looptime == 0

    done, pending = await asyncio.wait({stask}, timeout=10)
    assert not done
    assert task1.done()
    assert not task2.done()
    assert looptime == 10

    task2.cancel()

    done, pending = await asyncio.wait({stask}, timeout=10)
    assert done
    assert task1.done()
    assert task2.done()
    assert looptime == 10  # not 20!

    assert_logs([
        r"Sample tasks are not stopped: (finishing|cancelling) normally; tasks left: \{<Task",
        r"Sample tasks are stopped: (finishing|cancelling) normally; tasks left: set\(\)",
    ])


@pytest.mark.parametrize('cancelled', [False, True])
async def test_stop_itself_is_cancelled(assert_logs, cancelled, looptime):
    logger = logging.getLogger()
    task1 = asyncio.create_task(simple())
    task2 = asyncio.create_task(stuck())
    stask = asyncio.create_task(stop([task1, task2], title='sample', logger=logger, interval=1, cancelled=cancelled))
    assert looptime == 0

    done, pending = await asyncio.wait({stask}, timeout=10)
    assert not done
    assert task1.done()
    assert not task2.done()
    assert looptime == 10

    stask.cancel()

    done, pending = await asyncio.wait({stask}, timeout=10)
    assert done
    assert task1.done()
    assert not task2.done()
    assert looptime == 10  # not 20!

    assert_logs([
        r"Sample tasks are not stopped: (finishing|cancelling) normally; tasks left: \{<Task",
        r"Sample tasks are not stopped: (cancelling|double-cancelling) at stopping; tasks left: \{<Task",
    ], prohibited=[
        r"Sample tasks are stopped",
    ])

    task2.cancel()
    done, pending = await asyncio.wait({task1, task2}, timeout=99)
    assert done
    assert task1.done()
    assert task2.done()
    assert looptime == 10  # not 100+!
