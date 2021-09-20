import asyncio
from unittest.mock import Mock

import async_timeout
import pytest

from kopf._cogs.aiokits.aiotasks import Scheduler

CODE_OVERHEAD = 0.01


async def f(mock, *args):
    try:
        mock('started')
        for arg in args:
            if isinstance(arg, asyncio.Event):
                arg.set()
            elif isinstance(arg, float):
                await asyncio.sleep(arg)
            elif callable(arg):
                arg()
    except asyncio.CancelledError:
        mock('cancelled')
    else:
        mock('finished')


async def test_empty_scheduler_lifecycle(timer):
    with timer, async_timeout.timeout(1):
        scheduler = Scheduler()
        assert scheduler.empty()
        await scheduler.wait()
        assert scheduler.empty()
        await scheduler.close()
        assert scheduler.empty()
    assert timer.seconds < CODE_OVERHEAD


async def test_task_spawning_and_graceful_finishing(timer):
    mock = Mock()
    flag1 = asyncio.Event()
    flag2 = asyncio.Event()
    scheduler = Scheduler()

    result = await scheduler.spawn(f(mock, flag1, 0.1, flag2))
    assert result is None

    with timer, async_timeout.timeout(1):
        await flag1.wait()
    assert timer.seconds < CODE_OVERHEAD
    assert mock.call_args[0][0] == 'started'

    with timer, async_timeout.timeout(1):
        await flag2.wait()
    assert timer.seconds > 0.1
    assert timer.seconds < 0.1 + CODE_OVERHEAD
    assert mock.call_args[0][0] == 'finished'

    await scheduler.close()


async def test_task_spawning_and_cancellation(timer):
    mock = Mock()
    flag1 = asyncio.Event()
    flag2 = asyncio.Event()
    scheduler = Scheduler()

    result = await scheduler.spawn(f(mock, flag1, 1.0, flag2))
    assert result is None

    with timer, async_timeout.timeout(1):
        await flag1.wait()
    assert timer.seconds < CODE_OVERHEAD
    assert mock.call_args[0][0] == 'started'

    with timer, async_timeout.timeout(1):
        await scheduler.close()
    assert timer.seconds < CODE_OVERHEAD  # near-instant
    assert mock.call_args[0][0] == 'cancelled'


async def test_no_tasks_are_accepted_after_closing():
    scheduler = Scheduler()
    await scheduler.close()

    assert scheduler._closed
    assert scheduler._spawning_task.done()
    assert scheduler._cleaning_task.done()

    with async_timeout.timeout(1):
        with pytest.raises(RuntimeError, match=r"Cannot add new coroutines"):
            await scheduler.spawn(f(Mock(), 1.0))


async def test_successes_are_not_reported():
    exception_handler = Mock()
    scheduler = Scheduler(exception_handler=exception_handler)
    with async_timeout.timeout(1):
        await scheduler.spawn(f(Mock()))
        await scheduler.wait()
        await scheduler.close()
    assert exception_handler.call_count == 0


async def test_cancellations_are_not_reported():
    exception_handler = Mock()
    mock = Mock(side_effect=asyncio.CancelledError())
    scheduler = Scheduler(exception_handler=exception_handler)
    with async_timeout.timeout(1):
        await scheduler.spawn(f(mock, 1))
        await scheduler.wait()
        await scheduler.close()
    assert exception_handler.call_count == 0


async def test_exceptions_are_reported():
    exception = ValueError('hello')
    exception_handler = Mock()
    mock = Mock(side_effect=exception)
    scheduler = Scheduler(exception_handler=exception_handler)
    with async_timeout.timeout(1):
        await scheduler.spawn(f(mock))
        await scheduler.wait()
        await scheduler.close()
    assert exception_handler.call_count == 1
    assert exception_handler.call_args[0][0] is exception


async def test_tasks_are_parallel_if_limit_is_not_reached(timer):
    """
    time:  ////////----------------------0.1s------------------0.2s--///
    task1: ->spawn->start->sleep->finish->|
    task2: ->spawn->start->sleep->finish->|
    """
    task1_started = asyncio.Event()
    task1_finished = asyncio.Event()
    task2_started = asyncio.Event()
    task2_finished = asyncio.Event()
    scheduler = Scheduler(limit=2)

    with timer, async_timeout.timeout(1):
        await scheduler.spawn(f(Mock(), task1_started, 0.1, task1_finished))
        await scheduler.spawn(f(Mock(), task2_started, 0.1, task2_finished))
    assert timer.seconds < CODE_OVERHEAD  # i.e. spawning is not not blocking

    with timer, async_timeout.timeout(1):
        await task1_finished.wait()
        assert task2_started.is_set()
        await task2_finished.wait()

    assert timer.seconds > 0.1
    assert timer.seconds < 0.1 + CODE_OVERHEAD

    await scheduler.close()


async def test_tasks_are_pending_if_limit_is_reached(timer):
    """
    time:  ////////----------------------0.1s------------------0.2s--///
    task1: ->spawn->start->sleep->finish->|
    task2: ->spawn->.....(pending)......->start->sleep->finish->|
    """
    task1_started = asyncio.Event()
    task1_finished = asyncio.Event()
    task2_started = asyncio.Event()
    task2_finished = asyncio.Event()
    scheduler = Scheduler(limit=1)

    with timer, async_timeout.timeout(1):
        await scheduler.spawn(f(Mock(), task1_started, 0.1, task1_finished))
        await scheduler.spawn(f(Mock(), task2_started, 0.1, task2_finished))
    assert timer.seconds < CODE_OVERHEAD  # i.e. spawning is not not blocking

    with timer, async_timeout.timeout(1):
        await task1_finished.wait()
        assert not task2_started.is_set()
        await task2_finished.wait()

    assert timer.seconds > 0.2
    assert timer.seconds < 0.2 + CODE_OVERHEAD * 2

    await scheduler.close()
