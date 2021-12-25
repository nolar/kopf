import asyncio
from unittest.mock import Mock

import pytest

from kopf._cogs.aiokits.aiotasks import Scheduler


async def f(mock, *args):
    try:
        mock('started')
        for arg in args:
            if isinstance(arg, asyncio.Event):
                arg.set()
            elif isinstance(arg, (int, float)):
                await asyncio.sleep(arg)
            elif callable(arg):
                arg()
    except asyncio.CancelledError:
        mock('cancelled')
    else:
        mock('finished')


async def test_empty_scheduler_lifecycle(looptime):
    scheduler = Scheduler()
    assert scheduler.empty()
    await scheduler.wait()
    assert scheduler.empty()
    await scheduler.close()
    assert scheduler.empty()
    assert looptime == 0


async def test_task_spawning_and_graceful_finishing(looptime):
    mock = Mock()
    flag1 = asyncio.Event()
    flag2 = asyncio.Event()
    scheduler = Scheduler()

    result = await scheduler.spawn(f(mock, flag1, 123, flag2))
    assert result is None

    await flag1.wait()
    assert looptime == 0
    assert mock.call_args_list[0][0][0] == 'started'

    await flag2.wait()
    assert looptime == 123
    assert mock.call_args_list[1][0][0] == 'finished'

    await scheduler.close()


async def test_task_spawning_and_cancellation(looptime):
    mock = Mock()
    flag1 = asyncio.Event()
    flag2 = asyncio.Event()
    scheduler = Scheduler()

    result = await scheduler.spawn(f(mock, flag1, 123, flag2))
    assert result is None

    await flag1.wait()
    assert looptime == 0
    assert mock.call_args_list[0][0][0] == 'started'

    await scheduler.close()
    assert looptime == 0
    assert mock.call_args_list[1][0][0] == 'cancelled'


async def test_no_tasks_are_accepted_after_closing():
    scheduler = Scheduler()
    await scheduler.close()
    assert scheduler._closed
    assert scheduler._spawning_task.done()
    assert scheduler._cleaning_task.done()
    with pytest.raises(RuntimeError, match=r"Cannot add new coroutines"):
        await scheduler.spawn(f(Mock(), 123))


async def test_successes_are_not_reported():
    exception_handler = Mock()
    scheduler = Scheduler(exception_handler=exception_handler)
    await scheduler.spawn(f(Mock()))
    await scheduler.wait()
    await scheduler.close()
    assert exception_handler.call_count == 0


async def test_cancellations_are_not_reported():
    exception_handler = Mock()
    mock = Mock(side_effect=asyncio.CancelledError())
    scheduler = Scheduler(exception_handler=exception_handler)
    await scheduler.spawn(f(mock, 1))
    await scheduler.wait()
    await scheduler.close()
    assert exception_handler.call_count == 0


async def test_exceptions_are_reported():
    exception = ValueError('hello')
    exception_handler = Mock()
    mock = Mock(side_effect=exception)
    scheduler = Scheduler(exception_handler=exception_handler)
    await scheduler.spawn(f(mock))
    await scheduler.wait()
    await scheduler.close()
    assert exception_handler.call_count == 1
    assert exception_handler.call_args[0][0] is exception


async def test_tasks_are_parallel_if_limit_is_not_reached(looptime):
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

    await scheduler.spawn(f(Mock(), task1_started, 9, task1_finished))
    await scheduler.spawn(f(Mock(), task2_started, 9, task2_finished))
    assert looptime == 0  # i.e. spawning is not not blocking

    await task1_finished.wait()
    assert task2_started.is_set()
    await task2_finished.wait()
    assert looptime == 9

    await scheduler.close()


async def test_tasks_are_pending_if_limit_is_reached(looptime):
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

    await scheduler.spawn(f(Mock(), task1_started, 9, task1_finished))
    await scheduler.spawn(f(Mock(), task2_started, 9, task2_finished))
    assert looptime == 0  # i.e. spawning is not not blocking

    await task1_finished.wait()
    assert not task2_started.is_set()
    await task2_finished.wait()
    assert looptime == 18

    await scheduler.close()
