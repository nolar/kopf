import asyncio
import logging

import pytest

from kopf.utilities.aiotasks import create_guarded_task, create_task, reraise


class Error(Exception):
    pass


async def fail(msg: str) -> None:
    raise Error(msg)


async def delay(n: float) -> None:
    await asyncio.sleep(n)


async def sample() -> None:
    pass


async def test_guard_logs_on_exit(assert_logs, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    await create_guarded_task(sample(), name='this task', logger=logger)
    assert_logs(["This task has finished unexpectedly"])


async def test_guard_logs_on_failure(assert_logs, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    task = create_guarded_task(coro=fail("boo!"), name='this task', logger=logger)
    await asyncio.wait([task], timeout=0.01)  # let it start & react
    assert_logs(["This task has failed: boo!"])


async def test_guard_logs_on_cancellation(assert_logs, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    task = create_guarded_task(coro=delay(1), name='this task', logger=logger)
    await asyncio.wait([task], timeout=0.01)  # let it start
    task.cancel()
    await asyncio.wait([task], timeout=0.01)  # let it react
    assert_logs(["This task is cancelled"])


async def test_guard_is_silent_when_finishable(assert_logs, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    await create_guarded_task(sample(), name='this task', logger=logger, finishable=True)
    assert_logs([], prohibited=["This task has finished unexpectedly"])
    assert not caplog.messages


async def test_guard_is_silent_when_cancellable(assert_logs, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    task = create_guarded_task(coro=delay(1), name='this task', logger=logger, cancellable=True)
    await asyncio.wait([task], timeout=0.01)  # let it start
    task.cancel()
    await asyncio.wait([task], timeout=0.01)  # let it react
    assert_logs([], prohibited=["This task is cancelled"])
    assert not caplog.messages


@pytest.mark.parametrize('finishable', [True, False])
async def test_guard_escalates_on_failure(finishable):
    task = create_guarded_task(coro=fail("boo!"), name='this task', finishable=finishable)
    await asyncio.wait([task], timeout=0.01)  # let it start & react
    with pytest.raises(Error):
        await task


@pytest.mark.parametrize('cancellable', [True, False])
async def test_guard_escalates_on_cancellation(cancellable):
    task = create_guarded_task(coro=delay(1), name='this task', cancellable=cancellable)
    await asyncio.wait([task], timeout=0.01)  # let it start
    task.cancel()
    await asyncio.wait([task], timeout=0.01)  # let it react
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_guard_waits_for_the_flag():
    flag = asyncio.Event()

    task = create_guarded_task(coro=sample(), name='this task', flag=flag)
    await asyncio.wait([task], timeout=0.01)  # let it start
    assert not task.done()

    flag.set()
    await asyncio.wait([task], timeout=0.01)  # let it react
    assert task.done()


async def test_reraise_escalates_errors():
    task = create_task(fail("boo!"))
    await asyncio.wait([task], timeout=0.01)  # let it start & react
    with pytest.raises(Error):
        await reraise([task])


async def test_reraise_skips_cancellations():
    task = create_task(asyncio.Event().wait())
    done, pending = await asyncio.wait([task], timeout=0.01)  # let it start
    assert not done
    task.cancel()
    done, pending = await asyncio.wait([task], timeout=0.01)  # let it react
    assert done
    await reraise([task])
