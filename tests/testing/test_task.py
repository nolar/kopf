import asyncio
import contextlib

import pytest

from kopf._cogs.aiokits import aioadapters
from kopf.testing import KopfTask


async def _mock_operator(**kwargs):
    """Simulate an operator: wait for stop flag, then exit."""
    stop_flag = kwargs.get('stop_flag')
    ready_flag = kwargs.get('ready_flag')
    if ready_flag is not None:
        await aioadapters.raise_flag(ready_flag)
    if stop_flag is not None:
        await aioadapters.wait_flag(stop_flag)


async def _mock_operator_that_fails(**kwargs):
    """Simulate an operator that fails immediately."""
    raise RuntimeError("operator crashed")


async def test_task_basic_lifecycle(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    async with KopfTask(clusterwide=True) as task:
        assert isinstance(task, KopfTask)
    # exited cleanly


async def test_task_stop_flag_injected_when_not_provided(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    async with KopfTask(clusterwide=True):
        pass
    assert isinstance(mock_op.call_args.kwargs['stop_flag'], asyncio.Event)
    assert mock_op.call_args.kwargs['stop_flag'].is_set()


async def test_task_user_provided_stop_flag_is_used(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    stop_flag = asyncio.Event()
    async with KopfTask(clusterwide=True, stop_flag=stop_flag):
        pass
    assert mock_op.call_args.kwargs['stop_flag'] is stop_flag
    assert stop_flag.is_set()


async def test_task_user_provided_ready_flag_is_passed_through(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    ready_flag = asyncio.Event()
    async with KopfTask(clusterwide=True, ready_flag=ready_flag):
        await ready_flag.wait()
        assert ready_flag.is_set()
    assert mock_op.call_args.kwargs['ready_flag'] is ready_flag


async def test_task_operator_kwargs_forwarded(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    async with KopfTask(clusterwide=True, standalone=True, priority=100):
        pass
    assert mock_op.call_args.kwargs['standalone'] is True
    assert mock_op.call_args.kwargs['clusterwide'] is True
    assert mock_op.call_args.kwargs['priority'] == 100


async def test_task_timeout_raises_when_operator_hangs(mocker):

    async def _mock_operator_that_hangs(**kwargs):
        await asyncio.sleep(5)

    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_hangs)
    with pytest.raises(Exception, match="The operator didn't stop"):
        async with KopfTask(clusterwide=True, timeout=3) as runner:
            pass

    # Cleanup after this artificially caused premature interruption.
    assert runner._task is not None
    runner._task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await runner._task


async def test_task_exception_propagated_with_reraise(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    with pytest.raises(RuntimeError, match="operator crashed"):
        async with KopfTask(clusterwide=True, reraise=True):
            pass


async def test_task_exception_suppressed_without_reraise(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    async with KopfTask(clusterwide=True, reraise=False):
        pass
    # no exception raised


async def test_task_exception_chaining_when_block_also_raises(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    with pytest.raises(RuntimeError, match="operator crashed") as exc_info:
        async with KopfTask(clusterwide=True, reraise=True):
            raise ValueError("block error")
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ValueError)
