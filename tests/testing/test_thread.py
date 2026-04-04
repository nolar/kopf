import asyncio
import threading

import pytest

from kopf._cogs.aiokits import aioadapters
from kopf.testing import KopfThread

# KopfThread runs in a real background thread with a real event loop.
# The _target() cleanup includes asyncio.sleep(1.0) for aiohttp transport shutdown,
# which is real wall-clock time. The default CI timeout is 2s per test, so we need
# a longer timeout here to account for the cleanup overhead.
pytestmark = pytest.mark.timeout(10)


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


def test_thread_basic_lifecycle(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    with KopfThread(clusterwide=True) as thread:
        assert isinstance(thread, KopfThread)
    # exited cleanly


def test_thread_stop_flag_injected_when_not_provided(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    with KopfThread(clusterwide=True):
        pass
    assert isinstance(mock_op.call_args.kwargs['stop_flag'], threading.Event)
    assert mock_op.call_args.kwargs['stop_flag'].is_set()


def test_thread_user_provided_stop_flag_is_used(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    stop_flag = threading.Event()
    with KopfThread(clusterwide=True, stop_flag=stop_flag):
        pass
    assert mock_op.call_args.kwargs['stop_flag'] is stop_flag
    assert stop_flag.is_set()


def test_thread_user_provided_ready_flag_is_passed_through(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    ready_flag = threading.Event()
    with KopfThread(clusterwide=True, ready_flag=ready_flag):
        ready_flag.wait(timeout=5)
        assert ready_flag.is_set()
    assert mock_op.call_args.kwargs['ready_flag'] is ready_flag


def test_thread_operator_kwargs_forwarded(mocker):
    mock_op = mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator)
    with KopfThread(clusterwide=True, standalone=True, priority=100):
        pass
    assert mock_op.call_args.kwargs['standalone'] is True
    assert mock_op.call_args.kwargs['clusterwide'] is True
    assert mock_op.call_args.kwargs['priority'] == 100


def test_thread_timeout_raises_when_operator_hangs(mocker):

    async def _mock_operator_that_hangs(**kwargs):
        await asyncio.sleep(1)

    # The timeout path abandons the background thread's event loop with a pending task.
    # Use new= instead of side_effect= to avoid AsyncMock adding its own extra coroutine.
    mocker.patch('kopf._core.reactor.running.operator', new=_mock_operator_that_hangs)
    with pytest.raises(Exception, match="The operator didn't stop"):
        with KopfThread(clusterwide=True, timeout=0.5) as runner:
            pass

    # Cleanup after this artificially caused premature interruption.
    assert runner._thread is not None
    runner._thread.join(timeout=3)


def test_thread_exception_propagated_with_reraise(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    with pytest.raises(RuntimeError, match="operator crashed"):
        with KopfThread(clusterwide=True, reraise=True):
            pass


def test_thread_exception_suppressed_without_reraise(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    with KopfThread(clusterwide=True, reraise=False):
        pass
    # no exception raised


def test_thread_exception_chaining_when_block_also_raises(mocker):
    mocker.patch('kopf._core.reactor.running.operator', side_effect=_mock_operator_that_fails)
    with pytest.raises(RuntimeError, match="operator crashed") as exc_info:
        with KopfThread(clusterwide=True, reraise=True):
            raise ValueError("block error")
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ValueError)
