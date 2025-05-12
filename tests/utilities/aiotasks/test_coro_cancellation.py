import asyncio
import gc
import warnings
from unittest.mock import AsyncMock, Mock

import pytest

from kopf._cogs.aiokits.aiotasks import cancel_coro


async def f(mock):
    return mock()


# Kwargs are accepted to match the signatures, but are unused/not passed through due to no need.
# Usually those are `name` & `context`, as in `asyncio.create_task(…)`.
def factory(loop, coro_or_mock, **_):
    coro = coro_or_mock._mock_wraps if isinstance(coro_or_mock, AsyncMock) else coro_or_mock
    return asyncio.Task(coro, loop=loop)


@pytest.fixture(autouse=True)
async def coromock_task_factory():
    factory_spy = Mock(wraps=factory)
    asyncio.get_running_loop().set_task_factory(factory_spy)
    yield factory_spy
    asyncio.get_running_loop().set_task_factory(None)


async def test_coro_issues_a_warning_normally(coromock_task_factory):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('default')
        mock = Mock()
        coro = f(mock)

        # The warnings come only from the garbage collection, so dereference it.
        del coro
        gc.collect()

    # The 1st coro is the test function itself; the 2nd coro would be the coro-under-test.
    assert coromock_task_factory.call_count == 1  # i.e. the task was NOT created.
    assert not mock.called
    assert len(w) == 1
    assert issubclass(w[0].category, RuntimeWarning)
    assert str(w[0].message) == "coroutine 'f' was never awaited"


async def test_coro_is_closed_via_a_hack_with_no_warning(coromock_task_factory):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('default')
        mock = Mock()
        coro = f(mock)
        await cancel_coro(coro)

        # The warnings come only from the garbage collection, so dereference it.
        del coro
        gc.collect()

    # The 1st coro is the test function itself; the 2nd coro would be the coro-under-test.
    assert coromock_task_factory.call_count == 1  # i.e. the task was NOT created.
    assert not mock.called
    assert not w


async def test_coro_is_awaited_via_a_task_with_no_warning(coromock_task_factory):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('default')
        mock = Mock()
        coro = AsyncMock(wraps=f(mock))
        del coro.close
        await cancel_coro(coro)

        # The warnings come only from the garbage collection, so dereference it.
        del coro
        gc.collect()

    # The 1st coro is the test function itself; the 2nd coro is the coro-under-test.
    assert coromock_task_factory.call_count == 2  # i.e. the task WAS created.
    assert not mock.called
    assert not w
