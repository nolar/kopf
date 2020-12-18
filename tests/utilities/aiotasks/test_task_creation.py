import asyncio

import pytest

from kopf.utilities.aiotasks import create_task


async def sample() -> None:
    pass


@pytest.mark.skipif('sys.version_info < (3, 8)')
def test_py38_create_task_is_the_native_one():
    assert create_task is asyncio.create_task


@pytest.mark.skipif('sys.version_info >= (3, 8)')
async def test_py37_create_task_accepts_name(mocker):
    real_create_task = mocker.patch('asyncio.create_task')
    coro = sample()
    task = create_task(coro, name='unused')
    assert real_create_task.called
    assert task is real_create_task.return_value
    await coro  # to prevent "never awaited" errors
