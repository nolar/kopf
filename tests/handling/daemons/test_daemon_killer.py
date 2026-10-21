"""
The daemon killer iterates the running daemons while their stoppers remove them.

Every ``await scheduler.spawn(...)`` yields to the event loop, and a stopper
spawned a moment earlier can finish meanwhile: the daemon exits and its runner
deletes it from ``running_daemons``. The killer must not fail with
``RuntimeError: dictionary changed size during iteration`` when that happens,
neither when the operator is paused nor when it is exiting.
"""
import asyncio

import pytest

from kopf._core.engines import daemons
from kopf._core.engines.daemons import daemon_killer


@pytest.fixture()
async def running_daemons(memories, mocker):
    memory = await memories.recall({'metadata': {'uid': 'uid1', 'name': 'n', 'namespace': 'ns'}})
    running = memory.daemons_memory.running_daemons
    running.update({f'handler{i}': mocker.Mock(name=f'daemon{i}') for i in range(4)})

    # As a daemon's runner does once its daemon has stopped: it forgets it.
    async def stop_daemon(*, daemon, **_):
        for key, value in list(running.items()):
            if value is daemon:
                del running[key]

    mocker.patch.object(daemons, 'stop_daemon', stop_daemon)
    return running


async def test_all_daemons_are_stopped_when_paused_while_stoppers_remove_them(
        settings, memories, operator_paused, running_daemons, looptime):
    await operator_paused.make_toggle(True, name='paused')
    task = asyncio.create_task(daemon_killer(
        settings=settings, memories=memories, operator_paused=operator_paused))
    while running_daemons and not task.done():
        await asyncio.sleep(0)
    if task.done():
        await task  # re-raise the killer's failure, if any
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert running_daemons == {}


async def test_all_daemons_are_stopped_when_exiting_while_stoppers_remove_them(
        settings, memories, operator_paused, running_daemons, looptime):
    task = asyncio.create_task(daemon_killer(
        settings=settings, memories=memories, operator_paused=operator_paused))
    await asyncio.sleep(0)  # let it reach its wait for a pause
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert running_daemons == {}
