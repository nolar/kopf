"""
The daemon killer stops every daemon, even those exiting mid-iteration.

Each ``Scheduler.spawn()`` of a stopper gives control to the event loop.
If the stoppers spawned earlier finish meanwhile, their daemons exit
and remove themselves from the resource's running daemons --- the same dict
that the daemon killer iterates over. Here, a delay after every spawn
ensures that they do so before the next spawn.
"""
import asyncio

import pytest

import kopf
from kopf._cogs.aiokits import aiotasks


@pytest.fixture(autouse=True)
def slow_spawning(mocker):
    original = aiotasks.Scheduler.spawn

    async def spawn(self, *args, **kwargs):
        await original(self, *args, **kwargs)
        await asyncio.sleep(1)  # let the spawned stopper and its daemon finish

    mocker.patch.object(aiotasks.Scheduler, 'spawn', spawn)


@pytest.fixture()
async def daemons_memory(settings, memories, resource, dummy, simulate_cycle):

    @kopf.daemon(*resource, id='fn1')
    async def fn1(**kwargs):
        dummy.mock(**kwargs)
        await kwargs['stopped'].wait()

    @kopf.daemon(*resource, id='fn2')
    async def fn2(**kwargs):
        dummy.mock(**kwargs)
        await kwargs['stopped'].wait()

    # Spawn both daemons and wait until they run. Assume the finalizers are already added.
    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    while dummy.mock.call_count < 2:
        await asyncio.sleep(0)
    memory = await memories.recall(event_object)
    assert len(memory.daemons_memory.running_daemons) == 2
    return memory.daemons_memory


async def test_all_daemons_exit_on_operator_pausing_while_being_iterated_over(
        daemons_memory, conflicts_found, background_daemon_killer):
    await conflicts_found.turn_to(True)

    # The killer keeps running while the operator does; it would be done only if it failed.
    done, _ = await asyncio.wait([background_daemon_killer], timeout=10)
    assert not done
    assert not daemons_memory.running_daemons


async def test_all_daemons_exit_on_operator_exiting_while_being_iterated_over(
        daemons_memory, background_daemon_killer):
    background_daemon_killer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await background_daemon_killer
    assert not daemons_memory.running_daemons
