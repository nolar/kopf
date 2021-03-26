import asyncio
import time
import unittest.mock

import freezegun
import pytest

import kopf
from kopf.reactor.daemons import daemon_killer
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.processing import process_resource_event
from kopf.structs.bodies import RawBody
from kopf.structs.containers import ResourceMemories
from kopf.structs.ephemera import Memo
from kopf.structs.primitives import ToggleSet


class DaemonDummy:

    def __init__(self):
        super().__init__()
        self.mock = unittest.mock.MagicMock()
        self.kwargs = {}
        self.steps = {
            'called': asyncio.Event(),
            'finish': asyncio.Event(),
            'error': asyncio.Event(),
        }

    async def wait_for_daemon_done(self):
        stopped = self.kwargs['stopped']
        await stopped.wait()
        while not stopped._stopper.reason & stopped._stopper.reason.DONE:
            await asyncio.sleep(0)  # give control back to asyncio event loop


@pytest.fixture()
def dummy():
    return DaemonDummy()


@pytest.fixture()
def simulate_cycle(k8s_mocked, registry, settings, resource, memories, mocker):
    """
    Simulate K8s behaviour locally in memory (some meaningful approximation).
    """

    def _merge_dicts(src, dst):
        for key, val in src.items():
            if isinstance(val, dict) and key in dst:
                _merge_dicts(src[key], dst[key])
            else:
                dst[key] = val

    async def _simulate_cycle(event_object: RawBody):
        mocker.resetall()

        await process_resource_event(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            settings=settings,
            resource=resource,
            memories=memories,
            memobase=Memo(),
            indexers=OperatorIndexers(),
            raw_event={'type': 'irrelevant', 'object': event_object},
            event_queue=asyncio.Queue(),
        )

        # Do the same as k8s does: merge the patches into the object.
        for call in k8s_mocked.patch_obj.call_args_list:
            _merge_dicts(call[1]['patch'], event_object)

    return _simulate_cycle


@pytest.fixture()
async def operator_paused():
    return ToggleSet(any)


@pytest.fixture()
async def conflicts_found(operator_paused: ToggleSet):
    return await operator_paused.make_toggle(name="conflicts_found fixture")


@pytest.fixture()
async def background_daemon_killer(settings, memories, operator_paused):
    """
    Run the daemon killer in the background.
    """
    task = asyncio.create_task(daemon_killer(
        settings=settings, memories=memories, operator_paused=operator_paused))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.fixture()
def frozen_time():
    """
    A helper to simulate time movements to step over long sleeps/timeouts.
    """
    # TODO LATER: Either freezegun should support the system clock, or find something else.
    with freezegun.freeze_time("2020-01-01 00:00:00") as frozen:
        # Use freezegun-supported time instead of system clocks -- for testing purposes only.
        # NB: Patch strictly after the time is frozen -- to use fake_time(), not real time().
        with unittest.mock.patch('time.monotonic', time.time), \
             unittest.mock.patch('time.perf_counter', time.time):
            yield frozen


# The time-driven tests mock the sleeps, and shift the time as much as it was requested to sleep.
# This makes the sleep realistic for the app code, though executed instantly for the tests.
@pytest.fixture()
def manual_time(k8s_mocked, frozen_time):
    async def sleep_or_wait_substitute(delay, *_, **__):
        if delay is None:
            pass
        elif isinstance(delay, float):
            frozen_time.tick(delay)
        else:
            frozen_time.tick(min(delay))

    k8s_mocked.sleep_or_wait.side_effect = sleep_or_wait_substitute
    yield frozen_time

