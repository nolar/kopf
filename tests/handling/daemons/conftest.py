import asyncio
import contextlib
from unittest.mock import MagicMock

import looptime
import pytest

import kopf
from kopf._cogs.aiokits.aiotoggles import ToggleSet
from kopf._cogs.structs.bodies import RawBody
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.daemons import daemon_killer
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.reactor.processing import process_resource_event


class DaemonDummy:

    def __init__(self):
        super().__init__()
        self.mock = MagicMock()

    async def wait_for_daemon_done(self) -> None:
        stopped = self.mock.call_args[1]['stopped']
        await stopped.wait()
        while stopped.reason is None or not stopped.reason & stopped.reason.DONE:
            await asyncio.sleep(0)  # give control back to asyncio event loop


@pytest.fixture()
async def dummy(simulate_cycle):
    dummy = DaemonDummy()
    yield dummy

    # Cancel the background tasks, if any.
    event_object = {'metadata': {'deletionTimestamp': '...'}}
    with looptime.enabled(strict=True):
        await simulate_cycle(event_object)
        await dummy.wait_for_daemon_done()


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

    async def _simulate_cycle(
            event_object: RawBody,
            *,
            stream_pressure: asyncio.Event | None = None,
    ) -> None:
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
            stream_pressure=stream_pressure,
        )

        # Do the same as k8s does: merge the patches into the object.
        for call in k8s_mocked.patch.call_args_list:
            _merge_dicts(call[1]['payload'], event_object)

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
    yield task

    with contextlib.suppress(asyncio.CancelledError):
        task.cancel()
        await task
