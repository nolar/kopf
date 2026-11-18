"""The initial index population must not need a worker slot per object (#1243).

On startup every object of a resource gets its own worker, and each worker waits
for operator-wide readiness before it processes anything. Readiness is only
reached once every object has finished its initial indexing:
``process_resource_event`` (``core/reactor/processing.py``) drops its own toggle
after indexing, then awaits ``operator_indexed.wait_for(True)``.

With ``queueing.worker_limit`` below the number of objects, the workers that did
start hold every slot while they wait, so the remaining objects never index,
their toggles never drop, readiness never arrives, and nothing is handled at all
— the operator looks alive and does not react.

This drives the real ``worker`` coroutines, the real bounded ``Scheduler`` and the
real ``ToggleSet``, i.e. the machinery the watcher assembles per object.
"""
import asyncio
import contextlib
from typing import Any

import pytest

from kopf._cogs.aiokits import aiotasks
from kopf._cogs.aiokits.aiotoggles import ToggleSet
from kopf._cogs.configs.configuration import OperatorSettings
from kopf._core.reactor.queueing import Stream, worker

OBJECTS = 3  # more objects than the slots in the failing case


async def _run(limit: int, patience: float = 2.0) -> list[str]:
    """Run one worker per object under a scheduler with ``limit`` slots.

    Progress is polled rather than awaited to completion: under the defect the
    workers never finish, and closing the scheduler would surface a cancellation
    instead of the missing work.
    """
    settings = OperatorSettings()
    settings.queueing.idle_timeout = 0.05   # let an idle worker exit quickly
    settings.queueing.exit_timeout = 0.05

    # ``all`` is how the operator builds it (orchestration.py): an empty set is ready.
    operator_indexed = ToggleSet(all)
    streams: dict[Any, Stream] = {}
    handled: list[str] = []

    async def process_after_readiness(raw_event: Any, stream_pressure: Any,
                                      resource_indexed: Any, operator_indexed: Any,
                                      consistency_time: Any) -> None:
        """The shape of ``process_resource_event``: our object is indexed, so drop
        our toggle, then wait for every other object to do the same."""
        uid = raw_event['object']['metadata']['uid']
        if resource_indexed is not None:
            await operator_indexed.drop_toggle(resource_indexed)
        await operator_indexed.wait_for(True)
        handled.append(uid)
        return None

    scheduler = aiotasks.Scheduler(limit=limit)
    try:
        for i in range(OBJECTS):
            uid = f'uid{i}'
            key = ('resource', uid)
            streams[key] = Stream(backlog=asyncio.Queue(), pressure=asyncio.Event())
            streams[key].backlog.put_nowait({'type': 'ADDED', 'object': {'metadata': {'uid': uid}}})
            # What the watcher does for a new object: register its toggle, then spawn.
            resource_indexed = await operator_indexed.make_toggle(name=f'{key!r}')
            await scheduler.spawn(
                name=f'worker for {key}',
                coro=worker(
                    signaller=asyncio.Condition(),
                    settings=settings,
                    processor=process_after_readiness,
                    resource_indexed=resource_indexed,
                    operator_indexed=operator_indexed,
                    streams=streams,
                    key=key,
                ),
            )
        deadline = asyncio.get_running_loop().time() + patience
        while asyncio.get_running_loop().time() < deadline and len(handled) < OBJECTS:
            await asyncio.sleep(0.02)
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler.close()
    return handled


async def test_every_object_is_handled_when_there_are_enough_slots() -> None:
    """Sanity check for the harness itself: with enough slots, all objects run."""
    handled = await _run(limit=OBJECTS)
    assert sorted(handled) == [f'uid{i}' for i in range(OBJECTS)]


@pytest.mark.xfail(
    strict=True,
    reason="issue #1243: a worker waits for readiness while holding the only slot, "
           "so the other objects never index and readiness never arrives",
)
async def test_every_object_is_handled_when_slots_are_fewer_than_objects() -> None:
    """One slot, three objects: the operator must still handle all three."""
    handled = await _run(limit=1)
    assert sorted(handled) == [f'uid{i}' for i in range(OBJECTS)], (
        f"{len(handled)} of {OBJECTS} objects were handled with 1 worker slot: "
        "readiness needs every object indexed, but the started workers hold the "
        "slots while they wait, so the rest never index (#1243)"
    )
