"""The initial listing must be handled even with one worker slot (#1243).

At startup every object of a resource is listed at once, and the whole listing
has to be indexed before anything is processed. If the tasks that hold the
scheduler slots are the same ones that wait for that readiness, a
``worker_limit`` below the object count deadlocks: the started tasks wait, the
remaining objects never index, readiness never arrives, and nothing is handled.

The watcher is driven with its own real scheduler and workers; only the watch
stream is substituted, so the scheduling decisions under test are the real ones.
"""
import pytest

from kopf._cogs.clients import watching
from kopf._cogs.structs import bodies
from kopf._core.reactor.queueing import watcher

OBJECTS = 3  # more objects than the single slot below


@pytest.mark.usefixtures('settings')
async def test_initial_listing_is_handled_with_one_slot(
        monkeypatch, processor, settings, resource, registry):
    events = [
        {'type': 'ADDED', 'object': {'metadata': {'uid': f'uid{i}'}}} for i in range(OBJECTS)
    ]

    async def fake_watch(*, settings, resource, namespace, operator_paused=None):
        for event in events:
            yield event
        yield watching.Bookmark.LISTED

    monkeypatch.setattr(
        'kopf._core.reactor.queueing.watching.infinite_watch', fake_watch)

    settings.queueing.idle_timeout = 0.05
    settings.queueing.exit_timeout = 0.05
    settings.queueing.worker_limit = 1  # fewer slots than objects

    # Take the pre-indexing path without real indexers: this test is about slots
    # and gating, not about what an indexer computes.
    registry._indexing.has_handlers = lambda *a, **k: True

    handled: list[str] = []

    async def process(raw_event: bodies.RawEvent, **kwargs):
        handled.append(raw_event['object']['metadata']['uid'])
        return None

    processor.side_effect = process

    await watcher(
        namespace=None,
        resource=resource,
        settings=settings,
        registry=registry,
        processor=processor,
    )

    assert sorted(set(handled)) == [f'uid{i}' for i in range(OBJECTS)], (
        f"{sorted(set(handled))} were handled with 1 slot for {OBJECTS} objects: the "
        "initial listing must survive a worker limit below the object count (#1243)"
    )
