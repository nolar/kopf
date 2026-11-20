import asyncio
import contextlib

import pytest

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.clients import watching
from kopf._cogs.structs import bodies
from kopf._cogs.structs.references import Insights, Resource
from kopf._core.engines.peering import Identity
from kopf._core.reactor import orchestration
from kopf._core.reactor.orchestration import Ensemble, EnsembleKey, adjust_tasks


async def processor(*, raw_event: bodies.RawEvent, stream_pressure: asyncio.Event | None) -> None:
    pass


@pytest.fixture(autouse=True)
def _auto_mocked(k8s_mocked):
    pass


@pytest.fixture()
async def insights(request, settings):
    insights = Insights()
    await insights.backbone.fill(resources=[
        request.getfixturevalue(name)
        for name in ['peering_resource', 'cluster_peering_resource', 'namespaced_peering_resource']
        if name in request.fixturenames
    ])
    settings.peering.mandatory = True
    return insights


@pytest.fixture()
async def ensemble(_no_asyncio_pending_tasks):
    operator_indexed = aiotoggles.ToggleSet(all)
    operator_paused = aiotoggles.ToggleSet(any)
    peering_missing = await operator_paused.make_toggle()
    ensemble = Ensemble(
        operator_indexed=operator_indexed,
        operator_paused=operator_paused,
        peering_missing=peering_missing,
    )

    try:
        yield ensemble
    finally:
        await aiotasks.stop(ensemble.get_tasks(ensemble.get_keys()), title='...')  # cleanup


async def test_empty_insights_cause_no_adjustments(
        settings, insights, ensemble):

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert not ensemble.watcher_tasks
    assert not ensemble.peering_tasks
    assert not ensemble.pinging_tasks
    assert not ensemble.conflicts_found
    assert not ensemble.peering_unavailable


async def test_new_resources_and_namespaces_spawn_new_tasks(
        settings, ensemble: Ensemble, insights: Insights, peering_resource):
    settings.peering.namespaced = peering_resource.namespaced

    r1 = Resource(group='group1', version='version1', plural='plural1', namespaced=True)
    r2 = Resource(group='group2', version='version2', plural='plural2', namespaced=True)
    insights.watched_resources.add(r1)
    insights.watched_resources.add(r2)
    insights.namespaces.add('ns1')
    insights.namespaces.add('ns2')
    r1ns1 = EnsembleKey(resource=r1, namespace='ns1')
    r1ns2 = EnsembleKey(resource=r1, namespace='ns2')
    r2ns1 = EnsembleKey(resource=r2, namespace='ns1')
    r2ns2 = EnsembleKey(resource=r2, namespace='ns2')
    peerns = peering_resource.namespaced
    peer1 = EnsembleKey(resource=peering_resource, namespace='ns1' if peerns else None)
    peer2 = EnsembleKey(resource=peering_resource, namespace='ns2' if peerns else None)

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert set(ensemble.watcher_tasks) == {r1ns1, r1ns2, r2ns1, r2ns2}
    assert set(ensemble.peering_tasks) == {peer1, peer2}
    assert set(ensemble.pinging_tasks) == {peer1, peer2}
    assert set(ensemble.conflicts_found) == {peer1, peer2}
    assert set(ensemble.peering_unavailable) == {peer1, peer2}
    assert all(toggle.is_on() for toggle in ensemble.conflicts_found.values())
    assert all(toggle.is_off() for toggle in ensemble.peering_unavailable.values())


async def test_gone_resources_and_namespaces_stop_running_tasks(
        settings, ensemble: Ensemble, insights: Insights, peering_resource):
    settings.peering.namespaced = peering_resource.namespaced

    r1 = Resource(group='group1', version='version1', plural='plural1', namespaced=True)
    r2 = Resource(group='group2', version='version2', plural='plural2', namespaced=True)
    insights.watched_resources.add(r1)
    insights.watched_resources.add(r2)
    insights.namespaces.add('ns1')
    insights.namespaces.add('ns2')
    r1ns1 = EnsembleKey(resource=r1, namespace='ns1')
    r1ns2 = EnsembleKey(resource=r1, namespace='ns2')
    r2ns1 = EnsembleKey(resource=r2, namespace='ns1')
    r2ns2 = EnsembleKey(resource=r2, namespace='ns2')
    peerns = peering_resource.namespaced
    peer1 = EnsembleKey(resource=peering_resource, namespace='ns1' if peerns else None)

    await adjust_tasks(  # initialisation
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble
    )

    r1ns2_task = ensemble.watcher_tasks[r1ns2]
    r2ns1_task = ensemble.watcher_tasks[r2ns1]
    r2ns2_task = ensemble.watcher_tasks[r2ns2]
    r2ns1_indexed = await ensemble.operator_indexed.make_toggle(name='r2ns1')
    ensemble.resource_indexed[r2ns1] = r2ns1_indexed

    insights.watched_resources.discard(r2)
    insights.namespaces.discard('ns2')

    await adjust_tasks(  # action-under-test
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert set(ensemble.watcher_tasks) == {r1ns1}
    assert set(ensemble.peering_tasks) == {peer1}
    assert set(ensemble.pinging_tasks) == {peer1}
    assert set(ensemble.conflicts_found) == {peer1}
    assert set(ensemble.peering_unavailable) == {peer1}
    assert r1ns2_task.cancelled()
    assert r2ns1_task.cancelled()
    assert r2ns2_task.cancelled()
    assert r2ns1_indexed not in ensemble.operator_indexed
    assert r2ns1 not in ensemble.resource_indexed


async def test_gone_resource_removes_taskless_indexing_toggle(
        settings, ensemble: Ensemble, insights: Insights):
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    resource_indexed = await ensemble.operator_indexed.make_toggle(name='resource')
    ensemble.resource_indexed[dkey] = resource_indexed

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert resource_indexed not in ensemble.operator_indexed
    assert dkey not in ensemble.resource_indexed


async def test_cluster_tasks_continue_running_on_namespace_deletion(
        settings, ensemble: Ensemble, insights: Insights, cluster_peering_resource):
    settings.peering.namespaced = cluster_peering_resource.namespaced

    r1 = Resource(group='group1', version='version1', plural='plural1', namespaced=True)
    r2 = Resource(group='group2', version='version2', plural='plural2', namespaced=True)
    insights.watched_resources.add(r1)
    insights.watched_resources.add(r2)
    insights.namespaces.add(None)
    r1nsN = EnsembleKey(resource=r1, namespace=None)
    r2nsN = EnsembleKey(resource=r2, namespace=None)
    peerN = EnsembleKey(resource=cluster_peering_resource, namespace=None)

    await adjust_tasks(  # initialisation
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble
    )

    r1nsN_task = ensemble.watcher_tasks[r1nsN]
    r2nsN_task = ensemble.watcher_tasks[r2nsN]

    insights.namespaces.discard(None)

    await adjust_tasks(  # action-under-test
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert set(ensemble.watcher_tasks) == {r1nsN, r2nsN}
    assert set(ensemble.peering_tasks) == {peerN}
    assert set(ensemble.pinging_tasks) == {peerN}
    assert set(ensemble.conflicts_found) == {peerN}
    assert set(ensemble.peering_unavailable) == {peerN}
    assert not r1nsN_task.cancelled()
    assert not r2nsN_task.cancelled()
    assert not r1nsN_task.done()
    assert not r2nsN_task.done()


async def test_no_peering_tasks_with_no_peering_resources(
        settings, ensemble):

    settings.peering.mandatory = False
    insights = Insights()
    r1 = Resource(group='group1', version='version1', plural='plural1', namespaced=True)
    insights.watched_resources.add(r1)
    insights.namespaces.add('ns1')

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert ensemble.watcher_tasks
    assert not ensemble.peering_tasks
    assert not ensemble.pinging_tasks
    assert not ensemble.conflicts_found
    assert not ensemble.peering_unavailable


async def test_paused_with_mandatory_peering_but_absent_peering_resource(
        settings, ensemble: Ensemble):

    settings.peering.mandatory = True
    insights = Insights()

    await ensemble.peering_missing.turn_to(False)  # prerequisite
    assert ensemble.peering_missing.is_off()  # prerequisite
    assert ensemble.operator_paused.is_off()  # prerequisite

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert ensemble.peering_missing.is_on()
    assert ensemble.operator_paused.is_on()


async def test_unpaused_with_mandatory_peering_and_existing_peering_resource(
        settings, ensemble: Ensemble, insights: Insights, peering_resource):
    settings.peering.namespaced = peering_resource.namespaced

    await ensemble.peering_missing.turn_to(True)  # prerequisite
    assert ensemble.peering_missing.is_on()  # prerequisite
    assert ensemble.operator_paused.is_on()  # prerequisite

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert ensemble.peering_missing.is_off()
    assert ensemble.operator_paused.is_off()


async def test_orchestrator_escalates_ensemble_task_failure(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')

    async def fail() -> None:
        raise RuntimeError("boom")

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        ensemble.watcher_tasks[dkey] = asyncio.create_task(fail())

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    async with insights.revised:
        insights.revised.notify_all()

    with pytest.raises(RuntimeError, match="boom"):
        await asyncio.wait_for(runner, timeout=1.23)


async def test_orchestrator_removes_finished_task_during_reconnect_backoff(
        monkeypatch, settings, insights: Insights, looptime):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    adjusted = asyncio.Event()
    captured_ensemble: Ensemble | None = None
    completed_task: asyncio.Task | None = None
    adjust_calls = 0
    settings.watching.reconnect_backoff = 10

    async def finish() -> None:
        pass

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble, completed_task, adjust_calls
        adjust_calls += 1
        captured_ensemble = ensemble
        if completed_task is None:
            completed_task = asyncio.create_task(finish())
            ensemble.watcher_tasks[dkey] = completed_task
        adjusted.set()

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await adjusted.wait()
        await asyncio.sleep(5)

        assert completed_task is not None
        assert completed_task.done()
        assert captured_ensemble is not None
        assert not captured_ensemble.watcher_tasks
        assert adjust_calls == 1
        assert not runner.done()
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def _finish() -> None:
    pass


async def _get_cancelled() -> None:
    raise asyncio.CancelledError()


@pytest.mark.parametrize('finish', [_finish, _get_cancelled], ids=['returned', 'cancelled'])
async def test_orchestrator_respawns_finished_ensemble_task(
        monkeypatch, settings, insights: Insights, finish, looptime):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    respawned = asyncio.Event()
    captured_ensemble: Ensemble | None = None
    first_task: asyncio.Task | None = None
    second_task: asyncio.Task | None = None
    settings.watching.reconnect_backoff = 10

    async def run_forever() -> None:
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble, first_task, second_task
        captured_ensemble = ensemble
        if dkey not in ensemble.watcher_tasks:
            if first_task is None:
                first_task = asyncio.create_task(finish())
                ensemble.watcher_tasks[dkey] = first_task
            else:
                second_task = asyncio.create_task(run_forever())
                ensemble.watcher_tasks[dkey] = second_task
                respawned.set()

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(respawned.wait(), timeout=12.3)

        assert first_task is not None
        assert first_task.done()
        assert second_task is not None
        assert not second_task.done()
        assert captured_ensemble is not None
        assert captured_ensemble.watcher_tasks.get(dkey) is second_task
        assert looptime == 10
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def test_orchestrator_reuses_indexed_watcher_toggle(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    insights.watched_resources.add(resource)
    insights.indexed_resources.add(resource)
    insights.namespaces.add('ns')
    settings.watching.reconnect_backoff = 0

    captured_ensemble: Ensemble | None = None
    indexed_toggles: list[aiotoggles.Toggle] = []
    respawned = asyncio.Event()
    original_adjust = orchestration.adjust_tasks

    async def watcher(*, resource_indexed: aiotoggles.Toggle, **kwargs) -> None:
        indexed_toggles.append(resource_indexed)
        if len(indexed_toggles) == 1:
            return
        respawned.set()
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble
        captured_ensemble = ensemble
        await original_adjust(ensemble=ensemble, **kwargs)

    monkeypatch.setattr(orchestration.queueing, 'watcher', watcher)
    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(respawned.wait(), timeout=1.23)

        assert captured_ensemble is not None
        assert len(indexed_toggles) == 2
        assert indexed_toggles[0] is indexed_toggles[1]
        assert indexed_toggles[0] in captured_ensemble.operator_indexed
        assert captured_ensemble.operator_indexed.is_off()
        assert captured_ensemble.watcher_tasks[dkey] is not None
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def test_orchestrator_reuses_dropped_indexed_watcher_toggle(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    insights.watched_resources.add(resource)
    insights.indexed_resources.add(resource)
    insights.namespaces.add('ns')
    settings.watching.reconnect_backoff = 0

    captured_ensemble: Ensemble | None = None
    indexed_toggles: list[aiotoggles.Toggle] = []
    respawned = asyncio.Event()
    original_adjust = orchestration.adjust_tasks

    async def watcher(
            *,
            operator_indexed: aiotoggles.ToggleSet,
            resource_indexed: aiotoggles.Toggle,
            **kwargs,
    ) -> None:
        indexed_toggles.append(resource_indexed)
        if len(indexed_toggles) == 1:
            await operator_indexed.drop_toggle(resource_indexed)
            return
        respawned.set()
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble
        captured_ensemble = ensemble
        await original_adjust(ensemble=ensemble, **kwargs)

    monkeypatch.setattr(orchestration.queueing, 'watcher', watcher)
    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(respawned.wait(), timeout=1.23)

        assert captured_ensemble is not None
        assert len(indexed_toggles) == 2
        assert indexed_toggles[0] is indexed_toggles[1]
        assert indexed_toggles[0] not in captured_ensemble.operator_indexed
        assert captured_ensemble.operator_indexed.is_on()
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def test_restarted_watcher_of_unindexed_resource_releases_readiness(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    insights.watched_resources.add(resource)
    insights.indexed_resources.add(resource)
    insights.namespaces.add('ns')
    settings.watching.reconnect_backoff = 0

    captured_ensemble: Ensemble | None = None
    blocking_toggle: aiotoggles.Toggle | None = None
    blocked_when_first_watcher_ended: bool | None = None
    watch_calls = 0
    second_watch_started = asyncio.Event()
    listing_completed = asyncio.Event()
    original_adjust = orchestration.adjust_tasks

    # The real watcher is used: only the watch stream is replaced, so that the toggle is released
    # by the framework itself at the end of the listing, not by the test.
    async def infinite_watch(**kwargs):
        nonlocal watch_calls, blocking_toggle, blocked_when_first_watcher_ended
        watch_calls += 1
        if watch_calls == 1:
            # The first watcher ends before its listing is completed,
            # and the resource stops being indexed in the meantime.
            assert captured_ensemble is not None
            blocking_toggle = captured_ensemble.resource_indexed[dkey]
            blocked_when_first_watcher_ended = captured_ensemble.operator_indexed.is_off()
            insights.indexed_resources.discard(resource)
            return
        second_watch_started.set()
        await listing_completed.wait()
        yield watching.Bookmark.LISTED
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble
        captured_ensemble = ensemble
        await original_adjust(ensemble=ensemble, **kwargs)

    monkeypatch.setattr(watching, 'infinite_watch', infinite_watch)
    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(second_watch_started.wait(), timeout=1.23)
        assert captured_ensemble is not None
        assert resource not in insights.indexed_resources
        assert blocked_when_first_watcher_ended is True  # the scenario is real, not a vacuous one

        listing_completed.set()
        await asyncio.wait_for(captured_ensemble.operator_indexed.wait_for(True), timeout=1.23)

        assert blocking_toggle is not None
        assert blocking_toggle not in captured_ensemble.operator_indexed
        assert captured_ensemble.operator_indexed.is_on()
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


@pytest.mark.parametrize('finished', ['observer', 'keepalive'])
@pytest.mark.parametrize('outcome', ['returned', 'cancelled'])
async def test_orchestrator_restarts_only_the_finished_peering_task(
        monkeypatch, settings, insights: Insights, peering_resource, finished, outcome, looptime):
    settings.peering.namespaced = peering_resource.namespaced
    settings.watching.reconnect_backoff = 10
    namespace = 'ns' if peering_resource.namespaced else None
    insights.namespaces.add('ns')
    dkey = EnsembleKey(resource=peering_resource, namespace=namespace)
    sibling = 'keepalive' if finished == 'observer' else 'observer'
    operator_paused = aiotoggles.ToggleSet(any)

    first_spawned = asyncio.Event()
    captured_ensemble: Ensemble | None = None
    first: dict[str, asyncio.Task] = {}
    first_conflict: aiotoggles.Toggle | None = None
    first_unavailable: aiotoggles.Toggle | None = None
    calls = {'observer': 0, 'keepalive': 0}
    spawned_tasks: set[asyncio.Task] = set()
    respawned = asyncio.Event()
    original_adjust = orchestration.adjust_tasks

    async def run(kind: str) -> None:
        calls[kind] += 1
        task = asyncio.current_task()
        assert task is not None
        spawned_tasks.add(task)
        if calls[finished] >= 2:
            respawned.set()
        if outcome == 'returned' and kind == finished and calls[kind] == 1:
            return
        await asyncio.Event().wait()

    async def watcher(**kwargs) -> None:
        await run('observer')

    async def keepalive(**kwargs) -> None:
        await run('keepalive')

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble, first_conflict, first_unavailable
        captured_ensemble = ensemble
        await original_adjust(ensemble=ensemble, **kwargs)
        if not first:
            first['observer'] = ensemble.peering_tasks[dkey]
            first['keepalive'] = ensemble.pinging_tasks[dkey]
            first_conflict = ensemble.conflicts_found[dkey]
            first_unavailable = ensemble.peering_unavailable[dkey]
            first_spawned.set()

    monkeypatch.setattr(orchestration.queueing, 'watcher', watcher)
    monkeypatch.setattr(orchestration.peering, 'keepalive', keepalive)
    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await first_spawned.wait()
        if outcome == 'cancelled':
            first[finished].cancel()
        await asyncio.wait_for(respawned.wait(), timeout=12.3)

        assert captured_ensemble is not None
        assert first_conflict is not None
        assert first_unavailable is not None
        current = {
            'observer': captured_ensemble.peering_tasks[dkey],
            'keepalive': captured_ensemble.pinging_tasks[dkey],
        }

        # The finished task is replaced, once, and only after the backoff.
        assert first[finished].done()
        assert current[finished] is not first[finished]
        assert not current[finished].done()
        assert calls[finished] == 2
        assert looptime == settings.watching.reconnect_backoff

        # Its sibling is neither stopped nor replaced.
        assert current[sibling] is first[sibling]
        assert not first[sibling].done()
        assert calls[sibling] == 1

        # The shared toggles survive, so the restart cannot unpause a paused operator.
        assert captured_ensemble.conflicts_found[dkey] is first_conflict
        assert captured_ensemble.peering_unavailable[dkey] is first_unavailable
        assert first_conflict in operator_paused
        assert first_unavailable in operator_paused
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        await aiotasks.stop(spawned_tasks, title='peering test cleanup', quiet=True)


async def test_orchestrator_keeps_the_peering_record_when_the_observer_restarts(
        monkeypatch, settings, insights: Insights, peering_resource, looptime):
    settings.peering.namespaced = peering_resource.namespaced
    settings.watching.reconnect_backoff = 10
    namespace = 'ns' if peering_resource.namespaced else None
    insights.namespaces.add('ns')
    dkey = EnsembleKey(resource=peering_resource, namespace=namespace)
    operator_paused = aiotoggles.ToggleSet(any)

    touches: list[dict] = []
    observer_calls = 0
    first_keepalive: asyncio.Task | None = None
    captured_ensemble: Ensemble | None = None
    respawned = asyncio.Event()
    original_adjust = orchestration.adjust_tasks

    # The real keep-alive is used. It stores the record with a regular touch, and it removes
    # the record (a touch with the lifetime of 0) whenever it is stopped or cancelled.
    async def touch(**kwargs) -> None:
        touches.append(kwargs)

    async def watcher(**kwargs) -> None:
        nonlocal observer_calls
        observer_calls += 1
        if observer_calls == 1:
            return
        respawned.set()
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal captured_ensemble, first_keepalive
        captured_ensemble = ensemble
        await original_adjust(ensemble=ensemble, **kwargs)
        if first_keepalive is None:
            first_keepalive = ensemble.pinging_tasks[dkey]

    monkeypatch.setattr(orchestration.peering, 'touch', touch)
    monkeypatch.setattr(orchestration.queueing, 'watcher', watcher)
    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(respawned.wait(), timeout=12.3)

        # While the observer has exited and restarted, the operator's record stays in the
        # peering object, so no other operator can take over for the time of the restart.
        assert looptime == settings.watching.reconnect_backoff
        assert first_keepalive is not None
        assert captured_ensemble is not None
        assert captured_ensemble.pinging_tasks[dkey] is first_keepalive
        assert not first_keepalive.done()
        assert [touch.get('lifetime') for touch in touches] == [None]  # stored, never removed
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner

    # The record is removed only when the keep-alive itself is stopped on the operator's exit.
    assert [touch.get('lifetime') for touch in touches] == [None, 0]


@pytest.mark.parametrize('missing', ['observer', 'keepalive'])
async def test_missing_peering_task_is_spawned_without_touching_its_sibling(
        settings, ensemble: Ensemble, insights: Insights, peering_resource, missing):
    settings.peering.namespaced = peering_resource.namespaced
    namespace = 'ns' if peering_resource.namespaced else None
    insights.namespaces.add('ns')
    dkey = EnsembleKey(resource=peering_resource, namespace=namespace)
    sibling = 'keepalive' if missing == 'observer' else 'observer'
    tasks = {'observer': ensemble.peering_tasks, 'keepalive': ensemble.pinging_tasks}

    # The first adjustment spawns the complete pair.
    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )
    first = {kind: task_map[dkey] for kind, task_map in tasks.items()}
    first_conflict = ensemble.conflicts_found[dkey]
    first_unavailable = ensemble.peering_unavailable[dkey]

    # One task of the pair is gone, as if it had exited and been recycled.
    gone = tasks[missing].pop(dkey)
    await aiotasks.stop({gone}, title='peering test cleanup', quiet=True)

    await adjust_tasks(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        ensemble=ensemble,
    )

    assert tasks[missing][dkey] is not first[missing]
    assert not tasks[missing][dkey].done()
    assert tasks[sibling][dkey] is first[sibling]
    assert not first[sibling].done()
    assert ensemble.conflicts_found[dkey] is first_conflict
    assert ensemble.peering_unavailable[dkey] is first_unavailable


@pytest.mark.parametrize('finished', ['observer', 'keepalive'])
async def test_recycle_terminated_peering_task_keeps_its_sibling(ensemble: Ensemble, finished):
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')

    async def finish() -> None:
        pass

    async def run_forever() -> None:
        await asyncio.Event().wait()

    observer = asyncio.create_task(finish() if finished == 'observer' else run_forever())
    keepalive = asyncio.create_task(finish() if finished == 'keepalive' else run_forever())
    ensemble.peering_tasks[dkey] = observer
    ensemble.pinging_tasks[dkey] = keepalive
    await asyncio.sleep(0)

    await orchestration.recycle_terminated_tasks(
        ensemble=ensemble,
        tasks={observer if finished == 'observer' else keepalive},
    )

    if finished == 'observer':
        assert dkey not in ensemble.peering_tasks
        assert ensemble.pinging_tasks[dkey] is keepalive
        assert not keepalive.done()
    else:
        assert dkey not in ensemble.pinging_tasks
        assert ensemble.peering_tasks[dkey] is observer
        assert not observer.done()


@pytest.mark.parametrize('finished', ['observer', 'keepalive'])
async def test_orchestrator_escalates_peering_sibling_failure_during_reconnect_backoff(
        monkeypatch, settings, insights: Insights, looptime, finished):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    spawned_tasks: set[asyncio.Task] = set()
    settings.watching.reconnect_backoff = 10

    async def finish() -> None:
        pass

    async def fail_later() -> None:
        await asyncio.sleep(5)
        raise RuntimeError("boom during backoff")

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        if not spawned_tasks:
            observer = asyncio.create_task(
                finish() if finished == 'observer' else fail_later())
            keepalive = asyncio.create_task(
                finish() if finished == 'keepalive' else fail_later())
            ensemble.peering_tasks[dkey] = observer
            ensemble.pinging_tasks[dkey] = keepalive
            spawned_tasks.update({observer, keepalive})

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()

        with pytest.raises(RuntimeError, match="boom during backoff"):
            await asyncio.wait_for(runner, timeout=123)
        assert looptime == 10
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError, RuntimeError):
            await runner
        await aiotasks.stop(spawned_tasks, title='peering test cleanup', quiet=True)


@pytest.mark.parametrize('finished, what, other', [
    ('observer', 'observer', 'keep-alive'),
    ('keepalive', 'keep-alive', 'observer'),
])
async def test_recycle_terminated_tasks_logs_peering_recovery(
        ensemble: Ensemble, assert_logs, finished, what, other):
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')

    async def finish() -> None:
        pass

    async def run_forever() -> None:
        await asyncio.Event().wait()

    observer = asyncio.create_task(finish() if finished == 'observer' else run_forever())
    keepalive = asyncio.create_task(finish() if finished == 'keepalive' else run_forever())
    ensemble.peering_tasks[dkey] = observer
    ensemble.pinging_tasks[dkey] = keepalive
    await asyncio.sleep(0)

    await orchestration.recycle_terminated_tasks(
        ensemble=ensemble,
        tasks={observer if finished == 'observer' else keepalive},
    )

    assert_logs(
        [fr"Peering {what} for {resource} in 'ns' ended; reconciling after backoff\."],
        prohibited=[fr"Peering {other} for {resource}"],
    )


async def test_recycle_terminated_tasks_logs_both_peering_tasks_if_both_finished(
        ensemble: Ensemble, assert_logs):
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')

    async def finish() -> None:
        pass

    observer = asyncio.create_task(finish())
    keepalive = asyncio.create_task(finish())
    ensemble.peering_tasks[dkey] = observer
    ensemble.pinging_tasks[dkey] = keepalive
    await asyncio.sleep(0)

    await orchestration.recycle_terminated_tasks(ensemble=ensemble, tasks={observer, keepalive})

    assert dkey not in ensemble.peering_tasks
    assert dkey not in ensemble.pinging_tasks
    assert_logs([
        fr"Peering observer for {resource} in 'ns' ended; reconciling after backoff\.",
        fr"Peering keep-alive for {resource} in 'ns' ended; reconciling after backoff\.",
    ])


@pytest.mark.parametrize('namespace, where', [
    ('ns', "in 'ns'"),
    (None, "cluster-wide"),
])
async def test_recycle_terminated_tasks_logs_watcher_recovery(
        ensemble: Ensemble, assert_logs, namespace, where):
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace=namespace)

    async def finish() -> None:
        pass

    watcher = asyncio.create_task(finish())
    ensemble.watcher_tasks[dkey] = watcher
    await asyncio.sleep(0)

    await orchestration.recycle_terminated_tasks(ensemble=ensemble, tasks={watcher})

    assert_logs([fr"Watcher for {resource} {where} ended; reconciling after backoff\."])
    assert dkey not in ensemble.watcher_tasks


async def test_orchestrator_releases_insights_during_reconnect_backoff(
        monkeypatch, settings, insights: Insights, looptime):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    respawned = asyncio.Event()
    adjust_calls = 0
    settings.watching.reconnect_backoff = 10

    async def finish() -> None:
        pass

    async def run_forever() -> None:
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal adjust_calls
        adjust_calls += 1
        if dkey not in ensemble.watcher_tasks:
            coro = finish() if adjust_calls == 1 else run_forever()
            ensemble.watcher_tasks[dkey] = asyncio.create_task(coro)
            if adjust_calls > 1:
                respawned.set()

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.sleep(5)

        # The insights are revisable during the backoff, but the backoff is not cut short.
        async with insights.revised:
            insights.revised.notify_all()
        assert looptime == 5
        await asyncio.wait_for(respawned.wait(), timeout=12.3)

        assert looptime == 10
        assert adjust_calls == 2
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def test_orchestrator_batches_tasks_ending_during_reconnect_backoff(
        monkeypatch, settings, insights: Insights, looptime):
    operator_paused = aiotoggles.ToggleSet(any)
    resource1 = Resource(group='group', version='version', plural='plural1', namespaced=True)
    resource2 = Resource(group='group', version='version', plural='plural2', namespaced=True)
    dkey1 = EnsembleKey(resource=resource1, namespace='ns')
    dkey2 = EnsembleKey(resource=resource2, namespace='ns')
    respawned = asyncio.Event()
    generations = {dkey1: 0, dkey2: 0}
    settings.watching.reconnect_backoff = 10

    async def finish() -> None:
        pass

    async def finish_later() -> None:
        await asyncio.sleep(5)

    async def run_forever() -> None:
        await asyncio.Event().wait()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        for dkey, initial in [(dkey1, finish), (dkey2, finish_later)]:
            if dkey not in ensemble.watcher_tasks:
                generations[dkey] += 1
                coro = initial() if generations[dkey] == 1 else run_forever()
                ensemble.watcher_tasks[dkey] = asyncio.create_task(coro)
        if all(generation == 2 for generation in generations.values()):
            respawned.set()

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(respawned.wait(), timeout=123)

        assert looptime == 10  # one backoff for both tasks, not one backoff per task
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


async def test_orchestrator_escalates_task_failure_during_reconnect_backoff(
        monkeypatch, settings, insights: Insights, looptime):
    operator_paused = aiotoggles.ToggleSet(any)
    resource1 = Resource(group='group', version='version', plural='plural1', namespaced=True)
    resource2 = Resource(group='group', version='version', plural='plural2', namespaced=True)
    dkey1 = EnsembleKey(resource=resource1, namespace='ns')
    dkey2 = EnsembleKey(resource=resource2, namespace='ns')
    settings.watching.reconnect_backoff = 10

    async def finish() -> None:
        pass

    async def fail_later() -> None:
        await asyncio.sleep(5)
        raise RuntimeError("boom during backoff")

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        if not ensemble.watcher_tasks:
            ensemble.watcher_tasks[dkey1] = asyncio.create_task(finish())
            ensemble.watcher_tasks[dkey2] = asyncio.create_task(fail_later())

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    async with insights.revised:
        insights.revised.notify_all()

    with pytest.raises(RuntimeError, match="boom during backoff"):
        await asyncio.wait_for(runner, timeout=123)
    assert looptime == 10


async def test_orchestrator_applies_insights_revised_between_iterations(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    adjusted_again = asyncio.Event()
    adjust_calls = 0
    revision_task: asyncio.Task | None = None

    async def revise() -> None:
        async with insights.revised:
            insights.revised.notify_all()

    async def adjust(**kwargs) -> None:
        nonlocal adjust_calls, revision_task
        adjust_calls += 1
        if adjust_calls == 1:
            # The revision queues on the lock while the orchestrator still holds it.
            revision_task = asyncio.create_task(revise())
            await asyncio.sleep(0)
        else:
            adjusted_again.set()

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    try:
        async with insights.revised:
            insights.revised.notify_all()
        await asyncio.wait_for(adjusted_again.wait(), timeout=1.23)
        assert adjust_calls == 2
    finally:
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        if revision_task is not None:
            await revision_task


async def test_orchestrator_cancellation_reaps_insights_waiter_and_stops_ensemble(
        monkeypatch, settings, insights: Insights):
    operator_paused = aiotoggles.ToggleSet(any)
    resource = Resource(group='group', version='version', plural='plural', namespaced=True)
    dkey = EnsembleKey(resource=resource, namespace='ns')
    started = asyncio.Event()
    stopped = asyncio.Event()
    ensemble_task: asyncio.Task | None = None

    async def run_forever() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def adjust(*, ensemble: Ensemble, **kwargs) -> None:
        nonlocal ensemble_task
        ensemble_task = asyncio.create_task(run_forever())
        ensemble.watcher_tasks[dkey] = ensemble_task

    monkeypatch.setattr(orchestration, 'adjust_tasks', adjust)
    runner = asyncio.create_task(orchestration.orchestrator(
        processor=processor,
        identity=Identity('...'),
        settings=settings,
        insights=insights,
        operator_paused=operator_paused,
    ))
    await asyncio.sleep(0)

    async with insights.revised:
        insights.revised.notify_all()
    await started.wait()
    for _ in range(10):
        if insights.revised._waiters:
            break
        await asyncio.sleep(0)
    assert insights.revised._waiters

    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner

    assert ensemble_task is not None
    assert ensemble_task.cancelled()
    assert stopped.is_set()
    assert not insights.revised._waiters
