import asyncio

import pytest

from kopf._cogs.aiokits import aiotasks, aiotoggles
from kopf._cogs.structs import bodies
from kopf._cogs.structs.references import Insights, Resource
from kopf._core.engines.peering import Identity
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
    assert r1ns2_task.cancelled()
    assert r2ns1_task.cancelled()
    assert r2ns2_task.cancelled()


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
