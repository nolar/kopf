import freezegun
import pytest

from kopf._core.engines.peering import Peer, clean, touch


@pytest.mark.parametrize('lastseen', [
    pytest.param('2020-01-01T00:00:00', id='when-dead'),
    pytest.param('2020-12-31T23:59:59', id='when-alive'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_cleaning_peers_purges_them(
        kmock, settings, lastseen, peering_resource, peering_namespace):
    settings.peering.name = 'name0'
    kmock.objects[peering_resource, peering_namespace, 'name0'] = {}

    peer = Peer(identity='id1', lastseen=lastseen)
    await clean(peers=[peer], resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert len(kmock) == 1
    assert set(kmock[0].data['status']) == {'id1'}
    assert kmock[0].data['status']['id1'] is None
    assert kmock.objects[peering_resource, peering_namespace, 'name0'] == {'status': {}}


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_touching_a_peer_stores_it(kmock, settings, peering_resource, peering_namespace):
    settings.peering.name = 'name0'
    kmock.objects[peering_resource, peering_namespace, 'name0'] = {}

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert len(kmock) > 0
    patch = kmock[0].data
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1']['priority'] == 0
    assert patch['status']['id1']['lastseen'] == '2020-12-31T23:59:59.123456+00:00'
    assert patch['status']['id1']['lifetime'] == 60
    assert kmock.objects[peering_resource, peering_namespace, 'name0'] == {'status': {'id1': {'lastseen': ..., 'lifetime': 60, 'priority': 0}}}


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_expiring_a_peer_purges_it(kmock, settings, peering_resource, peering_namespace):
    settings.peering.name = 'name0'
    kmock.objects[peering_resource, peering_namespace, 'name0'] = {}

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace, lifetime=0)

    assert len(kmock) > 0
    patch = kmock[0].data
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None
    assert kmock.objects[peering_resource, peering_namespace, 'name0'] == {'status': {}}


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_logs_are_skipped_in_stealth_mode(
        kmock, settings, assert_logs, peering_resource, peering_namespace):

    settings.peering.stealth = True
    settings.peering.name = 'name0'
    kmock.objects[peering_resource, peering_namespace, 'name0'] = {}

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs(prohibited=[
        "Keep-alive in",
    ])


async def test_logs_are_logged_in_exposed_mode(
        kmock, settings, assert_logs, peering_resource, peering_namespace):

    settings.peering.stealth = False
    settings.peering.name = 'name0'
    kmock.objects[peering_resource, peering_namespace, 'name0'] = {}

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): ok",
    ])


@pytest.mark.parametrize('stealth', [True, False], ids=['stealth', 'exposed'])
async def test_logs_are_logged_when_absent(
        stealth, settings, assert_logs, peering_resource, peering_namespace):

    settings.peering.stealth = stealth
    settings.peering.name = 'name0'

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): not found",
    ])
