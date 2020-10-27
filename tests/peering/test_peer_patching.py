import aiohttp.web
import freezegun
import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, \
                                 NAMESPACED_PEERING_RESOURCE, Peer, clean, touch


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
@pytest.mark.parametrize('lastseen', [
    pytest.param('2020-01-01T00:00:00', id='when-dead'),
    pytest.param('2020-12-31T23:59:59', id='when-alive'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_cleaning_peers_purges_them(
        hostname, aresponses, resp_mocker, namespace, peering_resource, settings, lastseen):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(identity='id1', lastseen=lastseen)
    await clean(peers=[peer], settings=settings, namespace=namespace)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_touching_a_peer_stores_it(
        hostname, aresponses, resp_mocker, namespace, peering_resource, settings):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', settings=settings, namespace=namespace)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1']['priority'] == 0
    assert patch['status']['id1']['lastseen'] == '2020-12-31T23:59:59.123456'
    assert patch['status']['id1']['lifetime'] == 60


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_expiring_a_peer_purges_it(
        hostname, aresponses, resp_mocker, namespace, peering_resource, settings):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', settings=settings, namespace=namespace, lifetime=0)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_logs_are_skipped_in_stealth_mode(
        hostname, aresponses, resp_mocker, namespace, peering_resource, settings,
        assert_logs, caplog):

    caplog.set_level(0)
    settings.peering.stealth = True
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', settings=settings, namespace=namespace)

    assert_logs([], prohibited=[
        "Keep-alive in",
    ])


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
async def test_logs_are_logged_in_exposed_mode(
        hostname, aresponses, resp_mocker, namespace, peering_resource, settings,
        assert_logs, caplog):

    caplog.set_level(0)
    settings.peering.stealth = False
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', settings=settings, namespace=namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): ok",
    ])


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('stealth', [True, False], ids=['stealth', 'exposed'])
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
async def test_logs_are_logged_when_absent(
        hostname, aresponses, resp_mocker, namespace, peering_resource, stealth, settings,
        assert_logs, caplog):

    caplog.set_level(0)
    settings.peering.stealth = stealth
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aresponses.Response(status=404))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', settings=settings, namespace=namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): not found",
    ])
