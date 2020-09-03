import aiohttp.web
import freezegun
import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, \
                                 NAMESPACED_PEERING_RESOURCE, Peer, apply_peers


@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('namespace, peering_resource', [
    pytest.param('ns', NAMESPACED_PEERING_RESOURCE, id='namespace-scoped'),
    pytest.param(None, CLUSTER_PEERING_RESOURCE, id='cluster-scoped'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_applying_a_dead_peer_purges_it(
        hostname, aresponses, resp_mocker, namespace, peering_resource):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(id='id1', name='...', namespace='ns1', lastseen='2020-01-01T00:00:00')
    await apply_peers(peers=[peer], name='name0', namespace=namespace)

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
async def test_applying_an_alive_peer_stores_it(
        hostname, aresponses, resp_mocker, namespace, peering_resource):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(id='id1', name='...', namespace='ns1', lastseen='2020-12-31T23:59:59')
    await apply_peers(peers=[peer], name='name0', namespace=namespace)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1']['namespace'] == 'ns1'
    assert patch['status']['id1']['priority'] == 0
    assert patch['status']['id1']['lastseen'] == '2020-12-31T23:59:59'
    assert patch['status']['id1']['lifetime'] == 60


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
async def test_keepalive(
        hostname, aresponses, resp_mocker, namespace, peering_resource, lastseen):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(id='id1', name='name0', namespace=namespace, lastseen=lastseen)
    await peer.keepalive()

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1']['namespace'] == namespace
    assert patch['status']['id1']['priority'] == 0
    assert patch['status']['id1']['lastseen'] == '2020-12-31T23:59:59.123456'
    assert patch['status']['id1']['lifetime'] == 60


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
async def test_disappear(
        hostname, aresponses, resp_mocker, namespace, peering_resource, lastseen):

    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(id='id1', name='name0', namespace=namespace, lastseen=lastseen)
    await peer.disappear()

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None
