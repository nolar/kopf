import aiohttp.web
import freezegun
import pytest

from kopf.engines.peering import Peer, clean, touch


@pytest.mark.parametrize('lastseen', [
    pytest.param('2020-01-01T00:00:00', id='when-dead'),
    pytest.param('2020-12-31T23:59:59', id='when-alive'),
])
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_cleaning_peers_purges_them(
        hostname, aresponses, resp_mocker, settings, lastseen,
        peering_resource, peering_namespace):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    peer = Peer(identity='id1', lastseen=lastseen)
    await clean(peers=[peer], resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_touching_a_peer_stores_it(
        hostname, aresponses, resp_mocker, settings,
        peering_resource, peering_namespace):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1']['priority'] == 0
    assert patch['status']['id1']['lastseen'] == '2020-12-31T23:59:59.123456'
    assert patch['status']['id1']['lifetime'] == 60


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_expiring_a_peer_purges_it(
        hostname, aresponses, resp_mocker, settings,
        peering_resource, peering_namespace):

    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace, lifetime=0)

    assert patch_mock.called
    patch = await patch_mock.call_args_list[0][0][0].json()
    assert set(patch['status']) == {'id1'}
    assert patch['status']['id1'] is None


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_logs_are_skipped_in_stealth_mode(
        hostname, aresponses, resp_mocker, settings, assert_logs, caplog,
        peering_resource, peering_namespace):

    caplog.set_level(0)
    settings.peering.stealth = True
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs([], prohibited=[
        "Keep-alive in",
    ])


async def test_logs_are_logged_in_exposed_mode(
        hostname, aresponses, resp_mocker, settings, assert_logs, caplog,
        peering_resource, peering_namespace):

    caplog.set_level(0)
    settings.peering.stealth = False
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aiohttp.web.json_response({}))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): ok",
    ])


@pytest.mark.parametrize('stealth', [True, False], ids=['stealth', 'exposed'])
async def test_logs_are_logged_when_absent(
        hostname, aresponses, resp_mocker, stealth, settings, assert_logs, caplog,
        peering_resource, peering_namespace):

    caplog.set_level(0)
    settings.peering.stealth = stealth
    settings.peering.name = 'name0'
    patch_mock = resp_mocker(return_value=aresponses.Response(status=404))
    url = peering_resource.get_url(name='name0', namespace=peering_namespace)
    aresponses.add(hostname, url, 'patch', patch_mock)

    await touch(identity='id1', resource=peering_resource, settings=settings,
                namespace=peering_namespace)

    assert_logs([
        r"Keep-alive in 'name0' (in 'ns'|cluster-wide): not found",
    ])
