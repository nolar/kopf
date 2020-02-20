import aiohttp.web
import pytest

from kopf.clients.discovery import discover, is_namespaced, is_status_subresource
from kopf.structs.resources import Resource


async def test_discovery_of_existing_resource(
        resp_mocker, aresponses, hostname):

    res1info = {'name': 'someresources', 'namespaced': True}
    result = {'resources': [res1info]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    info = await discover(resource=resource)

    assert info == res1info


async def test_discovery_of_unexisting_resource(
        resp_mocker, aresponses, hostname):

    result = {'resources': []}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    info = await discover(resource=resource)

    assert info is None


@pytest.mark.parametrize('status', [403, 404])
async def test_discovery_of_unexisting_group_or_version(
        resp_mocker, aresponses, hostname, status):

    list_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    info = await discover(resource=resource)

    assert info is None


async def test_discovery_is_cached_per_session(
        resp_mocker, aresponses, hostname):

    res1info = {'name': 'someresources1', 'namespaced': True}
    res2info = {'name': 'someresources2', 'namespaced': True}

    result = {'resources': [res1info]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    result = {'resources': [res2info]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources1')
    info = await discover(resource=resource)
    assert info == res1info

    resource = Resource('some-group.org', 'someversion', 'someresources2')
    info = await discover(resource=resource)
    assert info is None  # cached as absent on the 1st call.

    resource = Resource('some-group.org', 'someversion', 'someresources1')
    info = await discover(resource=resource)
    assert info == res1info


@pytest.mark.parametrize('namespaced', [True, False])
async def test_is_namespaced(
        resp_mocker, aresponses, hostname, namespaced):

    res1info = {'name': 'someresources', 'namespaced': namespaced}
    result = {'resources': [res1info]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    result = await is_namespaced(resource=resource)

    assert result == namespaced


@pytest.mark.parametrize('namespaced', [True, False])
async def test_is_status_subresource_when_not_a_subresource(
        resp_mocker, aresponses, hostname, namespaced):

    res1info = {'name': 'someresources', 'namespaced': namespaced}
    result = {'resources': [res1info]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    result = await is_status_subresource(resource=resource)

    assert result is False  # an extra type-check


@pytest.mark.parametrize('namespaced', [True, False])
async def test_is_status_subresource_when_is_a_subresource(
        resp_mocker, aresponses, hostname, namespaced):

    res1info = {'name': 'someresources', 'namespaced': namespaced}
    res1status = {'name': 'someresources/status', 'namespaced': namespaced}
    result = {'resources': [res1info, res1status]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, '/apis/some-group.org/someversion', 'get', list_mock)

    resource = Resource('some-group.org', 'someversion', 'someresources')
    result = await is_status_subresource(resource=resource)

    assert result is True  # an extra type-check
