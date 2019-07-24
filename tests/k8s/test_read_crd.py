import aiohttp.web
import pytest

from kopf.clients.fetching import read_crd, CRD_CRD


async def test_when_present(
        resp_mocker, aresponses, hostname, resource):

    get_mock = resp_mocker(return_value=aiohttp.web.json_response({'a': 'b'}))
    aresponses.add(hostname, CRD_CRD.get_url(name=resource.name), 'get', get_mock)

    crd = await read_crd(resource=resource)
    assert crd == {'a': 'b'}

    assert get_mock.called
    assert get_mock.call_count == 1


@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_no_default(
        resp_mocker, aresponses, hostname, resource, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, CRD_CRD.get_url(name=resource.name), 'get', get_mock)

    with pytest.raises(aiohttp.ClientResponseError) as e:
        await read_crd(resource=resource)
    assert e.value.status == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_default(
        resp_mocker, aresponses, hostname, resource, default, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, CRD_CRD.get_url(name=resource.name), 'get', get_mock)

    crd = await read_crd(resource=resource, default=default)
    assert crd is default


@pytest.mark.parametrize('status', [400, 401, 500, 666])
async def test_raises_api_error_despite_default(
        resp_mocker, aresponses, hostname, resource, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, CRD_CRD.get_url(name=resource.name), 'get', get_mock)

    with pytest.raises(aiohttp.ClientResponseError) as e:
        await read_crd(resource=resource, default=object())
    assert e.value.status == status
