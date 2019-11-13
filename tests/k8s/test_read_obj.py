import aiohttp.web
import pytest

from kopf.clients.fetching import read_obj


@pytest.mark.resource_clustered  # see `resp_mocker`
async def test_when_present_clustered(
        resp_mocker, aresponses, hostname, resource):

    get_mock = resp_mocker(return_value=aiohttp.web.json_response({'a': 'b'}))
    aresponses.add(hostname, resource.get_url(namespace=None, name='name1'), 'get', get_mock)

    crd = await read_obj(resource=resource, namespace=None, name='name1')
    assert crd == {'a': 'b'}

    assert get_mock.called
    assert get_mock.call_count == 1


async def test_when_present_namespaced(
    resp_mocker, aresponses, hostname, resource):

    get_mock = resp_mocker(return_value=aiohttp.web.json_response({'a': 'b'}))
    aresponses.add(hostname, resource.get_url(namespace='ns1', name='name1'), 'get', get_mock)

    crd = await read_obj(resource=resource, namespace='ns1', name='name1')
    assert crd == {'a': 'b'}

    assert get_mock.called
    assert get_mock.call_count == 1


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_no_default(
        resp_mocker, aresponses, hostname, resource, namespace, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, resource.get_url(namespace=None, name='name1'), 'get', get_mock)
    aresponses.add(hostname, resource.get_url(namespace='ns1', name='name1'), 'get', get_mock)

    with pytest.raises(aiohttp.ClientResponseError) as e:
        await read_obj(resource=resource, namespace=namespace, name='name1')
    assert e.value.status == status


@pytest.mark.parametrize('default', [None, object()], ids=['none', 'object'])
@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [403, 404])
async def test_when_absent_with_default(
        resp_mocker, aresponses, hostname, resource, namespace, default, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, resource.get_url(namespace=None, name='name1'), 'get', get_mock)
    aresponses.add(hostname, resource.get_url(namespace='ns1', name='name1'), 'get', get_mock)

    crd = await read_obj(resource=resource, namespace=namespace, name='name1', default=default)
    assert crd is default


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 500, 666])
async def test_raises_api_error_despite_default(
        resp_mocker, aresponses, hostname, resource, namespace, status):

    get_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, resource.get_url(namespace=None, name='name1'), 'get', get_mock)
    aresponses.add(hostname, resource.get_url(namespace='ns1', name='name1'), 'get', get_mock)

    with pytest.raises(aiohttp.ClientResponseError) as e:
        await read_obj(resource=resource, namespace=namespace, name='name1', default=object())
    assert e.value.status == status
