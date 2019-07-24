import aiohttp.web
import pytest

from kopf.clients.fetching import list_objs_rv


async def test_when_successful_clustered(
        resp_mocker, aresponses, hostname, resource):

    result = {'items': [{}, {}]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=None), 'get', list_mock)

    items, resource_version = await list_objs_rv(resource=resource, namespace=None)
    assert items == result['items']

    assert list_mock.called
    assert list_mock.call_count == 1


async def test_when_successful_namespaced(
        resp_mocker, aresponses, hostname, resource):

    result = {'items': [{}, {}]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace='ns1'), 'get', list_mock)

    items, resource_version = await list_objs_rv(resource=resource, namespace='ns1')
    assert items == result['items']

    assert list_mock.called
    assert list_mock.call_count == 1


@pytest.mark.parametrize('namespace', [None, 'ns1'], ids=['without-namespace', 'with-namespace'])
@pytest.mark.parametrize('status', [400, 401, 403, 500, 666])
async def test_raises_api_error(
        resp_mocker, aresponses, hostname, resource, namespace, status):

    list_mock = resp_mocker(return_value=aresponses.Response(status=status, reason="boo!"))
    aresponses.add(hostname, resource.get_url(namespace=None), 'get', list_mock)
    aresponses.add(hostname, resource.get_url(namespace='ns1'), 'get', list_mock)

    with pytest.raises(aiohttp.ClientResponseError) as e:
        await list_objs_rv(resource=resource, namespace=namespace)
    assert e.value.status == status
