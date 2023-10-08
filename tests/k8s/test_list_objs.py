import aiohttp.web
import pytest

from kopf._cogs.clients.errors import APIError
from kopf._cogs.clients.fetching import list_objs
from kopf._cogs.structs.credentials import LoginError


async def test_listing_works(
        resp_mocker, aresponses, hostname, settings, logger, resource, namespace,
        cluster_resource, namespaced_resource):

    result = {'items': [{}, {}]}
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    cluster_url = cluster_resource.get_url(namespace=None)
    namespaced_url = namespaced_resource.get_url(namespace='ns')
    aresponses.add(hostname, cluster_url, 'get', list_mock)
    aresponses.add(hostname, namespaced_url, 'get', list_mock)

    items, resource_version = await list_objs(
        logger=logger,
        settings=settings,
        resource=resource,
        namespace=namespace,
    )
    assert items == result['items']

    assert list_mock.called
    assert list_mock.call_count == 1


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status', [400, 403, 500, 666])
async def test_raises_direct_api_errors(
        resp_mocker, aresponses, hostname, settings, logger, status, resource, namespace,
        cluster_resource, namespaced_resource):

    list_mock = resp_mocker(return_value=aresponses.Response(status=status, reason='oops'))
    cluster_url = cluster_resource.get_url(namespace=None)
    namespaced_url = namespaced_resource.get_url(namespace='ns')
    aresponses.add(hostname, cluster_url, 'get', list_mock)
    aresponses.add(hostname, namespaced_url, 'get', list_mock)

    with pytest.raises(APIError) as e:
        await list_objs(
            logger=logger,
            settings=settings,
            resource=resource,
            namespace=namespace,
        )
    assert e.value.status == status
