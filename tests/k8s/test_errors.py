import aiohttp
import pytest

from kopf.clients.auth import APIContext, reauthenticated_request
from kopf.clients.errors import APIClientResponseError, check_response


@reauthenticated_request
async def get_it(url: str, *, context: APIContext) -> None:
    response = await context.session.get(url)
    await check_response(response)
    return await response.json()


@pytest.mark.parametrize('status', [200, 202, 300, 304])
async def test_no_error_on_success(
        resp_mocker, aresponses, hostname, resource, status):

    resp = aresponses.Response(
        status=status,
        reason="boo!",
        headers={'Content-Type': 'application/json'},
        text='{"kind": "Status", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    await get_it(f"http://{hostname}/")


@pytest.mark.parametrize('status', [400, 401, 403, 404, 500, 666])
async def test_replaced_error_raised_with_payload(
        resp_mocker, aresponses, hostname, resource, status):

    resp = aresponses.Response(
        status=status,
        reason="boo!",
        headers={'Content-Type': 'application/json'},
        text='{"kind": "Status", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(aiohttp.ClientResponseError) as err:
        await get_it(f"http://{hostname}/")

    assert isinstance(err.value, APIClientResponseError)
    assert err.value.status == status
    assert err.value.message == 'msg'


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_original_error_raised_if_nonjson_payload(
        resp_mocker, aresponses, hostname, resource, status):

    resp = aresponses.Response(
        status=status,
        reason="boo!",
        headers={'Content-Type': 'application/json'},
        text='unparsable json',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(aiohttp.ClientResponseError) as err:
        await get_it(f"http://{hostname}/")

    assert not isinstance(err.value, APIClientResponseError)
    assert err.value.status == status
    assert err.value.message == 'boo!'


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_original_error_raised_if_parseable_nonk8s_payload(
        resp_mocker, aresponses, hostname, resource, status):

    resp = aresponses.Response(
        status=status,
        reason="boo!",
        headers={'Content-Type': 'application/json'},
        text='{"kind": "NonStatus", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(aiohttp.ClientResponseError) as err:
        await get_it(f"http://{hostname}/")

    assert not isinstance(err.value, APIClientResponseError)
    assert err.value.status == status
    assert err.value.message == 'boo!'
