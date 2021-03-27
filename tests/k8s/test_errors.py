import aiohttp
import pytest

from kopf.clients.auth import APIContext, reauthenticated_request
from kopf.clients.errors import APIConflictError, APIError, APIForbiddenError, \
                                APINotFoundError, APIUnauthorizedError, check_response


@reauthenticated_request
async def get_it(url: str, *, context: APIContext) -> None:
    response = await context.session.get(url)
    await check_response(response)
    return await response.json()


def test_aiohttp_is_not_leaked_outside():
    assert not issubclass(APIError, aiohttp.ClientError)


def test_exception_without_payload():
    exc = APIError(None, status=456)
    assert exc.status == 456
    assert exc.code is None
    assert exc.message is None
    assert exc.details is None


def test_exception_with_payload():
    exc = APIError({"message": "msg", "code": 123, "details": {"a": "b"}}, status=456)
    assert exc.status == 456
    assert exc.code == 123
    assert exc.message == "msg"
    assert exc.details == {"a": "b"}


@pytest.mark.parametrize('status', [200, 202, 300, 304])
async def test_no_error_on_success(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        headers={'Content-Type': 'application/json'},
        text='{"kind": "Status", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    await get_it(f"http://{hostname}/")


@pytest.mark.parametrize('status, exctype', [
    (400, APIError),
    (401, APIUnauthorizedError),
    (403, APIForbiddenError),
    (404, APINotFoundError),
    (409, APIConflictError),
    (500, APIError),
    (666, APIError),
])
async def test_error_with_payload(
        resp_mocker, aresponses, hostname, status, exctype):

    resp = aresponses.Response(
        status=status,
        headers={'Content-Type': 'application/json'},
        text='{"kind": "Status", "code": 123, "message": "msg", "details": {"a": "b"}}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(APIError) as err:
        await get_it(f"http://{hostname}/")

    assert not isinstance(err.value, aiohttp.ClientResponseError)
    assert isinstance(err.value, exctype)
    assert err.value.status == status
    assert err.value.code == 123
    assert err.value.message == 'msg'
    assert err.value.details == {'a': 'b'}


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_error_with_nonjson_payload(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        headers={'Content-Type': 'application/json'},
        text='unparsable json',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(APIError) as err:
        await get_it(f"http://{hostname}/")

    assert err.value.status == status
    assert err.value.code is None
    assert err.value.message is None
    assert err.value.details is None


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_error_with_parseable_nonk8s_payload(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        headers={'Content-Type': 'application/json'},
        text='{"kind": "NonStatus", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(APIError) as err:
        await get_it(f"http://{hostname}/")

    assert err.value.status == status
    assert err.value.code is None
    assert err.value.message is None
    assert err.value.details is None
