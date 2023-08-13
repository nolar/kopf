import aiohttp
import pytest

from kopf._cogs.clients.auth import APIContext, authenticated
from kopf._cogs.clients.errors import APIClientError, APIConflictError, APIError, \
                                      APIForbiddenError, APINotFoundError, \
                                      APIServerError, check_response


@authenticated
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
async def test_no_error_on_success(kmock, status):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << {"kind": "Status", "code": "xxx", "message": "msg"}
    await get_it(str(kmock.url))


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status, exctype', [
    (403, APIForbiddenError),
    (404, APINotFoundError),
    (409, APIConflictError),
    (400, APIClientError),
    (403, APIClientError),
    (404, APIClientError),
    (500, APIServerError),
    (503, APIServerError),
    (400, APIError),
    (500, APIError),
    (666, APIError),
])
async def test_error_with_payload(kmock, status, exctype):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << {"kind": "Status", "code": 123, "message": "msg", "details": {"a": "b"}}

    with pytest.raises(APIError) as err:
        await get_it(str(kmock.url))

    assert not isinstance(err.value, aiohttp.ClientResponseError)
    assert isinstance(err.value, exctype)
    assert err.value.status == status
    assert err.value.code == 123
    assert err.value.message == 'msg'
    assert err.value.details == {'a': 'b'}


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_error_with_nonjson_payload(kmock, status):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << b'unparsable json'

    with pytest.raises(APIError) as err:
        await get_it(str(kmock.url))

    assert err.value.status == status
    assert err.value.code is None
    assert err.value.message is None
    assert err.value.details is None


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_error_with_parseable_nonk8s_payload(kmock, status):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << {"kind": "NonStatus", "code": "xxx", "message": "msg"}

    with pytest.raises(APIError) as err:
        await get_it(str(kmock.url))

    assert err.value.status == status
    assert err.value.code is None
    assert err.value.message is None
    assert err.value.details is None
