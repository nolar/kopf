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
    exc = APIError(status=456, headers={'X-H': 'abc'})
    assert exc.status == 456
    assert exc.code is None
    assert exc.message is None
    assert exc.details is None
    assert str(exc) == ""
    assert repr(exc) == "APIError(status=456)"  # no headers!


def test_exception_with_dict_payload():
    exc = APIError({"message": "msg", "code": 123, "details": {"a": "b"}}, status=456, headers={'X-H': 'abc'})
    assert exc.status == 456
    assert exc.code == 123
    assert exc.message == "msg"
    assert exc.details == {"a": "b"}
    assert str(exc) == "('msg', {'message': 'msg', 'code': 123, 'details': {'a': 'b'}})"
    assert repr(exc) == "APIError('msg', {'message': 'msg', 'code': 123, 'details': {'a': 'b'}}, status=456)"


def test_exception_with_text_payload():
    exc = APIError("oops!", status=456, headers={'X-H': 'abc'})
    assert exc.status == 456
    assert exc.code is None
    assert exc.message is None
    assert exc.details is None
    assert str(exc) == "oops!"
    assert repr(exc) == "APIError('oops!', status=456)"  # no headers!


@pytest.mark.parametrize('status', [200, 202, 300, 304])
async def test_no_error_on_success(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        reason='oops',
        headers={'Content-Type': 'application/json'},
        text='{"kind": "Status", "code": "xxx", "message": "msg"}',
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    await get_it(f"http://{hostname}/")


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
async def test_error_with_dict_payload(
        resp_mocker, aresponses, hostname, status, exctype):

    resp = aresponses.Response(
        status=status,
        reason='oops',
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
async def test_error_with_text_payload(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        reason='oops',
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
    assert str(err.value) == "unparsable json"


@pytest.mark.parametrize('status', [400, 500, 666])
async def test_error_with_parseable_nonk8s_payload(
        resp_mocker, aresponses, hostname, status):

    resp = aresponses.Response(
        status=status,
        reason='oops',
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
    assert str(err.value) == ""


async def test_cutting_the_text_response_overflow(resp_mocker, aresponses, hostname):
    resp = aresponses.Response(
        status=400,
        reason='oops',
        headers={'Content-Type': 'application/json'},
        text='helloworld'*1000,
    )
    aresponses.add(hostname, '/', 'get', resp_mocker(return_value=resp))

    with pytest.raises(APIError) as err:
        await get_it(f"http://{hostname}/")

    # 256 comes from TEXT_ERROR_MAX_SIZE, 10 is the length of "helloworld".
    assert str(err.value) == "helloworld" * (256//10) + "hel..."
