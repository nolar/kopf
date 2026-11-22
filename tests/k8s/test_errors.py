import aiohttp
import freezegun
import pytest

from kopf._cogs.clients.auth import APIContext, authenticated
from kopf._cogs.clients.errors import APIClientError, APIConflictError, APIError, \
                                      APIForbiddenError, APINotFoundError, \
                                      APIServerError, APITooManyRequestsError, \
                                      APIUnprocessableEntityError, check_response


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


@pytest.mark.parametrize('headers, payload, expected', [
    pytest.param({}, None, None, id='none'),
    pytest.param({'Retry-After': '10'}, None, 10, id='new-style'),
    pytest.param({'retry-after': '10'}, None, 10, id='new-style-lowercase'),
    pytest.param({'Retry-After': '1.5'}, None, 2, id='new-style-float'),
    pytest.param({'Retry-After': 'Wed, 21 Oct 2015 07:28:00 GMT'}, None, 0,
                 id='new-style-past-date'),
    pytest.param({'Retry-After': 'Wed, 21 Oct 2015 07:28:00'}, None, 0,
                 id='new-style-past-naive-date'),
    pytest.param({'Retry-After': 'Fri, 01 Jan 2021 00:00:00 GMT'}, None, 10,
                 id='new-style-future-date'),
    pytest.param({'Retry-After': '999999'}, None, 999999, id='new-style-unbounded'),
    pytest.param({'Retry-After': 'invalid'}, None, None, id='new-style-invalid'),
    pytest.param({'Retry-After': 'inf'}, None, None, id='new-style-infinite'),
    pytest.param({}, {'details': {'retryAfterSeconds': 7}}, 7, id='old-style'),
    pytest.param({}, {'details': {'retryAfterSeconds': 'invalid'}}, None, id='old-style-invalid'),
    pytest.param({}, {'details': 'invalid'}, None, id='old-style-malformed-details'),
    pytest.param({'Retry-After': '10'}, {'details': {'retryAfterSeconds': 7}}, 10, id='both'),
])
@freezegun.freeze_time('2020-12-31T23:59:50Z')
def test_exception_retry_after(headers, payload, expected):
    exc = APIError(payload, status=429, headers=headers)
    assert exc.retry_after == expected


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
async def test_no_error_on_success(kmock, status):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << {"kind": "Status", "code": "xxx", "message": "msg"}
    await get_it(str(kmock.url))


# Note: 401 is wrapped into a LoginError and is tested elsewhere.
@pytest.mark.parametrize('status, exctype', [
    (403, APIForbiddenError),
    (404, APINotFoundError),
    (409, APIConflictError),
    (422, APIUnprocessableEntityError),
    (429, APITooManyRequestsError),
    (400, APIClientError),
    (403, APIClientError),
    (404, APIClientError),
    (422, APIClientError),
    (500, APIServerError),
    (503, APIServerError),
    (400, APIError),
    (500, APIError),
    (666, APIError),
])
async def test_error_with_dict_payload(kmock, status, exctype):
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


@pytest.mark.parametrize('status', [400, 429, 500, 666])
async def test_error_with_text_payload(kmock, status):
    headers = {'Content-Type': 'application/json'}
    kmock['/'] << status << headers << b'unparsable json'

    with pytest.raises(APIError) as err:
        await get_it(str(kmock.url))

    assert err.value.status == status
    assert err.value.code is None
    assert err.value.message is None
    assert err.value.details is None
    assert str(err.value) == "unparsable json"


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
    assert str(err.value) == ""


async def test_cutting_the_text_response_overflow(kmock):
    kmock['get /'] << 400 << {'Content-Type': 'application/json'} << 'helloworld'*1000

    with pytest.raises(APIError) as err:
        await get_it(kmock.url)

    # 256 comes from TEXT_ERROR_MAX_SIZE, 10 is the length of "helloworld".
    assert str(err.value) == "helloworld" * (256//10) + "hel..."
