import asyncio

import aiohttp.web
import pytest

from kopf._cogs.clients.api import request
from kopf._cogs.clients.errors import APIError


@pytest.fixture(autouse=True)
def sleep(mocker):
    """Do not let it actually sleep, even if it is a 0-sleep."""
    return mocker.patch('asyncio.sleep')


@pytest.fixture()
def request_fn(mocker):
    return mocker.patch('aiohttp.ClientSession.request')


async def test_regular_errors_escalate_without_retries(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, request_fn):
    request_fn.side_effect = Exception("boo")

    settings.networking.error_backoffs = [1, 2, 3]
    with pytest.raises(Exception) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert str(err.value) == "boo"
    assert request_fn.call_count == 1
    assert_logs(prohibited=["attempt", "escalating", "retry"])


@pytest.mark.parametrize('status', [400, 404, 499, 666, 999])
async def test_client_errors_escalate_without_retries(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, status):
    # side_effect instead of return_value -- to generate a new response on every call, not reuse it.
    mock = resp_mocker(side_effect=lambda: aiohttp.web.json_response({}, status=status, reason='oops'))
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = [1, 2, 3]
    with pytest.raises(APIError) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert err.value.status == status
    assert mock.call_count == 1
    assert_logs(prohibited=["attempt", "escalating", "retry"])


@pytest.mark.parametrize('status', [500, 503, 599])
async def test_server_errors_escalate_with_retries(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, status):
    # side_effect instead of return_value -- to generate a new response on every call, not reuse it.
    mock = resp_mocker(side_effect=lambda: aiohttp.web.json_response({}, status=status, reason='oops'))
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = [0, 0, 0]
    with pytest.raises(APIError) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert err.value.status == status
    assert mock.call_count == 4
    assert_logs([
        "attempt #1/4 failed; will retry",
        "attempt #2/4 failed; will retry",
        "attempt #3/4 failed; will retry",
        "attempt #4/4 failed; escalating",
    ])


async def test_connection_errors_escalate_with_retries(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, request_fn):
    request_fn.side_effect = aiohttp.ClientConnectionError()

    settings.networking.error_backoffs = [0, 0, 0]
    with pytest.raises(aiohttp.ClientConnectionError):
        await request('get', '/url', settings=settings, logger=logger)

    assert request_fn.call_count == 4
    assert_logs([
        "attempt #1/4 failed; will retry",
        "attempt #2/4 failed; will retry",
        "attempt #3/4 failed; will retry",
        "attempt #4/4 failed; escalating",
    ])


async def test_timeout_errors_escalate_with_retries(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, request_fn):
    request_fn.side_effect = asyncio.TimeoutError()

    settings.networking.error_backoffs = [0, 0, 0]
    with pytest.raises(asyncio.TimeoutError):
        await request('get', '/url', settings=settings, logger=logger)

    assert request_fn.call_count == 4
    assert_logs([
        "attempt #1/4 failed; will retry",
        "attempt #2/4 failed; will retry",
        "attempt #3/4 failed; will retry",
        "attempt #4/4 failed; escalating",
    ])


async def test_retried_until_succeeded(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname):
    mock = resp_mocker(side_effect=[
        aiohttp.web.json_response({}, status=505, reason='oops'),
        aiohttp.web.json_response({}, status=505, reason='oops'),
        aiohttp.web.json_response({}),
    ])
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = [0, 0, 0]
    await request('get', '/url', settings=settings, logger=logger)

    assert mock.call_count == 3  # 2 failures, 1 success; limited to 4 attempts.
    assert_logs([
        "attempt #1/4 failed; will retry",
        "attempt #2/4 failed; will retry",
        "attempt #3/4 succeeded",
    ], prohibited=[
        "attempt #4/4",
    ])


@pytest.mark.parametrize('backoffs, exp_calls', [
    ([], 1),
    ([0], 2),
    ([1], 2),
    ([0, 0], 3),
    ([1, 2], 3),
])
async def test_backoffs_as_lists(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, sleep,
        backoffs, exp_calls):
    # side_effect instead of return_value -- to generate a new response on every call, not reuse it.
    mock = resp_mocker(side_effect=lambda: aiohttp.web.json_response({}, status=500, reason='oops'))
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = backoffs
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert mock.call_count == exp_calls
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == backoffs


async def test_backoffs_as_floats(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, sleep):
    # side_effect instead of return_value -- to generate a new response on every call, not reuse it.
    mock = resp_mocker(side_effect=lambda: aiohttp.web.json_response({}, status=500, reason='oops'))
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = 5.0
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert mock.call_count == 2
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == [5.0]


async def test_backoffs_as_iterables(
        assert_logs, settings, logger, resp_mocker, aresponses, hostname, sleep):

    class Itr:
        def __iter__(self):
            return iter([1, 2, 3])

    # side_effect instead of return_value -- to generate a new response on every call, not reuse it.
    mock = resp_mocker(side_effect=lambda: aiohttp.web.json_response({}, status=500, reason='oops'))
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts
    aresponses.add(hostname, '/url', 'get', mock)  # repeat=N would copy the mock, lose all counts

    settings.networking.error_backoffs = Itr()  # to be reused on every attempt
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert mock.call_count == 8
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == [1, 2, 3, 1, 2, 3]
