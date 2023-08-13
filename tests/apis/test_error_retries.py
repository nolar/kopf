import asyncio

import aiohttp.web
import pytest

from kopf._cogs.clients.api import request
from kopf._cogs.clients.errors import APIError

pytestmark = pytest.mark.usefixtures('fake_vault')


@pytest.fixture(autouse=True)
def sleep(mocker):
    """Do not let it actually sleep, even if it is a 0-sleep."""
    return mocker.patch('asyncio.sleep')


@pytest.fixture()
def request_fn(mocker):
    return mocker.patch('aiohttp.ClientSession.request')


async def test_regular_errors_escalate_without_retries(assert_logs, settings, logger, request_fn):
    request_fn.side_effect = Exception("boo")

    settings.networking.error_backoffs = [1, 2, 3]
    with pytest.raises(Exception) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert str(err.value) == "boo"
    assert request_fn.call_count == 1
    assert_logs([], prohibited=["attempt", "escalating", "retry"])


@pytest.mark.parametrize('status', [400, 404, 499, 666, 999])
async def test_client_errors_escalate_without_retries(
        caplog, assert_logs, settings, logger, kmock, status):
    caplog.set_level(0)
    api = kmock['get /url'] << {} << status

    settings.networking.error_backoffs = [1, 2, 3]
    with pytest.raises(APIError) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert err.value.status == status
    assert len(api) == 1
    assert_logs([], prohibited=["attempt", "escalating", "retry"])


@pytest.mark.parametrize('status', [500, 503, 599])
async def test_server_errors_escalate_with_retries(
        caplog, assert_logs, settings, logger, kmock, status):
    caplog.set_level(0)
    api = kmock['get /url'] << {} << status

    settings.networking.error_backoffs = [0, 0, 0]
    with pytest.raises(APIError) as err:
        await request('get', '/url', settings=settings, logger=logger)

    assert err.value.status == status
    assert len(api) == 4
    assert_logs([
        "attempt #1/4 failed; will retry",
        "attempt #2/4 failed; will retry",
        "attempt #3/4 failed; will retry",
        "attempt #4/4 failed; escalating",
    ])


async def test_connection_errors_escalate_with_retries(assert_logs, settings, logger, request_fn):
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


async def test_timeout_errors_escalate_with_retries(assert_logs, settings, logger, request_fn):
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


async def test_retried_until_succeeded(assert_logs, settings, logger, kmock):
    api1 = kmock['get /url'][:2] << {} << 505
    api2 = kmock['get /url'] << {}

    settings.networking.error_backoffs = [0, 0, 0]
    await request('get', '/url', settings=settings, logger=logger)

    assert len(api1) == 2  # 2 failures, 1 success
    assert len(api2) == 1  # 1 success; the other one is not requested
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
async def test_backoffs_as_lists(settings, logger, kmock, sleep, backoffs, exp_calls):
    api = kmock['get /url'][:4] << {} << 500

    settings.networking.error_backoffs = backoffs
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert len(api) == exp_calls
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == backoffs


async def test_backoffs_as_floats(settings, logger, kmock, sleep):
    api = kmock['get /url'] << {} << 500

    settings.networking.error_backoffs = 5.0
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert len(api) == 2
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == [5.0]


async def test_backoffs_as_iterables(settings, logger, kmock, sleep):
    api = kmock['get /url'][:8] << {} << 500

    class Itr:
        def __iter__(self):
            return iter([1, 2, 3])

    settings.networking.error_backoffs = Itr()  # to be reused on every attempt
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)
    with pytest.raises(APIError):
        await request('get', '/url', settings=settings, logger=logger)

    assert len(api) == 8
    all_sleeps = [call[0][0] for call in sleep.call_args_list]
    assert all_sleeps == [1, 2, 3, 1, 2, 3]
