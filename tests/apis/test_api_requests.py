import asyncio
import textwrap

import aiohttp.web
import pytest

from kopf._cogs.clients.api import delete, get, patch, post, request, stream
from kopf._cogs.clients.errors import APIError

pytestmark = pytest.mark.usefixtures('fake_vault')


@pytest.mark.parametrize('method', ['get', 'post', 'patch', 'delete'])
async def test_raw_requests_work(kmock, method, settings, logger):
    api = kmock[method, '/url'] << {}

    response = await request(
        method=method,
        url='/url',
        payload={'fake': 'payload'},
        headers={'fake': 'headers'},
        settings=settings,
        logger=logger,
    )
    assert isinstance(response, aiohttp.ClientResponse)  # unparsed!
    assert len(api) == 1
    assert api[0].method.lower() == method
    assert api[0].url.path == '/url'
    assert api[0].data == {'fake': 'payload'}
    assert api[0].headers['fake'] == 'headers'  # and other system headers


@pytest.mark.parametrize('method', ['get', 'post', 'patch', 'delete'])
async def test_raw_requests_are_not_parsed(kmock, method, settings, logger):
    kmock[method, '/url'] << b'BAD JSON!'
    response = await request(method, '/url', settings=settings, logger=logger)
    assert isinstance(response, aiohttp.ClientResponse)


@pytest.mark.parametrize('method', ['get', 'post', 'patch', 'delete'])
async def test_server_errors_escalate(kmock, method, settings, logger):
    kmock[method, '/url'] << {} << 666
    with pytest.raises(APIError) as err:
        await request(method, '/url', settings=settings, logger=logger)
    assert err.value.status == 666


@pytest.mark.parametrize('method', ['get', 'post', 'patch', 'delete'])
async def test_relative_urls_are_prepended_with_server(kmock, fake_vault, method, settings, logger):
    kmock[method, '/url'] << {}
    await request(method, '/url', settings=settings, logger=logger)
    assert len(kmock) == 1
    assert kmock[0].url.host == kmock.url.host


@pytest.mark.parametrize('method', ['get', 'post', 'patch', 'delete'])
@pytest.mark.kmock(hostnames=['fakehost.fakedomain.tld'])
async def test_absolute_urls_are_passed_through(kmock, fake_vault, method, settings, logger):
    kmock[method, '/url'] << {}
    await request(method, 'http://fakehost.fakedomain.tld/url', settings=settings, logger=logger)
    assert len(kmock) == 1
    assert kmock[0].url.host == 'fakehost.fakedomain.tld'


@pytest.mark.parametrize('fn, method', [
    (get, 'get'),
    (post, 'post'),
    (patch, 'patch'),
    (delete, 'delete'),
])
async def test_parsing_in_requests(kmock, fn, method, settings, logger):
    api = kmock[method, '/url'] << {'fake': 'result'}
    response = await fn(
        url='/url',
        payload={'fake': 'payload'},
        headers={'fake': 'headers'},
        settings=settings,
        logger=logger,
    )
    assert response == {'fake': 'result'}  # parsed!
    assert len(api) == 1
    assert api[0].method.lower() == method
    assert api[0].url.path == '/url'
    assert api[0].data == {'fake': 'payload'}
    assert api[0].headers['fake'] == 'headers'  # and other system headers


@pytest.mark.parametrize('method', ['get'])  # the only supported method at the moment
async def test_parsing_in_streams(kmock, method, settings, logger):
    kmock[method, '/url'] << {"fake": "result1"} << {"fake": "result2"}

    items = []
    async for item in stream(
        url='/url',
        payload={'fake': 'payload'},
        headers={'fake': 'headers'},
        settings=settings,
        logger=logger,
    ):
        items.append(item)

    assert items == [{'fake': 'result1'}, {'fake': 'result2'}]
    assert len(kmock) == 1
    assert kmock[0].method.lower() == method
    assert kmock[0].url.path == '/url'
    assert kmock[0].data == {'fake': 'payload'}
    assert kmock[0].headers['fake'] == 'headers'  # and other system headers


@pytest.mark.parametrize('fn, method', [
    (get, 'get'),
    (post, 'post'),
    (patch, 'patch'),
    (delete, 'delete'),
])
async def test_direct_timeout_in_requests(kmock, fn, method, settings, logger, looptime):
    kmock[method, '/url'] << (lambda: asyncio.sleep(10)) << {}

    with pytest.raises(asyncio.TimeoutError):
        timeout = aiohttp.ClientTimeout(total=1.23)
        # aiohttp raises an asyncio.TimeoutError which is automatically retried.
        # To reduce the test duration we disable retries for this test.
        settings.networking.error_backoffs = None
        await fn('/url', timeout=timeout, settings=settings, logger=logger)

    assert looptime == 1.23


@pytest.mark.parametrize('fn, method', [
    (get, 'get'),
    (post, 'post'),
    (patch, 'patch'),
    (delete, 'delete'),
])
async def test_settings_timeout_in_requests(kmock, fn, method, settings, logger, looptime):
    kmock[method, '/url'] << (lambda: asyncio.sleep(10)) << {}

    with pytest.raises(asyncio.TimeoutError):
        settings.networking.request_timeout = 1.23
        # aiohttp raises an asyncio.TimeoutError which is automatically retried.
        # To reduce the test duration we disable retries for this test.
        settings.networking.error_backoffs = None
        await fn('/url', settings=settings, logger=logger)

    assert looptime == 1.23


@pytest.mark.parametrize('method', ['get'])  # the only supported method at the moment
async def test_direct_timeout_in_streams(kmock, method, settings, logger, looptime):

    kmock[method, '/url'] << (lambda: asyncio.sleep(10)) << {}

    with pytest.raises(asyncio.TimeoutError):
        timeout = aiohttp.ClientTimeout(total=1.23)
        # aiohttp raises an asyncio.TimeoutError which is automatically retried.
        # To reduce the test duration we disable retries for this test.
        settings.networking.error_backoffs = None
        async for _ in stream('/url', timeout=timeout, settings=settings, logger=logger):
            pass

    assert looptime == 1.23


@pytest.mark.parametrize('method', ['get'])  # the only supported method at the moment
async def test_settings_timeout_in_streams(kmock, method, settings, logger, looptime):
    kmock[method, '/url'] << (lambda: asyncio.sleep(10)) << {}

    with pytest.raises(asyncio.TimeoutError):
        settings.networking.request_timeout = 1.23
        # aiohttp raises an asyncio.TimeoutError which is automatically retried.
        # To reduce the test duration we disable retries for this test.
        settings.networking.error_backoffs = None
        async for _ in stream('/url', settings=settings, logger=logger):
            pass

    assert looptime == 1.23


@pytest.mark.parametrize('delay, expected_times, expected_items', [
    pytest.param(0, [], [], id='instant-none'),
    pytest.param(2, [1], [{'fake': 'result1'}], id='fast-single'),
    pytest.param(9, [1, 4], [{'fake': 'result1'}, {'fake': 'result2'}], id='inf-double'),
])
@pytest.mark.parametrize('initial', [None, b''], ids=['slow-headers', 'fast-headers'])
@pytest.mark.parametrize('method', ['get'])  # the only supported method at the moment
async def test_stopper_in_streams(
        kmock, method, delay, initial, settings, logger, looptime,
        expected_items, expected_times):
    kmock[method, '/url'] << (
        initial,  # prepare and send the headers before the sleep
        lambda: asyncio.sleep(1),
        {"fake": "result1"},
        lambda: asyncio.sleep(3),
        {"fake": "result2"},
    )

    stopper = asyncio.Future()
    asyncio.get_running_loop().call_later(delay, stopper.set_result, None)

    items = []
    times = []
    async for item in stream('/url', stopper=stopper, settings=settings, logger=logger):
        items.append(item)
        times.append(float(looptime))

    assert items == expected_items
    assert times == expected_times
