import aiohttp.web
from typing import Optional, AsyncIterator, Tuple

from kopf.clients.auth import APISession, reauthenticated_request, reauthenticated_stream


@reauthenticated_request
async def request_fn(
        x: int,
        *,
        session: Optional[APISession],
) -> Tuple[APISession, int]:
    return session, x + 100


@reauthenticated_stream
async def stream_fn(
        x: int,
        *,
        session: Optional[APISession],
) -> AsyncIterator[Tuple[APISession, int]]:
    yield session, x + 100


async def test_session_is_injected_to_request(
        fake_vault, resp_mocker, aresponses, hostname, resource):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=None, name='xyz'), 'get', get_mock)

    session, result = await request_fn(1)

    assert session is not None
    assert result == 101


async def test_session_is_injected_to_stream(
        fake_vault, resp_mocker, aresponses, hostname, resource):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=None, name='xyz'), 'get', get_mock)

    counter = 0
    async for session, result in stream_fn(1):
        counter += 1

    assert session is not None
    assert result == 101
    assert counter == 1


async def test_session_is_passed_through_to_request(
        fake_vault, resp_mocker, aresponses, hostname, resource):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=None, name='xyz'), 'get', get_mock)

    explicit_session = APISession()
    session, result = await request_fn(1, session=explicit_session)

    assert session is explicit_session
    assert result == 101


async def test_session_is_passed_through_to_stream(
        fake_vault, resp_mocker, aresponses, hostname, resource):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=None, name='xyz'), 'get', get_mock)

    explicit_session = APISession()
    counter = 0
    async for session, result in stream_fn(1, session=explicit_session):
        counter += 1

    assert session is explicit_session
    assert result == 101
    assert counter == 1
