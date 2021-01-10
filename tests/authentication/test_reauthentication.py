from typing import AsyncIterator, Optional, Tuple

import aiohttp.web

from kopf.clients.auth import APIContext, reauthenticated_request, reauthenticated_stream
from kopf.structs.credentials import ConnectionInfo


@reauthenticated_request
async def request_fn(
        x: int,
        *,
        context: Optional[APIContext],
) -> Tuple[APIContext, int]:
    return context, x + 100


@reauthenticated_stream
async def stream_fn(
        x: int,
        *,
        context: Optional[APIContext],
) -> AsyncIterator[Tuple[APIContext, int]]:
    yield context, x + 100


async def test_session_is_injected_to_request(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    context, result = await request_fn(1)

    async with context.session:
        assert context is not None
        assert result == 101


async def test_session_is_injected_to_stream(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    context = None
    counter = 0
    async for context, result in stream_fn(1):
        counter += 1

    async with context.session:
        assert context is not None
        assert result == 101
        assert counter == 1


async def test_session_is_passed_through_to_request(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    explicit_context = APIContext(ConnectionInfo(server='http://irrelevant/'))
    context, result = await request_fn(1, context=explicit_context)

    async with context.session:
        assert context is explicit_context
        assert result == 101


async def test_session_is_passed_through_to_stream(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    explicit_context = APIContext(ConnectionInfo(server='http://irrelevant/'))
    counter = 0
    async for context, result in stream_fn(1, context=explicit_context):
        counter += 1

    async with context.session:
        assert context is explicit_context
        assert result == 101
        assert counter == 1
