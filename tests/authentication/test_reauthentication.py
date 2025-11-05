import aiohttp.web

from kopf._cogs.clients.auth import APIContext, authenticated
from kopf._cogs.structs.credentials import ConnectionInfo


@authenticated
async def fn(
        x: int,
        *,
        context: APIContext | None,
) -> tuple[APIContext, int]:
    return context, x + 100


async def test_session_is_injected(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    context, result = await fn(1)

    async with context.session:
        assert context is not None
        assert result == 101


async def test_session_is_passed_through(
        fake_vault, resp_mocker, aresponses, hostname, resource, namespace):

    result = {}
    get_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, resource.get_url(namespace=namespace, name='xyz'), 'get', get_mock)

    explicit_context = APIContext(ConnectionInfo(server='http://irrelevant/'))
    context, result = await fn(1, context=explicit_context)

    async with context.session:
        assert context is explicit_context
        assert result == 101
