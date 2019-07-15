import asyncio

import aiohttp

from kopf.engines.probing import health_reporter


async def test_liveness(aiohttp_unused_port):

    # The server startup is not instant, so we need a readiness flag.
    ready_flag = asyncio.Event()

    port = aiohttp_unused_port()
    server = asyncio.create_task(
        health_reporter(
            endpoint=f'http://:{port}/xyz',
            ready_flag=ready_flag,
        )
    )

    try:
        url = f'http://localhost:{port}/xyz'
        await ready_flag.wait()
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                assert isinstance(data, dict)
                assert data['status'] == 'OK'

    finally:
        server.cancel()
        try:
            await server
        except asyncio.CancelledError:
            pass
