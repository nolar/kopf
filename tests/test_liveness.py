import asyncio

import aiohttp
import pytest

from kopf.engines.probing import health_reporter
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.registries import OperatorRegistry
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import Activity, ActivityHandler


@pytest.fixture()
async def liveness_registry():
    return OperatorRegistry()


@pytest.fixture()
async def liveness_url(settings, liveness_registry, aiohttp_unused_port):

    # The server startup is not instant, so we need a readiness flag.
    ready_flag = asyncio.Event()

    port = aiohttp_unused_port()
    server = asyncio.create_task(
        health_reporter(
            endpoint=f'http://:{port}/xyz',
            registry=liveness_registry,
            settings=settings,
            ready_flag=ready_flag,
            indices=OperatorIndexers().indices,
            memo=Memo(),
        )
    )

    try:
        await ready_flag.wait()
        yield f'http://localhost:{port}/xyz'
    finally:
        server.cancel()
        try:
            await server
        except asyncio.CancelledError:
            pass


async def test_liveness_for_just_status(liveness_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(liveness_url) as response:
            data = await response.json()
            assert isinstance(data, dict)


async def test_liveness_with_reporting(liveness_url, liveness_registry):

    def fn1(**kwargs):
        return {'x': 100}

    def fn2(**kwargs):
        return {'y': '200'}

    liveness_registry._activities.append(ActivityHandler(
        fn=fn1, id='id1', activity=Activity.PROBE,
        param=None, errors=None, timeout=None, retries=None, backoff=None,
    ))
    liveness_registry._activities.append(ActivityHandler(
        fn=fn2, id='id2', activity=Activity.PROBE,
        param=None, errors=None, timeout=None, retries=None, backoff=None,
    ))

    async with aiohttp.ClientSession() as session:
        async with session.get(liveness_url) as response:
            data = await response.json()
            assert isinstance(data, dict)
            assert data == {'id1': {'x': 100}, 'id2': {'y': '200'}}


async def test_liveness_data_is_cached(liveness_url, liveness_registry):
    counter = 0

    def fn1(**kwargs):
        nonlocal counter
        counter += 1
        return {'counter': counter}

    liveness_registry._activities.append(ActivityHandler(
        fn=fn1, id='id1', activity=Activity.PROBE,
        param=None, errors=None, timeout=None, retries=None, backoff=None,
    ))

    async with aiohttp.ClientSession() as session:
        async with session.get(liveness_url) as response:
            data = await response.json()
            assert isinstance(data, dict)
            assert data == {'id1': {'counter': 1}}
        async with session.get(liveness_url) as response:
            data = await response.json()
            assert isinstance(data, dict)
            assert data == {'id1': {'counter': 1}}  # not 2!
