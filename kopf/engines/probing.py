import asyncio
import logging
import urllib.parse
from typing import Optional, Tuple

import aiohttp.web

logger = logging.getLogger(__name__)

LOCALHOST: str = 'localhost'
HTTP_PORT: int = 80

_Key = Tuple[str, int]  # hostname, port


async def get_health(
        request: aiohttp.web.Request,
) -> aiohttp.web.Response:
    return aiohttp.web.json_response({
        'status': 'OK',
    })


async def health_reporter(
        endpoint: str,
        *,
        ready_flag: Optional[asyncio.Event] = None,  # used for testing
) -> None:
    """
    Simple HTTP(S)/TCP server to report the operator's health to K8s probes.

    Runs forever until cancelled (which happens if any other root task
    is cancelled or failed). Once it will stop responding for any reason,
    Kubernetes will assume the pod is not alive anymore, and will restart it.
    """

    parts = urllib.parse.urlsplit(endpoint)
    if parts.scheme == 'http':
        host = parts.hostname or LOCALHOST
        port = parts.port or HTTP_PORT
        path = parts.path
    else:
        raise Exception(f"Unsupported scheme: {endpoint}")

    app = aiohttp.web.Application()
    app.add_routes([aiohttp.web.get(path, get_health)])

    runner = aiohttp.web.AppRunner(app, handle_signals=False)
    await runner.setup()

    site = aiohttp.web.TCPSite(runner, host, port, shutdown_timeout=1.0)
    await site.start()

    # Log with the actual URL: normalised, with hostname/port set.
    url = urllib.parse.urlunsplit([parts.scheme, f'{host}:{port}', path, '', ''])
    logger.debug("Serving health status at %s", url)
    if ready_flag is not None:
        ready_flag.set()

    try:
        # Sleep forever. No activity is needed.
        await asyncio.Event().wait()
    finally:
        # On any reason of exit, stop reporting the health.
        await asyncio.shield(runner.cleanup())
