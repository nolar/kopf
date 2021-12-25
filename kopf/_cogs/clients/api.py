import asyncio
import collections.abc
import itertools
import json
import ssl
import urllib.parse
from typing import Any, AsyncIterator, Mapping, Optional, Tuple

import aiohttp

from kopf._cogs.aiokits import aiotasks
from kopf._cogs.clients import auth, errors
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs


@auth.authenticated
async def get_default_namespace(
        *,
        context: Optional[auth.APIContext] = None,
) -> Optional[str]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")
    return context.default_namespace


@auth.authenticated
async def read_sslcert(
        *,
        context: Optional[auth.APIContext] = None,
) -> Tuple[str, bytes]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    parsed = urllib.parse.urlparse(context.server)
    host = parsed.hostname or ''  # NB: it cannot be None/empty in our case.
    port = parsed.port or 443
    loop = asyncio.get_running_loop()
    cert = await loop.run_in_executor(None, ssl.get_server_certificate, (host, port))
    return host, cert.encode('ascii')


@auth.authenticated
async def request(
        method: str,
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
        logger: typedefs.Logger,
) -> aiohttp.ClientResponse:
    if context is None:  # for type-checking!
        raise RuntimeError("API instance is not injected by the decorator.")

    if '://' not in url:
        url = context.server.rstrip('/') + '/' + url.lstrip('/')

    if timeout is None:
        timeout = aiohttp.ClientTimeout(
            total=settings.networking.request_timeout,
            sock_connect=settings.networking.connect_timeout,
        )

    backoffs = settings.networking.error_backoffs
    backoffs = backoffs if isinstance(backoffs, collections.abc.Iterable) else [backoffs]
    count = len(backoffs) + 1 if isinstance(backoffs, collections.abc.Sized) else None
    backoff: Optional[float]
    for retry, backoff in enumerate(itertools.chain(backoffs, [None]), start=1):
        idx = f"#{retry}/{count}" if count is not None else f"#{retry}"
        what = f"{method.upper()} {url}"
        try:
            if retry > 1:
                logger.debug(f"Request attempt {idx}: {what}")

            response = await context.session.request(
                method=method,
                url=url,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
            await errors.check_response(response)  # but do not parse it!

        except (aiohttp.ClientConnectionError, errors.APIServerError, asyncio.TimeoutError) as e:
            if backoff is None:  # i.e. the last or the only attempt.
                logger.error(f"Request attempt {idx} failed; escalating: {what} -> {e!r}")
                raise
            else:
                logger.error(f"Request attempt {idx} failed; will retry: {what} -> {e!r}")
                await asyncio.sleep(backoff)  # non-awakable! but still cancellable.
        else:
            if retry > 1:
                logger.debug(f"Request attempt {idx} succeeded: {what}")
            return response

    raise RuntimeError("Broken retryable routine.")  # impossible, but needed for type-checking.


async def get(
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        logger: typedefs.Logger,
) -> Any:
    response = await request(
        method='get',
        url=url,
        payload=payload,
        headers=headers,
        timeout=timeout,
        settings=settings,
        logger=logger,
    )
    async with response:
        return await response.json()


async def post(
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        logger: typedefs.Logger,
) -> Any:
    response = await request(
        method='post',
        url=url,
        payload=payload,
        headers=headers,
        timeout=timeout,
        settings=settings,
        logger=logger,
    )
    async with response:
        return await response.json()


async def patch(
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        logger: typedefs.Logger,
) -> Any:
    response = await request(
        method='patch',
        url=url,
        payload=payload,
        headers=headers,
        timeout=timeout,
        settings=settings,
        logger=logger,
    )
    async with response:
        return await response.json()


async def delete(
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        logger: typedefs.Logger,
) -> Any:
    response = await request(
        method='delete',
        url=url,
        payload=payload,
        headers=headers,
        timeout=timeout,
        settings=settings,
        logger=logger,
    )
    async with response:
        return await response.json()


async def stream(
        url: str,  # relative to the server/api root.
        *,
        settings: configuration.OperatorSettings,
        payload: Optional[object] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        stopper: Optional[aiotasks.Future] = None,
        logger: typedefs.Logger,
) -> AsyncIterator[Any]:
    response = await request(
        method='get',
        url=url,
        payload=payload,
        headers=headers,
        timeout=timeout,
        settings=settings,
        logger=logger,
    )
    response_close_callback = lambda _: response.close()  # to remove the positional arg.
    if stopper is not None:
        stopper.add_done_callback(response_close_callback)
    try:
        async with response:
            async for line in iter_jsonlines(response.content):
                yield json.loads(line.decode('utf-8'))
    except aiohttp.ClientConnectionError:
        if stopper is not None and stopper.done():
            pass
        else:
            raise
    finally:
        if stopper is not None:
            stopper.remove_done_callback(response_close_callback)


async def iter_jsonlines(
        content: aiohttp.StreamReader,
        chunk_size: int = 1024 * 1024,
) -> AsyncIterator[bytes]:
    """
    Iterate line by line over the response's content.

    Usage::

        async for line in _iter_lines(response.content):
            pass

    This is an equivalent of::

        async for line in response.content:
            pass

    Except that the aiohttp's line iteration fails if the accumulated buffer
    length is above 2**17 bytes, i.e. 128 KB (`aiohttp.streams.DEFAULT_LIMIT`
    for the buffer's low-watermark, multiplied by 2 for the high-watermark).
    Kubernetes secrets and other fields can be much longer, up to MBs in length.

    The chunk size of 1MB is an empirical guess for keeping the memory footprint
    reasonably low on huge amount of small lines (limited to 1 MB in total),
    while ensuring the near-instant reads of the huge lines (can be a problem
    with a small chunk size due to too many iterations).

    .. seealso::
        https://github.com/zalando-incubator/kopf/issues/275
    """

    # Minimize the memory footprint by keeping at most 2 copies of a yielded line in memory
    # (in the buffer and as a yielded value), and at most 1 copy of other lines (in the buffer).
    buffer = b''
    async for data in content.iter_chunked(chunk_size):
        buffer += data
        del data

        start = 0
        index = buffer.find(b'\n', start)
        while index >= 0:
            line = buffer[start:index]
            if line:
                yield line
            del line
            start = index + 1
            index = buffer.find(b'\n', start)

        if start > 0:
            buffer = buffer[start:]

    if buffer:
        yield buffer
