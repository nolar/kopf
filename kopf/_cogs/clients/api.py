import asyncio
import collections.abc
import itertools
import json
import ssl
import urllib.parse
from collections.abc import AsyncIterator, Mapping
from typing import Any

import aiohttp

from kopf._cogs.aiokits import aiotasks
from kopf._cogs.clients import auth, errors
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs


@auth.authenticated
async def get_default_namespace(
        *,
        context: auth.APIContext | None = None,
) -> str | None:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")
    return context.default_namespace


@auth.authenticated
async def read_sslcert(
        *,
        context: auth.APIContext | None = None,
) -> tuple[str, bytes]:
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        context: auth.APIContext | None = None,  # injected by the decorator
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
    backoff: float | None
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

        # aiohttp raises a generic error if the session/transport is closed, so we try to guess.
        # NB: "session closed" will reset the retry counter and do the full cycle with the new creds.
        except RuntimeError as e:
            if context.session.closed:
                # TODO: find a way to gracefully replace the active session in the existing context,
                #       so that all ongoing requests would switch to the new session & credentials.
                logger.error(f"Request attempt {idx} failed; TCP closed; will re-authenticate: {what}")
                raise errors.APISessionClosed("Session is closed.") from e
            raise

        # NOTE(vsaienko): during k8s upgrade API might throw 403 forbidden. Use retries for this exception as well.
        except (aiohttp.ClientConnectionError, errors.APIServerError, asyncio.TimeoutError, errors.APIForbiddenError) as e:
            if '[SSL: APPLICATION_DATA_AFTER_CLOSE_NOTIFY]' in str(e):  # for ClientOSError
                logger.error(f"Request attempt {idx} failed; SSL closed; will re-authenticate: {what}")
                raise errors.APISessionClosed("SSL data stream is closed.") from e
            elif backoff is None:  # i.e. the last or the only attempt.
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
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
        payload: object | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        stopper: aiotasks.Future | None = None,
        logger: typedefs.Logger,
) -> AsyncIterator[Any]:
    # This dirty trickery is for cases when the server thinks too slowly before
    # sending the headers, but the stopper is already set during the initial wait.
    def request_cancel_callback(_: aiotasks.Future) -> None:
        task = asyncio.current_task()
        assert task is not None  # for type-checkers; this is `async def`, so always in a task.
        task.cancel()

    if stopper is not None and not stopper.done():
        stopper.add_done_callback(request_cancel_callback)
    try:
        response = await request(
            method='get',
            url=url,
            payload=payload,
            headers=headers,
            timeout=timeout,
            settings=settings,
            logger=logger,
        )
    except asyncio.CancelledError:
        if stopper is not None and stopper.done():
            return
        else:
            raise  # triggered not by the stopper, escalate
    finally:
        if stopper is not None:
            stopper.remove_done_callback(request_cancel_callback)

    # Do not proceed if managed to avoid the cancellation somehow, but got here.
    if stopper is not None and stopper.done():
        response.close()
        return

    # Once the headers were sent & received, the stopper works slightly differently:
    # it closes the response instead of cancelling the already performed request.
    def response_close_callback(_: aiotasks.Future) -> None:
        response.close()

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
            raise  # triggered not by the stopper, escalate
    finally:
        if stopper is not None:
            stopper.remove_done_callback(response_close_callback)


async def iter_jsonlines(
        content: aiohttp.StreamReader,
        chunk_size: int = 1024 * 1024,
) -> AsyncIterator[bytes]:
    """
    Iterate line by line over the response's content.

    Usage:

    .. code-block:: python

        async for line in _iter_lines(response.content):
            pass

    This is an equivalent of:

    .. code-block:: python

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
