import base64
import contextlib
import functools
import os
import ssl
import tempfile
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, TypeVar, cast

import aiohttp

from kopf._cogs.clients import errors
from kopf._cogs.helpers import versions
from kopf._cogs.structs import credentials

# Per-operator storage and exchange point for authentication methods.
# Used by the client wrappers to retrieve the credentials and report the failures.
# Set by `spawn_tasks`, so that every operator's task has the same vault.
vault_var: ContextVar[credentials.Vault] = ContextVar('vault_var')

# A typevar to show that we return a function with the same signature as given.
_F = TypeVar('_F', bound=Callable[..., Any])


def authenticated(fn: _F) -> _F:
    """
    A decorator to inject a pre-authenticated session to a requesting routine.

    If a wrapped function fails on the authentication, this will be reported
    back to the credentials vault, which will trigger the re-authentication
    activity. Meanwhile, the request-performing function will be awaiting
    for the new credentials, and re-executed once they are available.
    """
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:

        # If a context is explicitly passed, make it a simple call without re-auth.
        # Exceptions are escalated to a caller, which is probably wrapped itself.
        if 'context' in kwargs:
            context = kwargs['context']
            response = await fn(*args, **kwargs)
            if isinstance(response, aiohttp.ClientResponse):
                # Keep track of responses which are using this context.
                context.add_response(response)
            return response

        # Otherwise, attempt the execution with the vault credentials and re-authenticate on 401s.
        vault: credentials.Vault = vault_var.get()
        async for key, info, context in vault.extended(APIContext, 'contexts'):
            try:
                response = await fn(*args, **kwargs, context=context)
                if isinstance(response, aiohttp.ClientResponse):
                    # Keep track of responses which are using this context.
                    context.add_response(response)
                return response
            except (errors.APIUnauthorizedError, errors.APISessionClosed) as e:
                await vault.invalidate(key, info, exc=e)

        # Normally, either `vault.extended()` or `vault.invalidate()` raise the login errors.
        # The for-cycle can only end if the yielded credentials are not invalidated before trying
        # the next ones -- but this case exits by `return` or by other (non-401) errors.
        raise RuntimeError("Reached an impossible state: the end of the authentication cycle.")

    return cast(_F, wrapper)


class APIContext:
    """
    A container for an aiohttp session and the caches of the environment info.

    The container is constructed only once for every :class:`KubeContext`,
    and then cached for later re-use (see :meth:`Vault.extended`).

    We assume that the whole operator runs in the same event loop, so there is
    no need to split the sessions for multiple loops. Synchronous handlers are
    threaded with other event loops per thread, but no operator's requests are
    performed inside of those threads: everything is in the main thread/loop.
    """

    # The main contained object used by the API methods.
    session: aiohttp.ClientSession

    # Contextual information for URL building.
    server: str
    default_namespace: str | None

    # List of open responses.
    responses: list[aiohttp.ClientResponse]

    def __init__(
            self,
            info: credentials.KubeContext,
    ) -> None:
        super().__init__()

        # Generic aiohttp session based on the constructed credentials.
        match info:
            case credentials.ConnectionInfo():
                self.session = self.make_aiohttp_session(info)
            case credentials.AiohttpSession():
                self.session = info.aiohttp_session
            case _:
                raise TypeError(f"Unsupported credentials type: {info!r}")

        # It is a good practice to self-identify a bit, even with a user-provided session.
        if self.session.headers.get('User-Agent') is None:
            self.session.headers['User-Agent'] = f'kopf/{versions.version or "unknown"}'

        # Add the extra payload information. We avoid overriding the constructor.
        self.server = info.server
        self.default_namespace = info.default_namespace

        self.responses = []

    def make_aiohttp_session(self, info: credentials.ConnectionInfo) -> aiohttp.ClientSession:

        # Some SSL data are not accepted directly, so we have to use temp files.
        # Do not even create temporary files if there is no need. It can be a readonly filesystem.
        with contextlib.ExitStack() as stack:

            cert_path: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None
            if info.certificate_path:
                cert_path = info.certificate_path
            elif info.certificate_data:
                cert_file = stack.enter_context(tempfile.NamedTemporaryFile(buffering=0))
                cert_file.write(decode_to_pem(info.certificate_data).encode('ascii'))
                cert_path = cert_file.name
            else:
                cert_path = None

            pkey_path: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None
            if info.private_key_path:
                pkey_path = info.private_key_path
            elif info.private_key_data:
                pkey_file = stack.enter_context(tempfile.NamedTemporaryFile(buffering=0))
                pkey_file.write(decode_to_pem(info.private_key_data).encode('ascii'))
                pkey_path = pkey_file.name
            else:
                pkey_path = None

            # The SSL part (both client certificate auth and CA verification).
            context: ssl.SSLContext
            if cert_path and pkey_path:
                context = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH,
                    cafile=info.ca_path,
                    cadata=decode_to_pem(info.ca_data) if info.ca_data is not None else None,
                )
                context.load_cert_chain(certfile=cert_path, keyfile=pkey_path)
            else:
                context = ssl.create_default_context(
                    cafile=info.ca_path,
                    cadata=decode_to_pem(info.ca_data) if info.ca_data is not None else None,
                )

        if info.insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        # The token auth part.
        headers: dict[str, str] = {}
        if info.scheme and info.token:
            headers['Authorization'] = f'{info.scheme} {info.token}'
        elif info.scheme:
            headers['Authorization'] = f'{info.scheme}'
        elif info.token:
            headers['Authorization'] = f'Bearer {info.token}'

        # The basic auth part.
        auth: aiohttp.BasicAuth | None
        if info.username and info.password:
            auth = aiohttp.BasicAuth(info.username, info.password)
        else:
            auth = None

        return aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=0,
                ssl=context,
            ),
            headers=headers,
            auth=auth,
        )

    def flush_closed_responses(self) -> None:
        # There's no point keeping references to already closed responses.
        self.responses[:] = [_response for _response in self.responses if not _response.closed]

    def add_response(self, response: aiohttp.ClientResponse) -> None:
        # Keep track of responses so they can be closed later when the session
        # is closed.
        self.flush_closed_responses()
        if not response.closed:
            self.responses.append(response)

    def close_open_responses(self) -> None:
        # Close all responses that are still open and are using this session.
        for response in self.responses:
            if not response.closed:
                response.close()
        self.responses.clear()

    async def close(self) -> None:
        # Close all open responses that use this session before closing the session itself.
        self.close_open_responses()

        # Closing is triggered by `Vault._flush_caches()` -- forward it to the actual session.
        await self.session.close()


def decode_to_pem(data: str | bytes) -> str:
    match data:
        case str() if data.startswith('-----BEGIN '):
            return data
        case bytes() if data.startswith(b'-----BEGIN '):
            return data.decode('ascii')
        case _:
            return base64.b64decode(data).decode('ascii')
