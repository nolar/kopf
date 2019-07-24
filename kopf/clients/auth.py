import functools
import ssl
import warnings
from contextvars import ContextVar
from typing import Optional, Callable, Any, TypeVar, Dict, cast

import aiohttp

from kopf.structs import credentials

# Per-operator storage and exchange point for authentication methods.
# Used by the client wrappers to retrieve the credentials and report the failures.
# Set by `spawn_tasks`, so that every operator's task has the same vault.
vault_var: ContextVar[credentials.Vault] = ContextVar('vault_var')

# A typevar to show that we return a function with the same signature as given.
_F = TypeVar('_F', bound=Callable[..., Any])


def reauthenticated_request(fn: _F) -> _F:
    """
    A client-specific decorator to re-authenticate a one-time request.

    If a wrapped function fails on the authentication, this will be reported
    back to the credentials container, which will trigger the re-authentication
    activity. Meanwhile, the request-performing function will be awaiting
    for the new credentials, and re-executed once they are available.
    """
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        vault: credentials.Vault = vault_var.get()
        async for key, info, session in vault.extended(APISession.from_connection_info, 'sessions'):
            try:
                return await fn(*args, **kwargs, session=session)
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    await vault.invalidate(key, exc=e)
                else:
                    raise
        else:
            raise credentials.LoginError("Ran out of connection credentials.")
    return cast(_F, wrapper)


def reauthenticated_stream(fn: _F) -> _F:
    """
    A client-specific decorator to re-authenticate an iterator-generator.

    If a wrapped function fails on the authentication, this will be reported
    back to the credentials source, which will trigger the re-authentication
    activity. Meanwhile, the function will be awaiting for the new credentials,
    and re-executed once they are available.
    """
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        vault: credentials.Vault = vault_var.get()
        async for key, info, session in vault.extended(APISession.from_connection_info, 'sessions'):
            try:
                async for item in fn(*args, **kwargs, session=session):
                    yield item
                break  # out of credentials cycle (instead of `return`)
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    await vault.invalidate(key, exc=e)
                else:
                    raise
        else:
            raise credentials.LoginError("Ran out of connection credentials.")
    return cast(_F, wrapper)


class APISession(aiohttp.ClientSession):
    """
    An extended aiohttp session, with k8s scopes for server & namespace scopes.

    It is constructed once per every `ConnectionInfo`, and then cached
    for later re-use (see `Vault.extended`).

    We assume that the whole operator runs in the same event loop, so there is
    no need to split the sessions for multiple loops. Synchronous handlers are
    threaded with other event loops per thread, but no operator's requests are
    performed inside of those threads: everything is in the main thread/loop.
    """
    server: str
    default_namespace: Optional[str] = None

    @classmethod
    def from_connection_info(
            cls,
            info: credentials.ConnectionInfo,
    ) -> "APISession":

        # The SSL part (both client certificate auth and CA verification).
        # TODO:2: also use cert/pkey/ca binary data
        context: ssl.SSLContext
        if info.certificate_path and info.private_key_path:
            context = ssl.create_default_context(
                cafile=info.ca_path,
                purpose=ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(
                certfile=info.certificate_path,
                keyfile=info.private_key_path)
        else:
            context = ssl.create_default_context(
                cafile=info.ca_path)
        if info.insecure:
            context.verify_mode = ssl.CERT_NONE
            context.check_hostname = False

        # The token auth part.
        headers: Dict[str, str] = {}
        if info.scheme and info.token:
            headers['Authorization'] = f'{info.scheme} {info.token}'
        elif info.scheme:
            headers['Authorization'] = f'{info.scheme}'
        elif info.token:
            headers['Authorization'] = f'Bearer {info.token}'

        # The basic auth part.
        auth: Optional[aiohttp.BasicAuth]
        if info.username and info.password:
            auth = aiohttp.BasicAuth(info.username, info.password)
        else:
            auth = None

        # It is a good practice to self-identify a bit.
        headers['User-Agent'] = f'kopf/unknown'  # TODO: add version someday

        # Generic aiohttp session based on the constructed credentials.
        session = cls(
            connector=aiohttp.TCPConnector(
                limit=0,
                ssl=context,
            ),
            headers=headers,
            auth=auth,
        )

        # Add the extra payload information. We avoid overriding the constructor.
        session.server = info.server
        session.default_namespace = info.default_namespace

        return session


# DEPRECATED: Should be removed with login()/get_pykube_cfg()/get_pykube_api().
# Previously, in some cases, get_pykube_cfg() was monkey-patched to inject
# custom authentication methods. Support these hacks as long as possible.
# See: piggybacking.login_via_pykube() for the usage of this monkey-patched function.
def get_pykube_cfg() -> Any:
    warnings.warn("get_pykube_cfg() is deprecated and unused.", DeprecationWarning)
    raise NotImplementedError("get_pykube_cfg() is not supported unless monkey-patched.")
