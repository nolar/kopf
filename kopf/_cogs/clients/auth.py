import base64
import functools
import os
import ssl
import tempfile
from contextvars import ContextVar
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, TypeVar, cast

import aiohttp

from kopf._cogs.clients import errors
from kopf._cogs.helpers import versions
from kopf._cogs.configs import configuration
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
            except errors.APIUnauthorizedError as e:
                await vault.invalidate(key, exc=e)

        # Normally, either `vault.extended()` or `vault.invalidate()` raise the login errors.
        # The for-cycle can only end if the yielded credentials are not invalidated before trying
        # the next ones -- but this case exits by `return` or by other (non-401) errors.
        raise RuntimeError("Reached an impossible state: the end of the authentication cycle.")

    return cast(_F, wrapper)


class APIContext:
    """
    A container for an aiohttp session and the caches of the environment info.

    The container is constructed only once for every `ConnectionInfo`,
    and then cached for later re-use (see `Vault.extended`).

    We assume that the whole operator runs in the same event loop, so there is
    no need to split the sessions for multiple loops. Synchronous handlers are
    threaded with other event loops per thread, but no operator's requests are
    performed inside of those threads: everything is in the main thread/loop.
    """

    # The main contained object used by the API methods.
    session: aiohttp.ClientSession

    # Contextual information for URL building.
    server: str
    default_namespace: Optional[str]

    # List of open responses.
    responses: List[aiohttp.ClientResponse]

    # Temporary caches of the information retrieved for and from the environment.
    _tempfiles: "_TempFiles"

    def __init__(
            self,
            info: credentials.ConnectionInfo,
    ) -> None:
        super().__init__()

        # Some SSL data are not accepted directly, so we have to use temp files.
        tempfiles = _TempFiles()
        ca_path: Optional[str]
        certificate_path: Optional[str]
        private_key_path: Optional[str]

        settings = configuration.OperatorSettings()

        if info.ca_path and info.ca_data:
            raise credentials.LoginError("Both CA path & data are set. Need only one.")
        elif info.ca_path:
            ca_path = info.ca_path
        elif info.ca_data:
            ca_path = tempfiles[base64.b64decode(info.ca_data)]
        else:
            ca_path = None

        if info.certificate_path and info.certificate_data:
            raise credentials.LoginError("Both certificate path & data are set. Need only one.")
        elif info.certificate_path:
            certificate_path = info.certificate_path
        elif info.certificate_data:
            certificate_path = tempfiles[base64.b64decode(info.certificate_data)]
        else:
            certificate_path = None

        if info.private_key_path and info.private_key_data:
            raise credentials.LoginError("Both private key path & data are set. Need only one.")
        elif info.private_key_path:
            private_key_path = info.private_key_path
        elif info.private_key_data:
            private_key_path = tempfiles[base64.b64decode(info.private_key_data)]
        else:
            private_key_path = None

        # The SSL part (both client certificate auth and CA verification).
        context: ssl.SSLContext
        if certificate_path and private_key_path:
            context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=ca_path)
            context.load_cert_chain(
                certfile=certificate_path,
                keyfile=private_key_path)
        else:
            context = ssl.create_default_context(
                cafile=ca_path)

        if info.insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

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
        headers['User-Agent'] = f'kopf/{versions.version or "unknown"}'

        # Generic aiohttp session based on the constructed credentials.
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=0,
                ssl=context,
            ),
            headers=headers,
            auth=auth,
            timeout=aiohttp.ClientTimeout(
                total=settings.session.total_timeout,
                sock_connect=settings.session.sock_connect_timeout,
                sock_read=settings.session.sock_read_timeout,
                connect=settings.session.connect_timeout
            ),
        )

        # Add the extra payload information. We avoid overriding the constructor.
        self.server = info.server
        self.default_namespace = info.default_namespace

        self.responses = []

        # For purging on garbage collection.
        self._tempfiles = tempfiles

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

        # Additionally, explicitly remove any temporary files we have created.
        # They will be purged on garbage collection anyway, but it is better to make it sooner.
        self._tempfiles.purge()


class _TempFiles(Mapping[bytes, str]):
    """
    A container for the temporary files, which are purged on garbage collection.

    The files are purged when the container is garbage-collected. The container
    is garbage-collected when its parent `APISession` is garbage-collected or
    explicitly closed (by `Vault` on removal of corresponding credentials).
    """

    def __init__(self) -> None:
        super().__init__()
        self._paths: Dict[bytes, str] = {}

    def __del__(self) -> None:
        self.purge()

    def __len__(self) -> int:
        return len(self._paths)

    def __iter__(self) -> Iterator[bytes]:
        return iter(self._paths)

    def __getitem__(self, item: bytes) -> str:
        if item not in self._paths:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(item)
            self._paths[item] = f.name
        return self._paths[item]

    def purge(self) -> None:
        for _, path in self._paths.items():
            try:
                os.remove(path)
            except OSError:
                pass  # already removed
        self._paths.clear()
