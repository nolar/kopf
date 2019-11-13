import functools
import warnings
from contextvars import ContextVar
from typing import Optional, Callable, Any, TypeVar, Dict, cast

import pykube
import requests

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
    factory = create_pykube_client
    purpose = f'pykube-client-with-defaults'
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        vault: credentials.Vault = vault_var.get()
        async for key, info, api in vault.extended(factory=factory, purpose=purpose):
            try:
                return await fn(*args, **kwargs, api=api)
            except pykube.exceptions.HTTPError as e:
                if e.code == 401:
                    await vault.invalidate(key, exc=e)
                else:
                    raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
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
    factory = functools.partial(create_pykube_client, timeout=None)
    purpose = f'pykube-client-no-timeout'
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        vault: credentials.Vault = vault_var.get()
        async for key, info, api in vault.extended(factory=factory, purpose=purpose):
            try:
                async for item in fn(*args, **kwargs, api=api):
                    yield item
                break  # out of credentials cycle (instead of `return`)
            except pykube.exceptions.HTTPError as e:
                if e.code == 401:
                    await vault.invalidate(key, exc=e)
                else:
                    raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    await vault.invalidate(key, exc=e)
                else:
                    raise
        else:
            raise credentials.LoginError("Ran out of connection credentials.")
    return cast(_F, wrapper)


# DEPRECATED: Should be removed with login()/get_pykube_cfg()/get_pykube_api().
# Previously, in some cases, get_pykube_cfg() was monkey-patched to inject
# custom authentication methods. Support these hacks as long as possible.
# See: piggybacking.login_via_pykube() for the usage of this monkey-patched function.
def get_pykube_cfg() -> Any:
    warnings.warn("get_pykube_cfg() is deprecated and unused.", DeprecationWarning)
    raise NotImplementedError("get_pykube_cfg() is not supported unless monkey-patched.")


def create_pykube_client(
        info: credentials.ConnectionInfo,
        **client_kwargs: Any,
) -> pykube.HTTPClient:
    return pykube.HTTPClient(create_pykube_config(info), **client_kwargs)


def create_pykube_config(
        info: credentials.ConnectionInfo,
) -> pykube.KubeConfig:
    return pykube.KubeConfig({
        "current-context": "self",
        "contexts": [{
            "name": "self",
            "context": _clean_dict({
                "cluster": "self",
                "user": "self",
                "namespace": info.default_namespace,
            }),
        }],
        "clusters": [{
            "name": "self",
            "cluster": _clean_dict({
                "server": info.server,
                "insecure-skip-tls-verify": info.insecure,
                "certificate-authority": info.ca_path,
                "certificate-authority-data": info.ca_data,
            }),
        }],
        "users": [{
            "name": "self",
            "user": _clean_dict({
                "token": info.token,
                "username": info.username,
                "password": info.password,
                "client-certificate": info.certificate_path,
                "client-certificate-data": info.certificate_data,
                "client-key": info.private_key_path,
                "client-key-data": info.private_key_data,
            }),
        }],
    })


def _clean_dict(d: Dict[str, Optional[object]]) -> Dict[str, object]:
    return {key: val for key, val in d.items() if val is not None}
