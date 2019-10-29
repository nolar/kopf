import functools
import logging
from contextvars import ContextVar
from typing import Optional, Callable, Any, TypeVar, Dict, cast

import pykube
import requests
import urllib3.exceptions

from kopf.structs import credentials

logger = logging.getLogger(__name__)


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


# Set in login(), consumed in get_pykube_cfg() and all API calls.
_pykube_cfg: Optional[pykube.KubeConfig] = None


def login(verify: bool = False) -> None:
    """
    Login to Kubernetes cluster, locally or remotely.

    Keep the logged in state or config object in the global variables,
    so that it can be available for future calls via the same function call.

    Automatic refresh/reload of the tokens or objects also should be done here.
    """

    # Pykube login is mandatory. If it fails, the framework will not run at all.
    try:
        import pykube
    except ImportError:
        raise  # mandatory
    else:
        login_pykube(verify=verify)

    # We keep the official client library auto-login only because it was
    # an implied behavior before switching to pykube -- to keep it so (implied).
    try:
        import kubernetes
    except ImportError:
        pass  # optional
    else:
        login_client(verify=verify)


def login_pykube(verify: bool = False) -> None:
    global _pykube_cfg
    try:
        _pykube_cfg = pykube.KubeConfig.from_service_account()
        logger.debug("Pykube is configured in cluster with service account.")
    except FileNotFoundError:
        try:
            _pykube_cfg = pykube.KubeConfig.from_file()
            logger.debug("Pykube is configured via kubeconfig file.")
        except (pykube.PyKubeError, FileNotFoundError):
            raise credentials.LoginError(f"Cannot authenticate pykube neither in-cluster, nor via kubeconfig.")

    if verify:
        verify_pykube()


def login_client(verify: bool = False) -> None:
    import kubernetes.client
    try:
        kubernetes.config.load_incluster_config()  # cluster env vars
        logger.debug("Client is configured in cluster with service account.")
    except kubernetes.config.ConfigException as e1:
        try:
            kubernetes.config.load_kube_config()  # developer's config files
            logger.debug("Client is configured via kubeconfig file.")
        except kubernetes.config.ConfigException as e2:
            raise credentials.LoginError(f"Cannot authenticate client neither in-cluster, nor via kubeconfig.")

    if verify:
        verify_client()


def verify_pykube() -> None:
    """
    Verify if login has succeeded, and the access configuration is still valid.

    All other errors (e.g. 403, 404) are ignored: it means, the host and port
    are configured and are reachable, the authentication token is accepted,
    and the rest are authorization or configuration errors (not a showstopper).
    """
    try:
        api = get_pykube_api()
        rsp = api.get(version="", base="/")
        rsp.raise_for_status()
        api.raise_for_status(rsp)  # replaces requests's HTTPError with its own.
    except requests.exceptions.ConnectionError as e:
        raise credentials.AccessError("Cannot connect to the Kubernetes API. "
                                      "Please configure the cluster access.")
    except pykube.exceptions.HTTPError as e:
        if e.code == 401:
            raise credentials.AccessError("Cannot authenticate to the Kubernetes API. "
                                          "Please login or configure the tokens.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise credentials.AccessError("Cannot authenticate to the Kubernetes API. "
                                          "Please login or configure the tokens.")


def verify_client() -> None:
    """
    Verify if login has succeeded, and the access configuration is still valid.

    All other errors (e.g. 403, 404) are ignored: it means, the host and port
    are configured and are reachable, the authentication token is accepted,
    and the rest are authorization or configuration errors (not a showstopper).
    """
    import kubernetes.client.rest
    try:
        api = kubernetes.client.CoreApi()
        api.get_api_versions()
    except urllib3.exceptions.HTTPError as e:
        raise credentials.AccessError("Cannot connect to the Kubernetes API. "
                                      "Please configure the cluster access.")
    except kubernetes.client.rest.ApiException as e:
        if e.status == 401:
            raise credentials.AccessError("Cannot authenticate to the Kubernetes API. "
                                          "Please login or configure the tokens.")


def get_pykube_cfg() -> pykube.KubeConfig:
    if _pykube_cfg is None:
        raise credentials.LoginError("Not logged in with PyKube.")
    return _pykube_cfg


# TODO: add some caching, but keep kwargs in mind. Maybe add a key= for purpose/use-place?
def get_pykube_api(
        timeout: Optional[float] = None,
) -> pykube.HTTPClient:
    kwargs = dict(timeout=timeout)
    return pykube.HTTPClient(get_pykube_cfg(), **kwargs)


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
