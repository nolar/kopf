"""
Rudimentary piggybacking on the known K8s API clients for authentication.

Kopf is not a client library, and avoids bringing too much logic
for proper authentication, especially all the complex auth-providers.

Instead, it uses the existing clients, triggers the (re-)authentication
in them, and extracts the basic credentials for its own use.

.. seealso::
    :mod:`credentials` and :func:`authentication`.
"""
import logging
from typing import Any, Optional, Sequence, Union

from kopf.structs import credentials

# Keep as constants to make them patchable. Higher priority is more preferred.
PRIORITY_OF_CLIENT: int = 10
PRIORITY_OF_PYKUBE: int = 20


# We keep the official client library auto-login only because it was
# an implied behavior before switching to pykube -- to keep it so (implied).
def login_via_client(
        *args: Any,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        **kwargs: Any,
) -> Optional[credentials.ConnectionInfo]:

    try:
        import kubernetes.config
    except ImportError:
        return None

    try:
        kubernetes.config.load_incluster_config()  # cluster env vars
        logger.debug("Client is configured in cluster with service account.")
    except kubernetes.config.ConfigException as e1:
        try:
            kubernetes.config.load_kube_config()  # developer's config files
            logger.debug("Client is configured via kubeconfig file.")
        except kubernetes.config.ConfigException as e2:
            raise credentials.LoginError(f"Cannot authenticate client neither in-cluster, nor via kubeconfig.")

    # We do not even try to understand how it works and why. Just load it, and extract the results.
    # For kubernetes client >= 12.0.0 use the new 'get_default_copy' method
    if callable(getattr(kubernetes.client.Configuration, 'get_default_copy', None)):
        config = kubernetes.client.Configuration.get_default_copy()
    else:
        config = kubernetes.client.Configuration()

    # For auth-providers, this method is monkey-patched with the auth-provider's one.
    # We need the actual auth-provider's token, so we call it instead of accessing api_key.
    # Other keys (token, tokenFile) also end up being retrieved via this method.
    header: Optional[str] = config.get_api_key_with_prefix('authorization')
    parts: Sequence[str] = header.split(' ', 1) if header else []
    scheme, token = ((None, None) if len(parts) == 0 else
                     (None, parts[0]) if len(parts) == 1 else
                     (parts[0], parts[1]))  # RFC-7235, Appendix C.

    # Interpret the config object for our own minimalistic credentials.
    # Note: kubernetes client has no concept of a "current" context's namespace.
    return credentials.ConnectionInfo(
        server=config.host,
        ca_path=config.ssl_ca_cert,  # can be a temporary file
        insecure=not config.verify_ssl,
        username=config.username or None,  # an empty string when not defined
        password=config.password or None,  # an empty string when not defined
        scheme=scheme,
        token=token,
        certificate_path=config.cert_file,  # can be a temporary file
        private_key_path=config.key_file,  # can be a temporary file
        priority=PRIORITY_OF_CLIENT,
    )


# Pykube login is mandatory. If it fails, the framework will not run at all.
def login_via_pykube(
        *args: Any,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        **kwargs: Any,
) -> Optional[credentials.ConnectionInfo]:

    try:
        import pykube
    except ImportError:
        return None

    # Read the pykube config either way for later interpretation.
    config: pykube.KubeConfig
    try:
        config = pykube.KubeConfig.from_service_account()
        logger.debug("Pykube is configured in cluster with service account.")
    except FileNotFoundError:
        try:
            config = pykube.KubeConfig.from_file()
            logger.debug("Pykube is configured via kubeconfig file.")
        except (pykube.PyKubeError, FileNotFoundError):
            raise credentials.LoginError(f"Cannot authenticate pykube "
                                         f"neither in-cluster, nor via kubeconfig.")

    # We don't know how this token will be retrieved, we just get it afterwards.
    provider_token = None
    if config.user.get('auth-provider'):
        api = pykube.HTTPClient(config)
        api.get(version='', base='/')  # ignore the response status
        provider_token = config.user.get('auth-provider', {}).get('config', {}).get('access-token')

    # Interpret the config object for our own minimalistic credentials.
    ca: Optional[pykube.config.BytesOrFile] = config.cluster.get('certificate-authority')
    cert: Optional[pykube.config.BytesOrFile] = config.user.get('client-certificate')
    pkey: Optional[pykube.config.BytesOrFile] = config.user.get('client-key')
    return credentials.ConnectionInfo(
        server=config.cluster.get('server'),
        ca_path=ca.filename() if ca else None,  # can be a temporary file
        insecure=config.cluster.get('insecure-skip-tls-verify'),
        username=config.user.get('username'),
        password=config.user.get('password'),
        token=config.user.get('token') or provider_token,
        certificate_path=cert.filename() if cert else None,  # can be a temporary file
        private_key_path=pkey.filename() if pkey else None,  # can be a temporary file
        default_namespace=config.namespace,
        priority=PRIORITY_OF_PYKUBE,
    )
