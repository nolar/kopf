"""
Rudimentary piggybacking on the known K8s API clients for authentication.

Kopf is not a client library, and avoids bringing too much logic
for proper authentication, especially all the complex auth-providers.

Instead, it uses the existing clients, triggers the (re-)authentication
in them, and extracts the basic credentials for its own use.

.. seealso::
    :mod:`credentials` and :func:`authentication`.
"""
import os
from typing import Any, Dict, Optional, Sequence

import yaml

from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import credentials

# Keep as constants to make them patchable. Higher priority is more preferred.
PRIORITY_OF_CLIENT: int = 10
PRIORITY_OF_PYKUBE: int = 20

# Rudimentary logins are added only if the clients are absent, so the priorities can overlap.
PRIORITY_OF_KUBECONFIG: int = 10
PRIORITY_OF_SERVICE_ACCOUNT: int = 20


def has_client() -> bool:
    try:
        import kubernetes
    except ImportError:
        return False
    else:
        return True


def has_pykube() -> bool:
    try:
        import pykube
    except ImportError:
        return False
    else:
        return True


# We keep the official client library auto-login only because it was
# an implied behavior before switching to pykube -- to keep it so (implied).
def login_via_client(
        *,
        logger: typedefs.Logger,
        **_: Any,
) -> Optional[credentials.ConnectionInfo]:

    # Keep imports in the function, as module imports are mocked in some tests.
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
            raise credentials.LoginError("Cannot authenticate the client library "
                                         "neither in-cluster, nor via kubeconfig.")

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


def login_via_pykube(
        *,
        logger: typedefs.Logger,
        **_: Any,
) -> Optional[credentials.ConnectionInfo]:

    # Keep imports in the function, as module imports are mocked in some tests.
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
            raise credentials.LoginError("Cannot authenticate pykube "
                                         "neither in-cluster, nor via kubeconfig.")

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


def has_service_account() -> bool:
    return os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token')


def login_with_service_account(**_: Any) -> Optional[credentials.ConnectionInfo]:
    """
    A minimalistic login handler that can get raw data from a service account.

    Authentication capabilities can be limited to keep the code short & simple.
    No parsing or sophisticated multi-step token retrieval is performed.

    This login function is intended to make Kopf runnable in trivial cases
    when neither pykube-ng nor the official client library are installed.
    """

    # As per https://kubernetes.io/docs/tasks/run-application/access-api-from-pod/
    token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    ca_path = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'

    if os.path.exists(token_path):
        with open(token_path, encoding='utf-8') as f:
            token = f.read().strip()

        namespace: Optional[str] = None
        if os.path.exists(ns_path):
            with open(ns_path, encoding='utf-8') as f:
                namespace = f.read().strip()

        return credentials.ConnectionInfo(
            server='https://kubernetes.default.svc',
            ca_path=ca_path if os.path.exists(ca_path) else None,
            token=token or None,
            default_namespace=namespace or None,
            priority=PRIORITY_OF_SERVICE_ACCOUNT,
        )
    else:
        return None


def has_kubeconfig() -> bool:
    env_var_set = bool(os.environ.get('KUBECONFIG'))
    file_exists = os.path.exists(os.path.expanduser('~/.kube/config'))
    return env_var_set or file_exists


def login_with_kubeconfig(**_: Any) -> Optional[credentials.ConnectionInfo]:
    """
    A minimalistic login handler that can get raw data from a kubeconfig file.

    Authentication capabilities can be limited to keep the code short & simple.
    No parsing or sophisticated multi-step token retrieval is performed.

    This login function is intended to make Kopf runnable in trivial cases
    when neither pykube-ng nor the official client library are installed.
    """

    # As per https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/
    kubeconfig = os.environ.get('KUBECONFIG')
    if not kubeconfig and os.path.exists(os.path.expanduser('~/.kube/config')):
        kubeconfig = '~/.kube/config'
    if not kubeconfig:
        return None

    paths = [path.strip() for path in kubeconfig.split(os.pathsep)]
    paths = [os.path.expanduser(path) for path in paths if path]

    # As prescribed: if the file is absent or non-deserialisable, then fail. The first value wins.
    current_context: Optional[str] = None
    contexts: Dict[Any, Any] = {}
    clusters: Dict[Any, Any] = {}
    users: Dict[Any, Any] = {}
    for path in paths:

        with open(path, encoding='utf-8') as f:
            config = yaml.safe_load(f.read()) or {}

        if current_context is None:
            current_context = config.get('current-context')
        for item in config.get('contexts', []):
            if item['name'] not in contexts:
                contexts[item['name']] = item.get('context') or {}
        for item in config.get('clusters', []):
            if item['name'] not in clusters:
                clusters[item['name']] = item.get('cluster') or {}
        for item in config.get('users', []):
            if item['name'] not in users:
                users[item['name']] = item.get('user') or {}

    # Once fully parsed, use the current context only.
    if current_context is None:
        raise credentials.LoginError('Current context is not set in kubeconfigs.')
    context = contexts[current_context]
    cluster = clusters[context['cluster']]
    user = users[context['user']]

    # Unlike pykube's login, we do not make a fake API request to refresh the token.
    provider_token = user.get('auth-provider', {}).get('config', {}).get('access-token')

    # Map the retrieved fields into the credentials object.
    return credentials.ConnectionInfo(
        server=cluster.get('server'),
        ca_path=cluster.get('certificate-authority'),
        ca_data=cluster.get('certificate-authority-data'),
        insecure=cluster.get('insecure-skip-tls-verify'),
        certificate_path=user.get('client-certificate'),
        certificate_data=user.get('client-certificate-data'),
        private_key_path=user.get('client-key'),
        private_key_data=user.get('client-key-data'),
        username=user.get('username'),
        password=user.get('password'),
        token=user.get('token') or provider_token,
        default_namespace=context.get('namespace'),
        priority=PRIORITY_OF_KUBECONFIG,
    )
