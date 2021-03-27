"""
Several webhooks servers & tunnels supported out of the box.
"""
import asyncio
import base64
import contextlib
import functools
import ipaddress
import json
import logging
import os
import pathlib
import socket
import ssl
import tempfile
import urllib.parse
from typing import TYPE_CHECKING, AsyncIterator, Collection, Dict, Iterable, Optional, Tuple, Union

import aiohttp.web

from kopf.clients import scanning
from kopf.reactor import admission
from kopf.structs import reviews

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    StrPath = Union[str, os.PathLike[str]]
else:
    StrPath = Union[str, os.PathLike]


class MissingDependencyError(ImportError):
    """ A server/tunnel is used which requires an optional dependency. """


class WebhookServer:
    """
    A local HTTP/HTTPS endpoint.

    Currently, the server is based on ``aiohttp``, but the implementation
    can change in the future without warning.

    This server is also used by specialised tunnels when they need
    a local endpoint to be tunneled.

    * ``addr``, ``port`` is where to listen for connections
      (defaults to ``localhost`` and ``9443``).
    * ``path`` is the root path for a webhook server
      (defaults to no root path).
    * ``host`` is an optional override of the hostname for webhook URLs;
      if not specified, the ``addr`` will be used.

    Kubernetes requires HTTPS, so HTTPS is the default mode of the server.
    This webhook server supports SSL both for the server certificates
    and for client certificates (e.g., for authentication) at the same time:

    * ``cadata``, ``cafile`` is the CA bundle to be passed as a "client config"
      to the webhook configuration objects, to be used by clients/apiservers
      when talking to the webhook server; it is not used in the server itself.
    * ``cadump`` is a path to save the resulting CA bundle to be used
      by clients, i.e. apiservers; it can be passed to ``curl --cacert ...``;
      if ``cafile`` is provided, it contains the same content.
    * ``certfile``, ``pkeyfile`` define the server's endpoint certificate;
      if not specified, a self-signed certificate and CA will be generated
      for both ``addr`` & ``host`` as SANs (but only ``host`` for CommonName).
    * ``password`` is either for decrypting the provided ``pkeyfile``,
      or for encrypting and decrypting the generated private key.
    * ``extra_sans`` are put into the self-signed certificate as SANs (DNS/IP)
      in addition to the host & addr (in case some other endpoints exist).
    * ``verify_mode``, ``verify_cafile``, ``verify_capath``, ``verify_cadata``
      will be loaded into the SSL context for verifying the client certificates
      when provided and if provided by the clients, i.e. apiservers or curl;
      (`ssl.SSLContext.verify_mode`, `ssl.SSLContext.load_verify_locations`).
    * ``insecure`` flag disables HTTPS and runs an HTTP webhook server.
      This is used in ngrok for a local endpoint, but can be used for debugging
      or when the certificate-generating dependencies/extras are not installed.
    """
    DEFAULT_HOST: Optional[str] = None

    addr: Optional[str]  # None means "all interfaces"
    port: Optional[int]  # None means random port
    host: Optional[str]
    path: Optional[str]

    cadata: Optional[bytes]  # -> .webhooks.*.clientConfig.caBundle
    cafile: Optional[StrPath]
    cadump: Optional[StrPath]

    context: Optional[ssl.SSLContext]
    insecure: bool
    certfile: Optional[StrPath]
    pkeyfile: Optional[StrPath]
    password: Optional[str]

    extra_sans: Iterable[str]

    verify_mode: Optional[ssl.VerifyMode]
    verify_cafile: Optional[StrPath]
    verify_capath: Optional[StrPath]
    verify_cadata: Optional[Union[str, bytes]]

    def __init__(
            self,
            *,
            # Listening socket, root URL path, and the reported URL hostname:
            addr: Optional[str] = None,
            port: Optional[int] = None,
            path: Optional[str] = None,
            host: Optional[str] = None,
            # The CA bundle to be passed to "client configs":
            cadata: Optional[bytes] = None,
            cafile: Optional[StrPath] = None,
            cadump: Optional[StrPath] = None,
            # A pre-configured SSL context (if any):
            context: Optional[ssl.SSLContext] = None,
            # The server's own certificate, or lack of it (loaded into the context):
            insecure: bool = False,  # http is needed for ngrok
            certfile: Optional[StrPath] = None,
            pkeyfile: Optional[StrPath] = None,
            password: Optional[str] = None,
            # Generated certificate's extra info.
            extra_sans: Iterable[str] = (),
            # Verification of client certificates (loaded into the context):
            verify_mode: Optional[ssl.VerifyMode] = None,
            verify_cafile: Optional[StrPath] = None,
            verify_capath: Optional[StrPath] = None,
            verify_cadata: Optional[Union[str, bytes]] = None,
    ) -> None:
        super().__init__()
        self.addr = addr
        self.port = port
        self.path = path
        self.host = host
        self.cadata = cadata
        self.cafile = cafile
        self.cadump = cadump
        self.context = context
        self.insecure = insecure
        self.certfile = certfile
        self.pkeyfile = pkeyfile
        self.password = password
        self.extra_sans = extra_sans
        self.verify_mode = verify_mode
        self.verify_cafile = verify_cafile
        self.verify_capath = verify_capath
        self.verify_cadata = verify_cadata

    async def __call__(self, fn: reviews.WebhookFn) -> AsyncIterator[reviews.WebhookClientConfig]:

        # Redefine as a coroutine instead of a partial to avoid warnings from aiohttp.
        async def _serve_fn(request: aiohttp.web.Request) -> aiohttp.web.Response:
            return await self._serve(fn, request)

        cadata, context = self._build_ssl()
        path = self.path.rstrip('/') if self.path else ''
        app = aiohttp.web.Application()
        app.add_routes([aiohttp.web.post(f"{path}/{{id:.*}}", _serve_fn)])
        runner = aiohttp.web.AppRunner(app, handle_signals=False)
        await runner.setup()
        try:
            addr = self.addr or None  # None is aiohttp's "any interface"
            port = self.port or self._allocate_free_port()
            site = aiohttp.web.TCPSite(runner, addr, port, ssl_context=context)
            await site.start()

            # Log with the actual URL: normalised, with hostname/port set.
            schema = 'http' if context is None else 'https'
            url = self._build_url(schema, addr or '*', port, self.path or '')
            logger.debug(f"Listening for webhooks at {url}")
            host = self.host or self.DEFAULT_HOST or self._get_accessible_addr(self.addr)
            url = self._build_url(schema, host, port, self.path or '')
            logger.debug(f"Accessing the webhooks at {url}")

            client_config = reviews.WebhookClientConfig(url=url)
            if cadata is not None:
                client_config['caBundle'] = base64.b64encode(cadata).decode('ascii')

            yield client_config
            await asyncio.Event().wait()
        finally:
            # On any reason of exit, stop serving the endpoint.
            await runner.cleanup()

    @staticmethod
    async def _serve(
            fn: reviews.WebhookFn,
            request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        """
        Serve a single admission request: an aiohttp-specific implementation.

        Mind 2 different ways the errors are reported:

        * Directly by the webhook's response, i.e. to the apiservers.
          This means that the webhook request was done improperly;
          the original API request might be good, but we could not confirm that.
        * In ``.response.status``, as apiservers send it to the requesting user.
          This means that the original API operation was done improperly,
          while the webhooks are functional.
        """
        # The extra information that is passed down to handlers for authentication/authorization.
        # Note: this is an identity of an apiserver, not of the user that sends an API request.
        headers = dict(request.headers)
        sslpeer = request.transport.get_extra_info('peercert') if request.transport else None
        webhook = request.match_info.get('id')
        try:
            text = await request.text()
            data = json.loads(text)
            response = await fn(data, webhook=webhook, sslpeer=sslpeer, headers=headers)
            return aiohttp.web.json_response(response)
        except admission.AmbiguousResourceError as e:
            raise aiohttp.web.HTTPConflict(reason=str(e))
        except admission.UnknownResourceError as e:
            raise aiohttp.web.HTTPNotFound(reason=str(e))
        except admission.WebhookError as e:
            raise aiohttp.web.HTTPBadRequest(reason=str(e))
        except json.JSONDecodeError as e:
            raise aiohttp.web.HTTPBadRequest(reason=str(e))

    @staticmethod
    def _allocate_free_port() -> int:
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', 0))  # '' is a special IPv4 form for "any interface"
            return int(s.getsockname()[1])

    @staticmethod
    def _get_accessible_addr(addr: Optional[str]) -> str:
        """
        Convert a "catch-all" listening address to the accessible hostname.

        "Catch-all" interfaces like `0.0.0.0` or `::/0` can be used
        for listening to utilise all interfaces, but cannot be accessed.
        Some other real ("specified") address must be used for that.

        If the address is not IPv4/IPv6 address or is a regular "specified"
        address, it is used as is. Only the special addressed are overridden.
        """
        if addr is None:
            return 'localhost'  # and let the system resolved it to IPv4/IPv6
        try:
            ipv4 = ipaddress.IPv4Address(addr)
        except ipaddress.AddressValueError:
            pass
        else:
            return '127.0.0.1' if ipv4.is_unspecified else addr
        try:
            ipv6 = ipaddress.IPv6Address(addr)
        except ipaddress.AddressValueError:
            pass
        else:
            return '::1' if ipv6.is_unspecified else addr
        return addr

    @staticmethod
    def _build_url(schema: str, host: str, port: int, path: str) -> str:
        try:
            ipv6 = ipaddress.IPv6Address(host)
        except ipaddress.AddressValueError:
            pass
        else:
            host = f'[{ipv6}]'
        is_default_port = ((schema == 'http' and port == 80) or
                           (schema == 'https' and port == 443))
        netloc = host if is_default_port else f'{host}:{port}'
        return urllib.parse.urlunsplit([schema, netloc, path, '', ''])

    def _build_ssl(self) -> Tuple[Optional[bytes], Optional[ssl.SSLContext]]:
        """
        A macros to construct an SSL context, possibly generating SSL certs.

        Returns a CA bundle to be passed to the "client configs",
        and a properly initialised SSL context to be used by the server.
        Or ``None`` for both if an HTTP server is needed.
        """
        cadata = self.cadata
        context = self.context
        if self.insecure and self.context is not None:
            raise ValueError("Insecure mode cannot have an SSL context specified.")

        # Read the provided CA bundle for webhooks' "client config"; not used by the server itself.
        if cadata is None and self.cafile is not None:
            cadata = pathlib.Path(self.cafile).read_bytes()

        # Kubernetes does not work with HTTP, so we do not bother and always run HTTPS too.
        # Except when explicitly said to be insecure, e.g. by ngrok (free plan only supports HTTP).
        if context is None and not self.insecure:
            context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)

        if context is not None:

            # Load a CA for verifying the client certificates (if provided) by this server.
            if self.verify_mode is not None:
                context.verify_mode = self.verify_mode
            if self.verify_cafile or self.verify_capath or self.verify_cadata:
                logger.debug("Loading a CA for client certificate verification.")
                context.load_verify_locations(
                    self.verify_cafile,
                    self.verify_capath,
                    self.verify_cadata,
                )
                if context.verify_mode == ssl.CERT_NONE:
                    context.verify_mode = ssl.CERT_OPTIONAL

            # Load the specified server's certificate, or generate a self-signed one if possible.
            # If cafile/cadata are not defined, use the server's certificate as a CA for clients.
            if self.certfile is not None and self.pkeyfile is not None:
                logger.debug("Using a provided certificate for HTTPS.")
                context.load_cert_chain(
                    self.certfile,
                    self.pkeyfile,
                    self.password,
                )
                if cadata is None and self.certfile is not None:
                    cadata = pathlib.Path(self.certfile).read_bytes()
            else:
                logger.debug("Generating a self-signed certificate for HTTPS.")
                host = self.host or self.DEFAULT_HOST
                addr = self._get_accessible_addr(self.addr)
                hostnames = [host or addr, addr] + list(self.extra_sans)
                certdata, pkeydata = self.build_certificate(hostnames, self.password)
                with tempfile.NamedTemporaryFile() as certf, tempfile.NamedTemporaryFile() as pkeyf:
                    certf.write(certdata)
                    pkeyf.write(pkeydata)
                    certf.flush()
                    pkeyf.flush()
                    context.load_cert_chain(certf.name, pkeyf.name, self.password)

                # For a self-signed certificate, the CA bundle is the certificate itself,
                # regardless of what cafile/cadata are provided from outside.
                cadata = certdata

        # Dump the provided or self-signed CA (but not the key!), e.g. for `curl --cacert ...`
        if self.cadump is not None and cadata is not None:
            pathlib.Path(self.cadump).write_bytes(cadata)

        return cadata, context

    @staticmethod
    def build_certificate(
            hostnames: Collection[str],
            password: Optional[str] = None,
    ) -> Tuple[bytes, bytes]:
        """
        Build a self-signed certificate with SANs (subject alternative names).

        Returns a tuple of the certificate and its private key (PEM-formatted).

        The certificate is "minimally sufficient", without much of the extra
        information on the subject besides its common and alternative names.
        However, IP addresses are properly recognised and normalised for better
        compatibility with strict SSL clients (like apiservers of Kubernetes).
        The first non-IP hostname becomes the certificate's common name --
        by convention, non-configurable. If no hostnames are found, the first
        IP address is used as a fallback. Magic IPs like 0.0.0.0 are excluded.

        ``certbuilder`` is used as an implementation because it is lightweight:
        2.9 MB vs. 8.7 MB for cryptography. Still, it is too heavy to include
        as a normal runtime dependency (for 8.8 MB of Kopf itself), so it is
        only available as the ``kopf[dev]`` extra for development-mode dependencies.
        This can change in the future if self-signed certificates become used
        at runtime (e.g. in production/staging environments or other real clusters).
        """
        try:
            import certbuilder
            import oscrypto.asymmetric
        except ImportError:
            raise MissingDependencyError(
                "Using self-signed certificates requires an extra dependency: "
                "run `pip install certbuilder` or `pip install kopf[dev]`. "
                "Or pass `insecure=True` to a webhook server to use only HTTP. "
                "Or generate your own certificates and pass as certfile=/pkeyfile=. "
                "More: https://kopf.readthedocs.io/en/stable/admission/")

        # Detect which ones of the hostnames are probably IPv4/IPv6 addresses.
        # A side-effect: bring them all to their canonical forms.
        parsed_ips: Dict[str, Union[ipaddress.IPv4Address, ipaddress.IPv6Address]] = {}
        for hostname in hostnames:
            try:
                parsed_ips[hostname] = ipaddress.IPv4Address(hostname)
            except ipaddress.AddressValueError:
                pass
            try:
                parsed_ips[hostname] = ipaddress.IPv6Address(hostname)
            except ipaddress.AddressValueError:
                pass

        # Later, only the normalised IPs are used as SANs, not the raw IPs.
        # Remove bindable but non-accessible addresses (like 0.0.0.0) form the SANs.
        true_hostnames = [hostname for hostname in hostnames if hostname not in parsed_ips]
        accessible_ips = [str(ip) for ip in parsed_ips.values() if not ip.is_unspecified]

        # Build a certificate as the framework believe is good enough for itself.
        subject = {'common_name': true_hostnames[0] if true_hostnames else accessible_ips[0]}
        public_key, private_key = oscrypto.asymmetric.generate_pair('rsa', bit_size=2048)
        builder = certbuilder.CertificateBuilder(subject, public_key)
        builder.ca = True
        builder.key_usage = {'digital_signature', 'key_encipherment', 'key_cert_sign', 'crl_sign'}
        builder.extended_key_usage = {'server_auth', 'client_auth'}
        builder.self_signed = True
        builder.subject_alt_ips = list(set(accessible_ips))  # deduplicate
        builder.subject_alt_domains = list(set(true_hostnames) | set(accessible_ips))  # deduplicate
        certificate = builder.build(private_key)
        cert_pem: bytes = certbuilder.pem_armor_certificate(certificate)
        pkey_pem: bytes = oscrypto.asymmetric.dump_private_key(private_key, password, target_ms=10)
        return cert_pem, pkey_pem


class WebhookK3dServer(WebhookServer):
    """
    A tunnel from inside of K3d/K3s to its host where the operator is running.

    With this tunnel, a developer can develop the webhooks when fully offline,
    since all the traffic is local and never leaves the host machine.

    The forwarding is maintained by K3d itself. This tunnel only replaces
    the endpoints for the Kubernetes webhook and injects an SSL certificate
    with proper CN/SANs --- to match Kubernetes's SSL validity expectations.
    """
    DEFAULT_HOST = 'host.k3d.internal'


class WebhookMinikubeServer(WebhookServer):
    """
    A tunnel from inside of Minikube to its host where the operator is running.

    With this tunnel, a developer can develop the webhooks when fully offline,
    since all the traffic is local and never leaves the host machine.

    The forwarding is maintained by Minikube itself. This tunnel only replaces
    the endpoints for the Kubernetes webhook and injects an SSL certificate
    with proper CN/SANs --- to match Kubernetes's SSL validity expectations.
    """
    DEFAULT_HOST = 'host.minikube.internal'


class WebhookNgrokTunnel:
    """
    Tunnel admission webhook request via an external tunnel: ngrok_.

    .. _ngrok: https://ngrok.com/

    ``addr``, ``port``, and ``path`` have the same meaning as in
    `kopf.WebhookServer`: where to listen for connections locally.
    Ngrok then tunnels this endpoint remotely with.

    Mind that the ngrok webhook tunnel runs the local webhook server
    in an insecure (HTTP) mode. For secure (HTTPS) mode, a paid subscription
    and properly issued certificates are needed. This goes beyond Kopf's scope.
    If needed, implement your own ngrok tunnel.

    Besides, ngrok tunnel does not report any CA to the webhook client configs.
    It is expected that the default trust chain is sufficient for ngrok's certs.

    ``token`` can be used for paid subscriptions, which lifts some limitations.
    Otherwise, the free plan has a limit of 40 requests per minute
    (this should be enough for local development).

    ``binary``, if set, will use the specified ``ngrok`` binary path;
    otherwise, ``pyngrok`` downloads the binary at runtime (not recommended).

    .. warning::

        The public URL is not properly protected and a malicious user
        can send requests to a locally running operator. If the handlers
        only process the data and make no side effects, this should be fine.

        Despite ngrok provides basic auth ("username:password"),
        Kubernetes does not permit this information in the URLs.

        Ngrok partially "protects" the URLS by assigning them random hostnames.
        Additionally, you can add random paths. However, this is not "security",
        only a bit of safety for a short time (enough for development runs).
    """
    addr: Optional[str]  # None means "any interface"
    port: Optional[int]  # None means a random port
    path: Optional[str]
    token: Optional[str]
    region: Optional[str]
    binary: Optional[StrPath]

    def __init__(
            self,
            *,
            addr: Optional[str] = None,
            port: Optional[int] = None,
            path: Optional[str] = None,
            token: Optional[str] = None,
            region: Optional[str] = None,
            binary: Optional[StrPath] = None,
    ) -> None:
        super().__init__()
        self.addr = addr
        self.port = port
        self.path = path
        self.token = token
        self.region = region
        self.binary = binary

    async def __call__(self, fn: reviews.WebhookFn) -> AsyncIterator[reviews.WebhookClientConfig]:
        try:
            from pyngrok import conf, ngrok
        except ImportError:
            raise MissingDependencyError(
                "Using ngrok webhook tunnel requires an extra dependency: "
                "run `pip install pyngrok` or `pip install kopf[dev]`. "
                "More: https://kopf.readthedocs.io/en/stable/admission/")

        if self.binary is not None:
            conf.get_default().ngrok_path = str(self.binary)
        if self.region is not None:
            conf.get_default().region = self.region
        if self.token is not None:
            ngrok.set_auth_token(self.token)

        # Ngrok only supports HTTP with a free plan; HTTPS requires a paid subscription.
        local_server = WebhookServer(addr=self.addr, port=self.port, path=self.path, insecure=True)
        tunnel: Optional[ngrok.NgrokTunnel] = None
        loop = asyncio.get_running_loop()
        try:
            async for client_config in local_server(fn):

                # Re-create the tunnel for each new local endpoint (if it did change at all).
                if tunnel is not None:
                    await loop.run_in_executor(None, ngrok.disconnect, tunnel.public_url)
                parsed = urllib.parse.urlparse(client_config['url'])
                tunnel = await loop.run_in_executor(
                    None, functools.partial(ngrok.connect, f'{parsed.port}', bind_tls=True))

                # Adjust for local webhook server specifics (no port, but with the same path).
                # Report no CA bundle -- ngrok's certs (Let's Encrypt) are in a default trust chain.
                url = f"{tunnel.public_url}{self.path or ''}"
                logger.debug(f"Accessing the webhooks at {url}")
                yield reviews.WebhookClientConfig(url=url)  # e.g. 'https://e5fc05f6494b.ngrok.io/xyz'
        finally:
            if tunnel is not None:
                await loop.run_in_executor(None, ngrok.disconnect, tunnel.public_url)


class ClusterDetector:
    """
    A mixing for auto-server/auto-tunnel to detect the cluster type.

    The implementation of the server detection requires the least possible
    permissions or no permissions at all. In most cases, it will identify
    the server type by its SSL certificate meta-information (subject/issuer).
    SSL information is the most universal way for all typical local clusters.

    If SSL parsing fails, it will try to fetch the information from the cluster.
    However, it rarely contains any useful information about the cluster's
    surroundings and environment, but only about the cluster itself
    (though it helps with K3s).

    Note: the SSL certificate of the Kubernetes API is checked, not of webhooks.
    """
    @staticmethod
    async def guess_host() -> Optional[str]:
        try:
            import certvalidator
        except ImportError:
            raise MissingDependencyError(
                "Auto-guessing cluster types requires an extra dependency: "
                "run `pip install certvalidator` or `pip install kopf[dev]`. "
                "More: https://kopf.readthedocs.io/en/stable/admission/")

        hostname, cert = await scanning.read_sslcert()
        valcontext = certvalidator.ValidationContext(extra_trust_roots=[cert])
        validator = certvalidator.CertificateValidator(cert, validation_context=valcontext)
        certpath = validator.validate_tls(hostname)
        issuer_cn = certpath.first.issuer.native.get('common_name', '')
        subject_cn = certpath.first.subject.native.get('common_name', '')
        subject_org = certpath.first.subject.native.get('organization_name', '')

        if subject_cn == 'k3s' or subject_org == 'k3s' or issuer_cn.startswith('k3s-'):
            return WebhookK3dServer.DEFAULT_HOST
        elif subject_cn == 'minikube' or issuer_cn == 'minikubeCA':
            return WebhookMinikubeServer.DEFAULT_HOST
        else:
            versioninfo = await scanning.read_version()
            if '+k3s' in versioninfo.get('gitVersion', ''):
                return WebhookK3dServer.DEFAULT_HOST
        return None


class WebhookAutoServer(ClusterDetector, WebhookServer):
    """
    A locally listening webserver which attempts to guess its proper hostname.

    The choice is happening between supported webhook servers only
    (K3d/K3d and Minikube at the moment). In all other cases,
    a regular local server is started without hostname overrides.

    If automatic tunneling is possible, consider `WebhookAutoTunnel` instead.
    """
    async def __call__(self, fn: reviews.WebhookFn) -> AsyncIterator[reviews.WebhookClientConfig]:
        host = self.DEFAULT_HOST = await self.guess_host()
        if host is None:
            logger.debug(f"Cluster detection failed, running a simple local server.")
        else:
            logger.debug(f"Cluster detection found the hostname: {host}")
        async for client_config in super().__call__(fn):
            yield client_config


class WebhookAutoTunnel(ClusterDetector):
    """
    The same as `WebhookAutoServer`, but with possible tunneling.

    Generally, tunneling gives more possibilities to run in any environment,
    but it must not happen without a permission from the developers,
    and is not possible if running in a completely isolated/local/CI/CD cluster.
    Therefore, developers should activated automatic setup explicitly.

    If automatic tunneling is prohibited or impossible, use `WebhookAutoServer`.

    .. note::

        Automatic server/tunnel detection is highly limited in configuration
        and provides only the most common options of all servers & tunners:
        specifically, listening ``addr:port/path``.
        All other options are specific to their servers/tunnels
        and the auto-guessing logic cannot use/accept/pass them.
    """
    addr: Optional[str]  # None means "any interface"
    port: Optional[int]  # None means a random port
    path: Optional[str]

    def __init__(
            self,
            *,
            addr: Optional[str] = None,
            port: Optional[int] = None,
            path: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.addr = addr
        self.port = port
        self.path = path

    async def __call__(self, fn: reviews.WebhookFn) -> AsyncIterator[reviews.WebhookClientConfig]:
        server: reviews.WebhookServerProtocol
        host = await self.guess_host()
        if host is None:
            logger.debug(f"Cluster detection failed, using an ngrok tunnel.")
            server = WebhookNgrokTunnel(addr=self.addr, port=self.port, path=self.path)
        else:
            logger.debug(f"Cluster detection found the hostname: {host}")
            server = WebhookServer(addr=self.addr, port=self.port, path=self.path, host=host)
        async for client_config in server(fn):
            yield client_config
