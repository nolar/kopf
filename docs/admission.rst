=================
Admission control
=================

Admission hooks are callbacks from Kubernetes to the operator before
the resources are created or modified. There are two types of hooks:

* Validating admission webhooks.
* Mutating admission webhooks.

For more information on the admission webhooks,
see the Kubernetes documentation: `Dynamic Admission Control`__.

__ https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/


Dependencies
============

To minimize Kopf's footprint in production systems,
it does not include heavy-weight dependencies needed only for development,
such as SSL cryptography and certificate generation libraries.
For example, Kopf's footprint with critical dependencies is 8.8 MB,
while ``cryptography`` would add 8.7 MB; ``certbuilder`` adds "only" 2.9 MB.

To use all features of development-mode admission webhook servers and tunnels,
you have to install Kopf with an extra:

.. code-block:: bash

    pip install kopf[dev]

If this extra is not installed, Kopf will not generate self-signed certificates
and will run either with HTTP only or with externally provided certificates.

Also, without this extra, Kopf will not be able to establish Ngrok tunnels.
Though, it will be able to use K3d & Minikube servers with magic hostnames.

Any attempt to run it in a mode with self-signed certificates or tunnels
will raise a startup-time error with an explanation and suggested actions.


Validation handlers
===================

.. code-block:: python

    import kopf

    @kopf.on.validate('kopfexamples')
    def say_hello(warnings: list[str], **_):
        warnings.append("Verified with the operator's hook.")

    @kopf.on.validate('kopfexamples')
    def check_numbers(spec, **_):
        if not isinstance(spec.get('numbers', []), list):
            raise kopf.AdmissionError("Numbers must be a list if present.")

    @kopf.on.validate('kopfexamples')
    def convertible_numbers(spec, warnings, **_):
        if isinstance(spec.get('numbers', []), list):
            for val in spec.get('numbers', []):
                if not isinstance(val, float):
                    try:
                        float(val)
                    except ValueError:
                        raise kopf.AdmissionError(f"Cannot convert {val!r} to a number.")
                    else:
                        warnings.append(f"{val!r} is not a number but can be converted.")

    @kopf.on.validate('kopfexamples')
    def numbers_range(spec, **_):
        if isinstance(spec.get('numbers', []), list):
            if not all(0 <= float(val) <= 100 for val in spec.get('numbers', [])):
                raise kopf.AdmissionError("Numbers must be below 0..100.", code=499)

Each handler is mapped to its dedicated admission webhook and an endpoint
so that all handlers are executed in parallel independently of each other.
They must not expect that other checks are already performed by other handlers;
if such logic is needed, make it as one handler with a sequential execution.


Mutation handlers
=================

To mutate the object, modify the :kwarg:`patch`. Changes to :kwarg:`body`,
:kwarg:`spec`, etc, will not be remembered (and are not possible):

.. code-block:: python

    import kopf

    @kopf.on.mutate('kopfexamples')
    def ensure_default_numbers(spec, patch, **_):
        if 'numbers' not in spec:
            patch.spec['numbers'] = [1, 2, 3]

    @kopf.on.mutate('kopfexamples')
    def convert_numbers_if_possible(spec, patch, **_):
        if 'numbers' in spec and isinstance(spec.get('numbers'), list):
            patch.spec['numbers'] = [_maybe_number(v) for v in spec['numbers']]

    def _maybe_number(v):
        try:
            return float(v)
        except ValueError:
            return v

The semantics is the same or as close as possible to the Kubernetes API's one.
``None`` values will remove the relevant keys.

Under the hood, the patch object will remember each change
and will return a JSONPatch structure to Kubernetes.


Handler options
===============

Handlers have a limited capability to inform Kubernetes about its behaviour.
The following options are supported:

``persistent`` (``bool``) webhooks will not be removed from the managed
configurations on exit; non-persisted webhooks will be removed if possible.
Such webhooks will prevent all admissions even when the operator is down.
This option has no effect if there is no managed configuration.
The webhook cleanup only happens on graceful exits; on forced exits, even
non-persisted webhooks might be persisted and block the admissions.

``operation`` (``str``) will configure this handler/webhook to be called only
for a specific operation. For multiple operations, add several decorators.
Possible values are ``"CREATE"``, ``"UPDATE"``, ``"DELETE"``, ``"CONNECT"``.
The default is ``None``, i.e. all operations (equivalent to ``"*"``).

``subresource`` (``str``) will only react when to the specified subresource.
Usually it is ``"status"`` or ``"scale"``, but can be anything else.
The value ``None`` means that only the main resource body will be checked.
The value ``"*"`` means that both the main body and any subresource are checked.
The default is ``None``, i.e. only the main body to be checked.

``side_effects`` (``bool``) tells Kubernetes that the handler can have side
effects in non-dry-run mode. In dry-run mode, it must have no side effects.
The dry-run mode is passed to the handler as a :kwarg:`dryrun` kwarg.
The default is ``False``, i.e. the handler has no side effects.

``ignore_failures`` (``bool``) marks the webhook as tolerant to errors.
This includes errors of the handler itself (disproved admissions),
so as HTTP/TCP communication errors when apiservers talk to the webhook server.
By default, an inaccessible or rejecting webhook blocks the admission.

The developers can use regular :doc:`/filters`. In particular, the ``labels``
will be passed to the webhook configuration as ``.webhooks.*.objectSelector``
for optimization purposes: so that admissions are not even sent to the webhook
server if it is known that they will be filtered out and therefore allowed.

Server-side filtering supports everything except callbacks:
i.e., ``"strings"``, ``kopf.PRESENT`` and ``kopf.ABSENT`` markers.
The callbacks will be evaluated after the admission review request is received.

.. warning::

    Be careful with the builtin resources and admission hooks.
    If a handler is broken or misconfigured, it can prevent creating
    those resources, e.g. pods, in the whole cluster. This will render
    the cluster unusable until the configuration is manually removed.

    Start the development in local clusters, validating/mutating the custom
    resources first, and enable ``ignore_errors`` initially.
    Enable the strict mode of the handlers only when stabilised.


In-memory containers
====================

Kopf provides :doc:`/memos` for each resource. However, webhooks can happen
before a resource is created. This affects how the memos work.

For update and deletion requests, the actual memos of the resources are used.

For the admission requests on resource creation, a memo is created and discarded
immediately. It means that the creation's memos are useless at the moment.

This can change in the future: the memos of resource creation attempts
will be preserved for a limited but short time (configurable),
so that the values could be shared between the admission and the handling, but
so that there are no memory leaks if the resource never succeeds in admission.


Admission warnings
==================

Starting with Kubernetes 1.19 (check with ``kubectl version``),
admission warnings can be returned from admission handlers.

To populate warnings, accept a **mutable** :kwarg:`warnings` (``list[str]``)
and add strings to it:

.. code-block:: python

    import kopf

    @kopf.on.validate('kopfexamples')
    def ensure_default_numbers(spec, warnings: list[str], **_):
        if spec.get('field') == 'value':
            warnings.append("The default value is used. It is okay but worth changing.")

The admission warnings look like this (requires kubectl 1.19+):

.. code-block:: none

    $ kubectl create -f examples/obj.yaml
    Warning: The default value is used. It is okay but worth changing.
    kopfexample.kopf.dev/kopf-example-1 created

.. note::

    Despite Kopf's intention to utilise Python's native features that
    semantically map to Kubernetes's or operators' features,
    Python StdLib's :mod:`warnings` is not used for admission warnings
    (the initial idea was to catch ``UserWarning`` and ``warnings.warn("...")``
    calls and return them as admission warnings).

    The StdLib's module is documented as thread-unsafe (therefore, task-unsafe)
    and requires hacking the global state which might affect other threads
    and/or tasks -- there is no clear way to do this consistently.

    This may be revised in the future and provided as an additional feature.


Admission errors
================

Unlike with regular handlers and their error handling logic (:doc:`/errors`),
the webhooks cannot do retries or backoffs. So, the ``backoff=``, ``errors=``,
``retries=``, ``timeout=`` options are not accepted on the admission handlers.

A special exception :class:`kopf.AdmissionError` is provided to customize
the status code and the message of the admission review response.

All other exceptions,
including :class:`kopf.PermanentError` and :class:`kopf.TemporaryError`,
equally fail the admission (be that validating or mutating admission).
However, they return the general HTTP code 500 (non-customisable).

One and only one error is returned to the user who make an API request.
In cases when Kubernetes makes several parallel requests to several webhooks
(typically with managed webhook configurations, the fastest error is used).
Within Kopf (usually with custom webhook servers/tunnels or self-made
non-managed webhook configurations), errors are prioritised: first, admission
errors, then permanent errors, then temporary errors, then arbitrary errors
are used to select the only error to report in the admission review response.

.. code-block:: python

    @kopf.on.validate('kopfexamples')
    def validate1(spec, **_):
        if spec.get('field') == 'value':
            raise kopf.AdmissionError("Meh! I don't like it. Change the field.", code=400)

The admission errors look like this (manually indented for readability):

.. code-block:: none

    $ kubectl create -f examples/obj.yaml
    Error from server: error when creating "examples/obj.yaml":
        admission webhook "validate1.auto.kopf.dev" denied the request:
            Meh! I don't like it. Change the field.

Note that Kubernetes executes multiple webhooks in parallel.
The first one to return the result is the one and the only shown;
other webhooks are not shown even if they fail with useful messages.
With multiple failing admissions, the message will be varying on each attempt.


Webhook management
==================

Admission (both for validation and for mutation) only works when the cluster
has special resources created: either ``kind: ValidatingWebhookConfiguration``
or ``kind: MutatingWebhookConfiguration`` or both.
Kopf can automatically manage the webhook configuration resources
in the cluster if it is given RBAC permissions to do so.

To manage the validating/mutating webhook configurations, Kopf requires
the following RBAC permissions in its service account (see :doc:`/deployment`):

.. code-block:: yaml

    apiVersion: rbac.authorization.k8s.io/v1beta1
    kind: ClusterRole
    rules:
      - apiGroups: [admissionregistration.k8s.io/v1, admissionregistration.k8s.io/v1beta1]
        resources: [validatingwebhookconfigurations, mutatingwebhookconfigurations]
        verbs: [create, patch]

By default, configuration management is disabled (for safety and stability).
To enable, set the name of the managed configuration objects:

.. code-block:: python

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.admission.managed = 'auto.kopf.dev'

Multiple records for webhooks will be added or removed for multiple resources
to those configuration objects as needed. Existing records will be overwritten.
If the configuration resource is absent, it will be created
(but at most one for validating and one for mutating configurations).

Kopf manages the webhook configurations according to how Kopf itself believes
it is sufficient to achieve the goal. Many available Kubernetes features
are not covered by this management. To use these features and control
the configuration with precision, operator developers can disable
the automated management and take care of the configuration manually.


Servers and tunnels
===================

Kubernetes admission webhooks are designed to be passive rather than active
(from the operator's point of view; vice versa from Kubernetes's point of view).
It means, the webhooks must passively wait for requests via an HTTPS endpoint.
There is currently no official way how an operator can actively pull or poll
the admission requests and send the responses back
(as it is done for all other resource changes streamed via the Kubernetes API).

It is typically non-trivial to forward the requests from a remote or isolated
cluster to a local host machine where the operator is running for development.

However, one of Kopf's main promises is to work the same way both in-cluster
and on the developers' machines. It cannot be made "the same way" for webhooks,
but Kopf attempts to make these modes similar to each other code-wise.

To fulfil its promise, Kopf delegates this task to webhook servers and tunnels,
which are capable of receiving the webhook requests, marshalling them
to the handler callbacks, and then returning the results to Kubernetes.

Due to numerous ways of how the development and production environments can be
configured, Kopf does not provide a default configuration for a webhook server,
so it must be set by the developer:

.. code-block:: python

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        if os.environ.get('ENVIRONMENT') is None:
            # Only as an example:
            settings.admission.server = kopf.WebhookK3dServer(port=54321)
            settings.admission.managed = 'auto.kopf.dev'
        else:
            # Assuming that the configuration is done manually:
            settings.admission.server = kopf.WebhookServer(addr='0.0.0.0', port=8080)
            settings.admission.managed = 'auto.kopf.dev'

If there are admission handlers present and no webhook server/tunnel configured,
the operator will fail at startup with an explanatory message.

Kopf provides several webhook servers and tunnels out of the box,
each with its configuration parameters (see their descriptions):

*Webhook servers* listen on an HTTPS port locally and handle requests.

* :class:`kopf.WebhookServer` is helpful for local development and ``curl`` and
  a Kubernetes cluster that runs directly on the host machine and can access it.
  It is also used internally by most tunnels for a local target endpoint.
* :class:`kopf.WebhookK3dServer` is for local K3d/K3s clusters (even in a VM),
  accessing the server via a magical hostname ``host.k3d.internal``.
* :class:`kopf.WebhookMinikubeServer` for local Minikube clusters (even in VMs),
  accessing the server via a magical hostname ``host.minikube.internal``.
* :class:`kopf.WebhookDockerDesktopServer` for the DockerDesktop cluster,
  accessing the server via a magical hostname ``host.docker.internal``.

*Webhook tunnels* forward the webhook requests through external endpoints
usually to a locally running *webhook server*.

* :class:`kopf.WebhookNgrokTunnel` established a tunnel through ngrok_.

.. _ngrok: https://ngrok.com/

For ease of use, the cluster type can be recognised automatically in some cases:

* :class:`kopf.WebhookAutoServer` runs locally, detects Minikube & K3s, and
  uses them via their special hostnames. If it cannot detect the cluster type,
  it runs a simple local webhook server. The auto-server never tunnels.
* :class:`kopf.WebhookAutoTunnel` attempts to use an auto-server if possible.
  If not, it uses one of the available tunnels (currently, only ngrok).
  This is the most universal way to make any environment work.

.. note::
    External tunnelling services usually limit the number of requests.
    For example, ngrok has a limit of 40 requests per minute on a free plan.

    The services also usually provide paid subscriptions to overcome that limit.
    It might be a wise idea to support the service you rely on with some money.
    If that is not an option, you can implement free tunnelling your way.

.. note::
    A reminder: using development-mode tunnels and self-signed certificates
    requires extra dependencies: ``pip install kopf[dev]``.


Authenticate apiservers
=======================

There are many ways how webhook clients (Kubernetes's apiservers)
can authenticate against webhook servers (the operator's webhooks),
and even more ways to validate the supplied credentials.

More on that, apiservers cannot be configured to authenticate against
webhooks dynamically at runtime, as `this requires control-plane configs`__,
which are out of reach of Kopf.

__ https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/#authenticate-apiservers

For simplicity, Kopf does not authenticate webhook clients.

However, Kopf's built-in webhook servers & tunnels extract the very basic
request information and pass it to the admission handlers
for additional verification and possibly for authentification:

* :kwarg:`headers` (``Mapping[str, str]``) contains all HTTPS headers,
  including ``Authorization: Basic ...``, ``Authorization: Bearer ...``.
* :kwarg:`sslpeer` (``Mapping[str, Any]``) contains the SSL peer information
  as returned by :func:`ssl.SSLSocket.getpeercert` or ``None`` if no proper SSL
  certificate is provided by a client (i.e. by apiservers talking to webhooks).

An example of headers:

.. code-block:: python

    {'Host': 'localhost:54321',
     'Authorization': 'Basic dXNzc2VyOnBhc3Nzdw==',  # base64("ussser:passsw")
     'Content-Length': '844',
     'Content-Type': 'application/x-www-form-urlencoded'}

An example of a self-signed peer certificate presented to ``sslpeer``:

.. code-block:: python

    {'subject': ((('commonName', 'Example Common Name'),),
                 (('emailAddress', 'example@kopf.dev'),)),
     'issuer': ((('commonName', 'Example Common Name'),),
                (('emailAddress', 'example@kopf.dev'),)),
     'version': 1,
     'serialNumber': 'F01984716829537E',
     'notBefore': 'Mar  7 17:12:20 2021 GMT',
     'notAfter': 'Mar  7 17:12:20 2022 GMT'}

To reproduce these examples without configuring the Kubernetes apiservers
but only Kopf & CLI tools, do the following:

Step 1: Generate a self-signed ceritificate to be used as a client certificate:

.. code-block:: bash

    openssl req -x509 -newkey rsa:2048 -keyout client-key.pem -out client-cert.pem -days 365 -nodes
    # Country Name (2 letter code) []:
    # State or Province Name (full name) []:
    # Locality Name (eg, city) []:
    # Organization Name (eg, company) []:
    # Organizational Unit Name (eg, section) []:
    # Common Name (eg, fully qualified host name) []:Example Common Name
    # Email Address []:example@kopf.dev

Step 2: Start an operator with the certificate as a CA (for simplicity;
in normal setups, there is a separate CA, which signs the client certificates;
explaining this topic is beyond the scope of this framework's documentation):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def config(settings: kopf.OperatorSettings, **_):
        settings.admission.managed = 'auto.kopf.dev'
        settings.admission.server = kopf.WebhookServer(cafile='client-cert.pem')

    @kopf.on.validate('kex')
    def show_auth(headers, sslpeer, **_):
        print(f'{headers=}')
        print(f'{sslpeer=}')

Step 3: Save the admission review payload into a local file:

.. code-block:: bash

    cat >review.json << EOF
    {
      "kind": "AdmissionReview",
      "apiVersion": "admission.k8s.io/v1",
      "request": {
        "uid": "1ca13837-ad60-4c9e-abb8-86f29d6c0e84",
        "kind": {"group": "kopf.dev", "version": "v1", "kind": "KopfExample"},
        "resource": {"group": "kopf.dev", "version": "v1", "resource": "kopfexamples"},
        "requestKind": {"group": "kopf.dev", "version": "v1", "kind": "KopfExample"},
        "requestResource": {"group": "kopf.dev", "version": "v1", "resource": "kopfexamples"},
        "name": "kopf-example-1",
        "namespace": "default",
        "operation": "CREATE",
        "userInfo": {"username": "admin", "uid": "admin", "groups": ["system:masters", "system:authenticated"]},
        "object": {
          "apiVersion": "kopf.dev/v1",
          "kind": "KopfExample",
          "metadata": {"name": "kopf-example-1", "namespace": "default"}
        },
        "oldObject": null,
        "dryRun": true
      }
    }
    EOF

Step 4: Send the admission review payload to the operator's webhook server
using the generated client certificate, observe the client identity printed
to stdout by the webhook server and returned in the warnings:

.. code-block:: bash

    curl --insecure --cert client-cert.pem --key client-key.pem https://ussser:passsw@localhost:54321 -d @review.json
    # {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
    #  "response": {"uid": "1ca13837-ad60-4c9e-abb8-86f29d6c0e84",
    #               "allowed": true,
    #               "warnings": ["SSL peer is Example Common Name."]}}

Using this data, operator developers can implement servers/tunnels
with custom authentication methods when and if needed.


Debugging with SSL
==================

Kubernetes requires that the webhook URLs are always HTTPS, never HTTP.
For this reason, Kopf runs the webhook servers/tunnels with HTTPS by default.

If a webhook server is configured without a server certificate,
a self-signed certificate is generated at startup, and only HTTPS is served.

.. code-block:: python

    @kopf.on.startup()
    def config(settings: kopf.OperatorSettings, **_):
        settings.admission.server = kopf.WebhookServer()

That endpoint can be accessed directly with ``curl``:

.. code-block:: bash

    curl --insecure https://localhost:54321 -d @review.json

It is possible to store the generated certificate itself and use as a CA:

.. code-block:: python

    @kopf.on.startup()
    def config(settings: kopf.OperatorSettings, **_):
        settings.admission.server = kopf.WebhookServer(cadump='selfsigned.pem')

.. code-block:: bash

    curl --cacert selfsigned.pem https://localhost:54321 -d @review.json

For production, a properly generated certificate should be used.
The CA, if not specified, is assumed to be in the default trust chain.
This applies to all servers:
:class:`kopf.WebhookServer`, :class:`kopf.WebhookK3dServer`, etc.

.. code-block:: python

    @kopf.on.startup()
    def config(settings: kopf.OperatorSettings, **_):
        settings.admission.server = kopf.WebhookServer(
            cafile='/secrets/ca.pem',       # or cadata, or capath.
            certfile='/secrets/cert.pem',
            pkeyfile='/secrets/pkey.pem',
            password='...',                 # for the private key, if used.
            file_check_interval=60,
        )

You can use cert-manager or other externally provided certificate files
at their known (mounted) locations without the full restart of the operator.
Once any of the specified files changes, e.g. due to certificate or private key
automated renewal, the webhook server will restart with the new certificate
(at the latest after ``file_check_interval`` seconds, which defaults to 60s).

.. note::
    ``cadump`` (output) can be used together with ``cafile``/``cadata`` (input),
    though it will be the exact copy of the CA and does not add any benefit.

As a last resort, if SSL is still a problem, it can be disabled and an insecure
HTTP server can be used. This does not work with Kubernetes but can be used
for direct access during development; it is also used by some tunnels that
do not support HTTPS tunnelling (or require paid subscriptions):

.. code-block:: python

    @kopf.on.startup()
    def config(settings: kopf.OperatorSettings, **_):
        settings.admission.server = kopf.WebhookServer(insecure=True)


Custom servers/tunnels
======================

Operator developers can provide their custom servers and tunnels by implementing
an async iterator over client configs (`kopf.WebhookClientConfig`).
There are two ways to implement servers/tunnels.

One is a simple but non-configurable coroutine:

.. code-block:: python

    async def mytunnel(fn: kopf.WebhookFn) -> AsyncIterator[kopf.WebhookClientConfig]:
        ...
        yield client_config
        await asyncio.Event().wait()

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.admission.server = mytunnel  # no arguments!

Another one is a slightly more complex but configurable class:

.. code-block:: python

    class MyTunnel:
        async def __call__(self, fn: kopf.WebhookFn) -> AsyncIterator[kopf.WebhookClientConfig]:
            ...
            yield client_config
            await asyncio.Event().wait()

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.admission.server = MyTunnel()  # arguments are possible.

The iterator MUST accept a positional argument of type :class:`kopf.WebhookFn`
and call it with the JSON-parsed payload when a review request is received;
then, it MUST ``await`` the result and JSON-serialize it as a review response:

.. code-block:: python

    response = await fn(request)

Optionally (though highly recommended), several keyword arguments can be passed
to extend the request data (if not passed, they all use ``None`` by default):

* ``webhook`` (``str``) -- to execute only one specific handler/webhook.
  The id usually comes from the URL, which the framework injects automatically.
  It is highly recommended to provide at least this hint:
  otherwise, all admission handlers are executed, with mutating and validating
  handlers mixed, which can lead to mutating patches returned for validation
  requests, which in turn will fail the admission on the Kubernetes side.
* ``headers`` (``Mapping[str, str]``) -- the HTTPS headers of the request
  are passed to handlers as :kwarg:`headers` and can be used for authentication.
* ``sslpeer`` (``Mapping[str, Any]``) -- the SSL peer information taken from
  the client certificate (if provided and if verified); it is passed
  to handlers as :kwarg:`sslpeer` and can be used for authentication.

.. code-block:: python

    response = await fn(request, webhook=handler_id, headers=headers, sslpeer=sslpeer)

There is no guarantee on what is happening in the callback and how it works.
The exact implementation can be changed in the future without warning: e.g.,
the framework can either invoke the admission handlers directly in the callback
or queue the request for a background execution and return an awaitable future.

The iterator must yield one or more client configs. Configs are dictionaries
that go to the managed webhook configurations as ``.webhooks.*.clientConfig``.

Regardless of how the client config is created, the framework extends the URLs
in the ``url`` and ``service.path`` fields with the handler/webhook ids,
so that a URL ``https://myhost/path`` becomes ``https://myhost/path/handler1``,
``https://myhost/path/handler2``, so on.

Remember: Kubernetes prohibits using query parameters and fragments in the URLs.

In most cases, only one yielded config is enough if the server is going
to serve the requests at the same endpoint.
In rare cases when the endpoint changes over time (e.g. for dynamic tunnels),
the server/tunnel should yield a new config every time the endpoint changes,
and the webhook manager will reconfigure all managed webhooks accordingly.

The server/tunnel must hold control by running the server or by sleeping.
To sleep forever, use ``await asyncio.Event().wait()``. If the server/tunnel
exits unexpectedly, this causes the whole operator to exit.

If the goal is to implement a tunnel only, but not a custom webhook server,
it is highly advised to inherit from or directly use :class:`kopf.WebhookServer`
to run a locally listening endpoint. This server implements all URL parsing
and request handling logic well-aligned with the rest of the framework:

.. code-block:: python

    # Inheritance:
    class MyTunnel1(kopf.WebhookServer):
        async def __call__(self, fn: kopf.WebhookFn) -> AsyncIterator[kopf.WebhookClientConfig]:
            ...
            for client_config in super().__call__(fn):
                ...  # renew a tunnel, adjust the config
                yield client_config

    # Composition:
    class MyTunnel2:
        async def __call__(self, fn: kopf.WebhookFn) -> AsyncIterator[kopf.WebhookClientConfig]:
            server = kopf.WebhookServer(...)
            for client_config in server(fn):
                ...  # renew a tunnel, adjust the config
                yield client_config


System resource cleanup
=======================

It is advised that custom servers/tunnels cleanup the system resources
they allocate at runtime. The easiest way is the ``try-finally`` block --
the cleanup will happen on the garbage collection of the generator object
(beware: it can be postponed in some environments, e.g. in PyPy).

For explicit cleanup of system resources, the servers/tunnels can implement
the asynchronous context manager protocol:

.. code-block:: python

    class MyServer:
        def __init__(self):
            super().__init__()
            self._resource = None

        async def __aenter__(self) -> "MyServer":
            self._resource = PotentiallyLeakableResource()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
            self._resource.cleanup()
            self._resource = None

        async def __call__(self, fn: kopf.WebhookFn) -> AsyncIterator[kopf.WebhookClientConfig]:
            for client_config in super().__call__(fn):
                yield client_config

The context manager should usually return ``self``, but it can return
a substitute webhook server/tunnel object, which will actually be used.
That way, the context manager turns into a factory of webhook server(s).

Keep in mind that the webhook server/tunnel is used only once per
the operator's lifetime; once it exits, the whole operator stops.
It makes no practical sense in making the webhook servers/tunnels reentrant.

.. note::

    **An implementation note:** webhook servers and tunnels provided by Kopf
    use a little hack to keep them usable with the simple protocol
    (a callable that yields the client configs) while also supporting
    the optional context manager protocol for system resource safety:
    when the context manager is exited, it force-closes the generators
    that yield the client configs as if they were garbage-collected.
    Users' final webhook servers/tunnels do not need this level of complication.

.. seealso::
    For reference implementations of servers and tunnels,
    see the `provided webhooks`__.

__ https://github.com/nolar/kopf/blob/master/kopf/toolkits/webhooks.py
