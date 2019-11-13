==============
Authentication
==============

To access a Kubernetes cluster, an endpoint and some credentials are needed.
They are usually taken either from the environment (environment variables),
or from the ``~/.kube/config`` file, or from external authentication services.

Kopf does not try to maintain all the authentication methods possible.
Instead, it allows the operator developers to implement their own custom
authentication methods, and piggybacks the existing Kubernetes clients.


Custom authentication
=====================

To implement a custom authentication method, one or few login-handlers
can be added. The login handlers should either return nothing (``None``),
or an instance of `kopf.ConnectionInfo`::

    import kopf

    @kopf.on.login()
    def login_fn(**kwargs):
        return kopf.ConnectionInfo(
            server='https://localhost',
            ca_path='/etc/ssl/ca.crt',
            ca_data=b'...',
            insecure=True,
            username='...',
            password='...',
            scheme='Bearer',
            token='...',
            certificate_path='~/.minikube/client.crt',
            private_key_path='~/.minikube/client.key',
            certificate_data=b'...',
            private_key_data=b'...',
        )

As with any other handlers, the login handler can be async if the network
communication is needed and async mode is supported::

    import kopf

    @kopf.on.login()
    async def login_fn(**kwargs):
        pass

A `kopf.ConnectionInfo` is a container to bring only the parameters necessary
for making the API calls, but not the ways of retrieving them. Specifically:

* TCP server host & port.
* SSL verification/ignorance flag.
* SSL certificate authority.
* SSL client certificate and its private key.
* HTTP ``Authorization: Basic username:password``.
* HTTP ``Authorization: Bearer token`` (or other schemes: Bearer, Digest, etc).
* URL's default namespace for the cases when this is implied.

No matter how the endpoints or credentials are retrieved, they are directly
mapped to TCP/SSL/HTTPS protocols in the API clients. It is the responsibility
of the authentication handlers to ensure that the values are consistent
and valid (e.g. via internal verification calls). It is in theory possible
to mix all authentication methods at once, or to have none of them at all.
If the credentials are inconsistent or invalid, there will be permanent
re-authentication happening.

Multiple handlers can be declared to retrieve different credentials,
or the same credentials via different libraries. All of the retrieved
credentials will be used in random order with no specific priority.


Piggybacking
============

In case no handlers are explicitly declared, Kopf attempts to authenticate
with the existing Kubernetes libraries if they are installed.
At the moment: pykube-ng_ and kubernetes_.
In the future, more libraries can be added for authentication piggybacking.

.. _pykube-ng: https://github.com/hjacobs/pykube
.. _kubernetes: https://github.com/kubernetes-client/python

*Piggybacking* means that the config parsing and authentication methods of these
libraries are used, and only the information needed for API calls is extracted.

If few of the piggybacked libraries are installed,
all of them will be attempted (as if multiple handlers are installed),
and all the credentials will be utilised in random order.

If that is not the desired case, and only one of the libraries is neeed,
declare a custom login handler explicitly, and use only the preferred library
by calling one of the piggybacking functions::

    import kopf

    @kopf.on.login()
    def login_fn(**kwargs):
        return kopf.login_via_pykube(**kwargs)

Or::

    import kopf

    @kopf.on.login()
    def login_fn(**kwargs):
        return kopf.login_via_client(**kwargs)

The same trick is also useful to limit the authentication attempts
by time or by number of retries (by default, it tries forever
until succeeded, returned nothing, or explicitly failed)::

    import kopf

    @kopf.on.login(retries=3)
    def login_fn(**kwargs):
        return kopf.login_via_pykube(**kwargs)

.. seealso::
    `kopf.login_via_pykube`, `kopf.login_via_client`.


Credentials lifecycle
=====================

Internally, all the credentials are gathered from all the active handlers
(either the declared ones, or all the fallback piggybacking ones)
in no particular order, and are fed into a *vault*.

The Kubernetes API calls then use random credentials from that *vault*.
If the API call fails with an HTTP 401 error, these credentials are marked
invalid, excluded from further use, and the next random credentials are tried.

When the *vault* is fully depleted, it freezes all the API calls, and triggers
the login handlers for re-authentication. Only the new credentials are used.
The credentials, which previously were known to be invalid, are ignored
to prevent a permanent never-ending re-authentication loop.

There is no credentials validation by making fake API calls.
Instead, the real API calls validate the credentials by using them,
and reporting them back to the *vault* as invalid (or keeping them as valid),
potentially causing new re-authentication activities.

In case the *vault* is depleted and no new credentials are provided
by the login handlers, the API calls fail, and so does the operator.

This internal logic is hidden from the operator developers, but it is worth
knowing how it works internally. See `Vault`.
