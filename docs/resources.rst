======================
Resource specification
======================

The following notations are supported to specify the resources to be handled.
As a rule of thumb, they are designed so that the intentions of a developer
are guessed the best way possible, and similar to ``kubectl`` semantics.

The resource name is always expected in the first place as the rightmost value.
The remaining parts are considered as an API group and an API version
of the resource -- given as either two separate strings, or as one,
but separated with a slash:

.. code-block:: python

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples')
    @kopf.on.event('kopf.dev/v1', 'kopfexamples')
    @kopf.on.event('apps', 'v1', 'deployments')
    @kopf.on.event('apps/v1', 'deployments')
    @kopf.on.event('', 'v1', 'pods')
    def fn(**_):
        pass

If only one API specification is given (except for ``v1``), it is treated
as an API group, and the preferred API version of that API group is used:

.. code-block:: python

    @kopf.on.event('kopf.dev', 'kopfexamples')
    @kopf.on.event('apps', 'deployments')
    def fn(**_):
        pass

It is also possible to specify the resources with ``kubectl``'s semantics:

.. code-block:: python

    @kopf.on.event('kopfexamples.kopf.dev')
    @kopf.on.event('deployments.apps')
    def fn(**_):
        pass

One exceptional case is ``v1`` as the API specification: it corresponds
to K8s's legacy core API (before API groups appeared), and is equivalent
to an empty API group name. The following specifications are equivalent:

.. code-block:: python

    @kopf.on.event('v1', 'pods')
    @kopf.on.event('', 'v1', 'pods')
    def fn(**_):
        pass

If neither the API group nor the API version is specified,
all resources with that name would match regardless of the API groups/versions.
However, it is reasonable to expect only one:

.. code-block:: python

    @kopf.on.event('kopfexamples')
    @kopf.on.event('deployments')
    @kopf.on.event('pods')
    def fn(**_):
        pass

In all examples above, where the resource identifier is expected, it can be
any name: plural, singular, kind, or a short name. As it is impossible to guess
which one is which, the name is remembered as is, and is later checked for all
possible names of the specific resources once those are discovered:

.. code-block:: python

    @kopf.on.event('kopfexamples')
    @kopf.on.event('kopfexample')
    @kopf.on.event('KopfExample')
    @kopf.on.event('kex')
    @kopf.on.event('StatefulSet')
    @kopf.on.event('deployments')
    @kopf.on.event('pod')
    def fn(**_):
        pass

The resource specification can be more specific on which name to match:

.. code-block:: python

    @kopf.on.event(kind='KopfExample')
    @kopf.on.event(plural='kopfexamples')
    @kopf.on.event(singular='kopfexample')
    @kopf.on.event(shortcut='kex')
    def fn(**_):
        pass

The whole categories of resources can be served, but they must be explicitly
specified to avoid unintended consequences:

.. code-block:: python

    @kopf.on.event(category='all')
    def fn(**_):
        pass

Note that the conventional category ``all`` does not really mean all resources,
but only those explicitly added to this category; some built-in resources
are excluded (e.g. ingresses, secrets).

To handle all resources in an API group/version, use a special marker instead
of the mandatory resource name:

.. code-block:: python

    @kopf.on.event('kopf.dev', 'v1', kopf.EVERYTHING)
    @kopf.on.event('kopf.dev/v1', kopf.EVERYTHING)
    @kopf.on.event('kopf.dev', kopf.EVERYTHING)
    def fn(**_):
        pass

As a consequence of the above, to handle every resource in the cluster
-- which might be not the best idea per se, but is technically possible --
omit the API group/version, and use the marker only:

.. code-block:: python

    @kopf.on.event(kopf.EVERYTHING)
    def fn(**_):
        pass

Serving everything is better when it is used with filters:

.. code-block:: python

    @kopf.on.event(kopf.EVERYTHING, labels={'only-this': kopf.PRESENT})
    def fn(**_):
        pass

.. note::

    Core v1 events are excluded from ``EVERYTHING``: they are created during
    handling of other resources in the implicit :doc:`events` from log messages,
    so they would cause unnecessary handling cycles for every essential change.

    To handle core v1 events, they must be named explicitly, e.g. like this:

    .. code-block:: python

        @kopf.on.event('v1', 'events')
        def fn(**_):
            pass

The resource specifications do not support multiple values, masks or globs.
To handle multiple independent resources, add multiple decorators
to the same handler function -- as shown above.
The handlers are deduplicated by the underlying function and its handler id
(which, in turn, equals to the function's name by default unless overridden),
so one function will never be triggered multiple times for the same resource
if there are some accidental overlaps in the specifications.

.. warning::

    Kopf tries to make it easy to specify resources a la ``kubectl``.
    However, some things cannot be made that easy. If resources are specified
    ambiguously, i.e. if 2+ resources of different API groups match the same
    resource specification, neither of them will be served, and a warning
    will be issued.

    This only applies to resource specifications where it is intended to have
    a specific resource by its name; specifications with intentional
    multi-resource mode are served as usually (e.g. by categories).

    However, ``v1`` resources have priority over all other resources. This
    resolves the conflict of ``pods.v1`` vs. ``pods.v1beta1.metrics.k8s.io``,
    so just ``"pods"`` can be specified and the intention will be understood.

    This mimics the behaviour of ``kubectl``, where such API priorities
    are `hard-coded`__.

    __ https://github.com/kubernetes/kubernetes/blob/323f34858de18b862d43c40b2cced65ad8e24052/staging/src/k8s.io/client-go/restmapper/discovery.go#L47-L49

    While it might be convenient to write short forms of resource names,
    the proper way is to always add at least an API group:

    .. code-block:: python

        import kopf

        @kopf.on.event('pods')  # NOT SO GOOD, ambiguous, though works
        @kopf.on.event('pods.v1')  # GOOD, specific
        @kopf.on.event('v1', 'pods')  # GOOD, specific
        @kopf.on.event('pods.metrics.k8s.io')  # GOOD, specific
        @kopf.on.event('metrics.k8s.io', 'pods')  # GOOD, specific
        def fn(**_):
            pass

    Keep the short forms only for prototyping and experimentation mode,
    and for ad-hoc operators with custom resources (not reusable and running
    in controlled clusters where no other similar resources can be defined).

.. warning::

    Some API groups are served by API extensions: e.g. ``metrics.k8s.io``.
    If the extension's deployment/service/pods are down, such a group will
    not be scannable (failing with "HTTP 503 Service Unavailable")
    and will block scanning the whole cluster if resources are specified
    with no group name (e.g. ``('pods')`` instead of ``('v1', 'pods')``).

    To avoid scanning the whole cluster and all (even unused) API groups,
    it is recommended to specify at least the group names for all resources,
    especially in reusable and publicly distributed operators.
