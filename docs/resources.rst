======================
Resource specification
======================

By-name resource selectors
==========================

The following notations are supported to specify the resources to be handled.
As a rule of thumb, they are designed to infer a developer's intentions
as accurately as possible, in a way similar to ``kubectl`` semantics.

The resource name is always expected to be the rightmost positional value.
The remaining parts are considered as an API group and an API version
of the resource --- given as either two separate strings, or as one
separated by a slash:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopf.dev', 'v1', 'kopfexamples')
    @kopf.on.event('kopf.dev/v1', 'kopfexamples')
    @kopf.on.event('apps', 'v1', 'deployments')
    @kopf.on.event('apps/v1', 'deployments')
    @kopf.on.event('', 'v1', 'pods')
    def fn(**_: Any) -> None:
        pass

If only one API specification is given (except for ``v1``), it is treated
as an API group, and the preferred API version of that API group is used:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopf.dev', 'kopfexamples')
    @kopf.on.event('apps', 'deployments')
    def fn(**_: Any) -> None:
        pass

It is also possible to specify the resources with ``kubectl``'s semantics:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopfexamples.kopf.dev')
    @kopf.on.event('deployments.apps')
    def fn(**_: Any) -> None:
        pass

One exceptional case is ``v1`` as the API specification: it corresponds
to K8s's legacy core API (before API groups appeared), and is equivalent
to an empty API group name. The following specifications are equivalent:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('v1', 'pods')
    @kopf.on.event('', 'v1', 'pods')
    def fn(**_: Any) -> None:
        pass

If neither the API group nor the API version is specified,
all resources with that name will match regardless of the API group or version.
However, it is reasonable to expect only one:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopfexamples')
    @kopf.on.event('deployments')
    @kopf.on.event('pods')
    def fn(**_: Any) -> None:
        pass

In all examples above, where a resource identifier is expected, it can be
any name: plural, singular, kind, or a short name. Since it is impossible to guess
which one is which, the name is remembered as-is and later matched against all
possible names of the specific resource once it is discovered:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopfexamples')
    @kopf.on.event('kopfexample')
    @kopf.on.event('KopfExample')
    @kopf.on.event('kex')
    @kopf.on.event('StatefulSet')
    @kopf.on.event('deployments')
    @kopf.on.event('pod')
    def fn(**_: Any) -> None:
        pass

The resource specification can be more specific on which name to match
by using the keyword arguments:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event(kind='KopfExample')
    @kopf.on.event(plural='kopfexamples')
    @kopf.on.event(singular='kopfexample')
    @kopf.on.event(shortcut='kex')
    @kopf.on.event(group='kopf.dev', plural='kopfexamples')
    @kopf.on.event(group='kopf.dev', version='v1', plural='kopfexamples')
    def fn(**_: Any) -> None:
        pass


By-category resource selectors
==============================

Whole categories of resources can be served, but they must be explicitly
specified to avoid unintended consequences:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event(category='all')
    def fn(**_: Any) -> None:
        pass

Note that the conventional category ``all`` does not actually mean all resources,
but only those explicitly added to this category; some built-in resources
are excluded (e.g. ingresses, secrets).


Catch-all resource selectors
============================

To handle all resources in an API group/version, use a special marker instead
of the mandatory resource name:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopf.dev', 'v1', kopf.EVERYTHING)
    @kopf.on.event('kopf.dev/v1', kopf.EVERYTHING)
    @kopf.on.event('kopf.dev', kopf.EVERYTHING)
    def fn(**_: Any) -> None:
        pass

As a consequence of the above, to handle every resource in the cluster
---which might not be the best idea, but is technically possible---
omit the API group/version and use the marker only:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event(kopf.EVERYTHING)
    def fn(**_: Any) -> None:
        pass

Serving everything is better when it is used with filters:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event(kopf.EVERYTHING, labels={'only-this': kopf.PRESENT})
    def fn(**_: Any) -> None:
        pass


Callable resource selectors
===========================

To have fine-grained control over which resources are handled,
you can use a single positional callback as the resource specifier.
It must accept one positional argument of type :class:`kopf.Resource`
and return a boolean indicating whether to handle the resource:

.. code-block:: python

    import kopf
    from typing import Any

    def kex_selector(resource: kopf.Resource) -> bool:
        return resource.plural == 'kopfexamples' and resource.preferred

    @kopf.on.event(kex_selector)
    def fn(**_: Any) -> None:
        pass

You can combine the callable resource selectors with other keyword selectors
(but not the positional by-name or catch-all selectors):

.. code-block:: python

    import kopf
    from typing import Any

    def kex_selector(resource: kopf.Resource) -> bool:
        return resource.plural == 'kopfexamples' and resource.preferred

    @kopf.on.event(kex_selector, group='kopf.dev')
    def fn(**_: Any) -> None:
        pass

There is a subtle difference between callable resource selectors and filters
(see ``when=…`` in :doc:`filters`): a callable filter applies to all events
coming from a live watch stream identified by a resource kind and a namespace
(or by a resource kind alone for watch streams of cluster-wide operators);
a callable resource selector decides whether to start the watch stream
for that resource kind at all, which can help reduce the load on the API.

.. note::
    Normally, Kopf selects only the "preferred" versions of each API group
    when filtered by names. This does not apply to callable selectors.
    To handle non-preferred versions, define a callable and return ``True``
    regardless of the version or its preferred field.


Exclusion of core v1 events
===========================

Core v1 events are excluded from ``EVERYTHING`` and from callable selectors
regardless of what the selector function returns: events are created during
handling of other resources via the implicit :doc:`events` from log messages,
so they would cause unnecessary handling cycles for every meaningful change.

To handle core v1 events, name them directly and explicitly:

.. code-block:: python

    import kopf
    from typing import Any

    def all_core_v1(resource: kopf.Resource) -> bool:
        return resource.group == '' and resource.preferred

    @kopf.on.event(all_core_v1)
    @kopf.on.event('v1', 'events')
    def fn(**_: Any) -> None:
        pass


Multiple resource selectors
===========================

The resource specifications do not support multiple values, masks, or globs.
To handle multiple independent resources, add multiple decorators
to the same handler function ---as shown above--- or use a callable selector.
The handlers are deduplicated by the underlying function and its handler id
(which equals the function's name by default unless overridden),
so a function will never be triggered multiple times for the same resource
even if there are accidental overlaps in the specifications.

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.event('kopfexamples')
    @kopf.on.event('v1', 'pods')
    def fn(**_: Any) -> None:
        pass


Ambiguous resource selectors
============================

.. warning::

    Kopf tries to make it easy to specify resources in the style of ``kubectl``.
    However, some things cannot be made that easy. If resources are specified
    ambiguously --- i.e. if 2 or more resources from different API groups match
    the same resource specification --- neither of them will be served,
    and a warning will be issued.

    This only applies to resource specifications that are intended to match
    a specific resource by name; specifications with intentional
    multi-resource mode are served as usual (e.g. by categories).

    However, ``v1`` resources have priority over all other resources. This
    resolves the conflict between ``pods.v1`` and ``pods.v1beta1.metrics.k8s.io``,
    so just ``"pods"`` can be specified and the intention will be understood.

    This mimics the behavior of ``kubectl``, where such API priorities
    are `hard-coded`__.

    __ https://github.com/kubernetes/kubernetes/blob/323f34858de18b862d43c40b2cced65ad8e24052/staging/src/k8s.io/client-go/restmapper/discovery.go#L47-L49

    While it may be convenient to write short forms of resource names,
    the proper approach is to always include at least an API group:

    .. code-block:: python

        import kopf
        from typing import Any

        @kopf.on.event('pods')  # NOT SO GOOD, ambiguous, though works
        @kopf.on.event('pods.v1')  # GOOD, specific
        @kopf.on.event('v1', 'pods')  # GOOD, specific
        @kopf.on.event('pods.metrics.k8s.io')  # GOOD, specific
        @kopf.on.event('metrics.k8s.io', 'pods')  # GOOD, specific
        def fn(**_: Any) -> None:
            pass

    Reserve short forms for prototyping and experimentation,
    and for ad-hoc operators with custom resources (non-reusable and running
    in controlled clusters where no other similar resources can be defined).

.. warning::

    Some API groups are served by API extensions, e.g. ``metrics.k8s.io``.
    If the extension's deployment/service/pods are down, such a group will
    not be scannable (failing with "HTTP 503 Service Unavailable")
    and will block scanning the entire cluster if resources are specified
    without a group name (e.g. ``('pods')`` instead of ``('v1', 'pods')``).

    To avoid scanning the entire cluster and all (even unused) API groups,
    it is recommended to specify at least the group name for all resources,
    especially in reusable and publicly distributed operators.
