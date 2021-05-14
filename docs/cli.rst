====================
Command-line options
====================

Most of the options relate to ``kopf run``, though some are shared by other
commands, such as ``kopf freeze`` and ``kopf resume``.


Scripting options
=================

.. option:: -m, --module

    A semantical equivalent to ``python -m`` --- which importable modules
    to import on startup.


Logging options
===============

.. option:: --quiet

    Be quiet: only show warnings and errors, but not the normal processing logs.

.. option:: --verbose

    Show what Kopf is doing, but hide the low-level asyncio & aiohttp logs.

.. option:: --debug

    Extremely verbose: log all the asyncio internals too, so as the API traffic.

.. option:: --log-format (plain|full|json)

    See more in :doc:`/configuration`.

.. option:: --log-prefix, --no-log-prefix

    Whether to prefix all object-related messages with the name of the object.
    By default, the prefixing is enabled.

.. option:: --log-refkey

    For JSON logs, under which top-level key to put the object-identifying
    information, such as its name, namespace, etc.


Scope options
=============

.. option:: -n, --namespace

    Serve this namespace or all namespaces mathing the pattern
    (or excluded from patterns). The option can be repeated multiple times.

    .. seealso::
        :doc:`/scopes` for the pattern syntax.

.. option:: -A, --all-namespaces

    Serve the whole cluster. This is different from ``--namespace *``:
    with ``--namespace *``, the namespaces are monitored, and every resource
    in every namespace is watched separately, starting and stopping as needed;
    with ``--all-namespaces``, the cluster endpoints of the Kubernetes API
    are used for resources, the namespaces are not monitored.


Probing options
===============

.. option:: --liveness

    The endpoint where to serve the probes and health-checks.
    E.g. ``http://0.0.0.0:1234/``. Only ``http://`` is currently supported.
    By default, the probing endpoint is not served.

.. seealso::
    :doc:`/probing`


Peering options
===============

.. option:: --standalone

    Disable any peering or auto-detection of peering. Run strictly as if
    this is the only instance of the operator.

.. option:: --peering

    The name of the peering object to use. Depending on the operator's scope
    (:option:`--all-namespaces` vs. :option:`--namespace`, see :doc:`/scopes`),
    it is either ``kind: KopfPeering`` or ``kind: ClusterKopfPeering``.

    If specified, the operator will not run until that peering exists
    (for the namespaced operators, until it exists in each served namespace).

    If not specified, the operator checks for the name "default" and uses it.
    If the "default" peering is absent, the operator runs in standalone mode.

.. option:: --priority

    Which priority to use for the operator. An operator with the highest
    priority wins the peering competitions and handlers the resources.

    The default priority is ``0``; :option:`--dev` sets it to ``666``.

.. seealso::
    :doc:`/peering`


Development mode
================

.. option:: --dev

    Run in the development mode. Currently, this implies ``--priority=666``.
    Other meanings can be added in the future, such as automatic reloading
    of the source code.
