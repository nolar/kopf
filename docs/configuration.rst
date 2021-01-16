=============
Configuration
=============

It is possible to fine-tune some aspects of Kopf-based operators,
like timeouts, synchronous handler pool sizes, automatic Kubernetes Event
creation from object-related log messages, etc.


Startup configuration
=====================

Every operator has its settings (even if there is more than one operator
in the same processes, e.g. due to :doc:`embedding`). The settings affect
how the framework behaves in details.

The settings can be modified in the startup handlers (see :doc:`startup`):

.. code-block:: python

    import kopf
    import logging

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = logging.WARNING
        settings.watching.connect_timeout = 1 * 60
        settings.watching.server_timeout = 10 * 60

All the settings have reasonable defaults, so the configuration should be used
only for fine-tuning when and if necessary.

For more settings, see `kopf.OperatorSettings` and :kwarg:`settings` kwarg.


Logging formats and levels
==========================

The following log formats are supported on CLI:

* Full logs (the default) -- with timestamps, log levels, and logger names:

    .. code-block:: bash

        kopf run -v --log-format=full

    .. code-block:: console

        [2019-11-04 17:49:25,365] kopf.reactor.activit [INFO    ] Initial authentication has been initiated.
        [2019-11-04 17:49:25,650] kopf.objects         [DEBUG   ] [default/kopf-example-1] Resuming is in progress: ...

* Plain logs, with only the message:

    .. code-block:: bash

        kopf run -v --log-format=plain

    .. code-block:: console

        Initial authentication has been initiated.
        [default/kopf-example-1] Resuming is in progress: ...

  For non-JSON logs, the object prefix can be disabled to make the logs
  completely flat (as in JSON logs):

    .. code-block:: bash

        kopf run -v --log-format=plain --no-log-prefix

    .. code-block:: console

        Initial authentication has been initiated.
        Resuming is in progress: ...

* JSON logs, with only the message:

    .. code-block:: bash

        kopf run -v --log-format=json

    .. code-block:: console

        {"message": "Initial authentication has been initiated.", "severity": "info", "timestamp": "2020-12-31T23:59:59.123456"}
        {"message": "Resuming is in progress: ...", "object": {"apiVersion": "kopf.dev/v1", "kind": "KopfExample", "name": "kopf-example-1", "uid": "...", "namespace": "default"}, "severity": "debug", "timestamp": "2020-12-31T23:59:59.123456"}

  For JSON logs, the object reference key can be configured to match
  the log parsers (if used) -- instead of the default ``"object"``:

    .. code-block:: bash

        kopf run -v --log-format=json --log-refkey=k8s-obj

    .. code-block:: console

        {"message": "Initial authentication has been initiated.", "severity": "info", "timestamp": "2020-12-31T23:59:59.123456"}
        {"message": "Resuming is in progress: ...", "k8s-obj": {...}, "severity": "debug", "timestamp": "2020-12-31T23:59:59.123456"}

    Note that the object prefixing is disabled for JSON logs by default, as the
    identifying information is available in the ref-keys. The prefixing can be
    explicitly re-enabled if needed:

    .. code-block:: bash

        kopf run -v --log-format=json --log-prefix

    .. code-block:: console

        {"message": "Initial authentication has been initiated.", "severity": "info", "timestamp": "2020-12-31T23:59:59.123456"}
        {"message": "[default/kopf-example-1] Resuming is in progress: ...", "object": {...}, "severity": "debug", "timestamp": "2020-12-31T23:59:59.123456"}

.. note::

    Logging verbosity and formatting are only configured via CLI options,
    not via ``settings.logging`` as all other aspects of configuration.
    When the startup handlers happen for ``settings``, it is too late:
    some initial messages could be already logged in the existing formats,
    or not logged when they should be due to verbosity/quietness levels.


Logging events
==============

``settings.posting`` allows to control which log messages should be post as
Kubernetes events. Use ``logging`` constants or integer values to set the level:
e.g., ``logging.WARNING``, ``logging.ERROR``, etc.
The default is ``logging`.INFO``.

.. code-block:: python

    import logging
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = logging.ERROR

The event-posting can be disabled completely (the default is to be enabled):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.enabled = False

.. note::

    These settings also affect `kopf.event` and related functions:
    `kopf.info`, `kopf.warn`, `kopf.exception`, etc --
    even if they are called explicitly in the code.

    To avoid these settings having impact on your code, post events
    directly with an API client library instead of Kopf-provided toolkit.


.. _configure-sync-handlers:

Synchronous handlers
====================

``settings.execution`` allows to set the number of synchronous workers used
by the operator for synchronous handlers, or replace the asyncio executor
with another one:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.execution.max_workers = 20


It is possible to replace the whole asyncio executor used
for synchronous handlers (see :doc:`async`).

Please note that the handlers that started in a previous executor, will be
continued and finished with their original executor. This includes the startup
handler itself. To avoid it, make the on-startup handler asynchronous:

.. code-block:: python

    import concurrent.futures
    import kopf

    @kopf.on.startup()
    async def configure(settings: kopf.OperatorSettings, **_):
        settings.execution.executor = concurrent.futures.ThreadPoolExecutor()

The same executor is used both for regular sync handlers and for sync daemons.
If you expect large number of synchronous daemons (e.g. for large clusters),
make sure to pre-scale the executor accordingly
(the default in Python is 5x times the CPU cores):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    async def configure(settings: kopf.OperatorSettings, **kwargs):
        settings.execution.max_workers = 1000


API timeouts
============

Few timeouts can be controlled when communicating with Kubernetes API:

``settings.watching.server_timeout`` (seconds) is how long the session
with a watching request will exist before closing it from the **server** side.
This value is passed to the server side in a query string, and the server
decides on how to follow it. The watch-stream is then gracefully closed.
The default is to use the server setup (``None``).

``settings.watching.client_timeout`` (seconds) is how long the session
with a watching request will exist before closing it from the **client** side.
This includes the connection establishing and event streaming.
The default is forever (``None``).

``settings.watching.connect_timeout`` (seconds) is how long a connection
can be established before failing. (With current aiohttp-based implementation,
this corresponds to ``sock_connect=`` timeout, not to ``connect=`` timeout,
which would also include the time for getting a connection from the pool.)

It makes no sense to set the client-side timeout shorter than the server side
timeout, but it is given to the developers' responsibility to decide.

The server-side timeouts are unpredictable, they can be 10 seconds or
10 minutes. Yet, it feels wrong to assume any "good" values in a framework
(especially since it works without timeouts defined, just produces extra logs).

``settings.watching.reconnect_backoff`` (seconds) is a backoff interval between
watching requests -- in order to prevent API flooding in case of errors
or disconnects. The default is 0.1 seconds (nearly instant, but not flooding).

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.watching.server_timeout = 10 * 60


Finalizers
==========

A resource is blocked from deletion if the framework believes it is safer
to do so, e.g. if non-optional deletion handlers are present
or if daemons/timers are running at the moment.

For this, a finalizer_ is added to the object. It is removed when the framework
believes it is safe to release the object for actual deletion.

.. _finalizer: https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#finalizers

The name of the finalizer can be configured:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.finalizer = 'my-operator.example.com/kopf-finalizer'

The default is the one that was hard-coded before:
``kopf.zalando.org/KopfFinalizerMarker``.


.. _progress-storing:

Handling progress
=================

In order to keep the handling state across multiple handling cycles, and to be
resilient to errors and tolerable to restarts and downtimes, the operator keeps
its state in a configured state storage. See more in :doc:`continuity`.

To store the state only in the annotations with your own prefix:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.progress_storage = kopf.AnnotationsProgressStorage(prefix='my-op.example.com')

To store the state only in the status or any other field:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.progress_storage = kopf.StatusProgressStorage(field='status.my-operator')

To store in multiple places (stored in sync, but the first found state will be
used when fetching, i.e. the first storage has precedence):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.progress_storage = kopf.MultiProgressStorage([
            kopf.AnnotationsProgressStorage(prefix='my-op.example.com'),
            kopf.StatusProgressStorage(field='status.my-operator'),
        ])

The default storage is at both annotations and status, with annotations having
precedence over the status (this is done as a transitioning solution
from status-only storage in the past to annotations-only storage in the future).
The annotations are ``kopf.zalando.org/{id}``,
the status fields are ``status.kopf.progress.{id}``.
It is an equivalent of:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.progress_storage = kopf.SmartProgressStorage()

It is also possible to implement custom state storage instead of storing
the state directly in the resource's fields -- e.g., in external databases.
For this, inherit from `kopf.ProgressStorage` and implement its abstract methods
(``fetch()``, ``store()``, ``purge()``, optionally ``flush()``).

.. note::

    The legacy behavior is an equivalent of
    ``kopf.StatusProgressStorage(field='status.kopf.progress')``.

    Starting with Kubernetes 1.16, both custom and built-in resources have
    strict structural schemas with pruning of unknown fields
    (more information is in `Future of CRDs: Structural Schemas`__).

    __ https://kubernetes.io/blog/2019/06/20/crd-structural-schema/

    Long story short, unknown fields are silently pruned by Kubernetes API.
    As a result, Kopf's status storage will not be able to actually store
    anything in the resource, as it will be instantly lost.
    (See `#321 <https://github.com/zalando-incubator/kopf/issues/321>`_.)

    To quickly fix this for custom resources, modify their definitions
    with ``x-kubernetes-preserve-unknown-fields: true``. For example:

    .. code-block:: yaml

        apiVersion: apiextensions.k8s.io/v1
        kind: CustomResourceDefinition
        spec:
          scope: ...
          group: ...
          names: ...
          versions:
            - name: v1
              served: true
              storage: true
              schema:
                openAPIV3Schema:
                  type: object
                  x-kubernetes-preserve-unknown-fields: true

    See a more verbose example in ``examples/crd.yaml``.

    For built-in resources, such as pods, namespaces, etc, the schemas cannot
    be modified, so a full switch to annotations storage is advised.

    The new default "smart" storage is supposed to ensure a smooth upgrade
    of Kopf-based operators to the new state location without special upgrade
    actions or conversions needed.


Change detection
================

For change-detecting handlers, Kopf keeps the last handled configuration --
i.e. the last state that has been successfully handled. New changes are compared
against the last handled configuration, and a diff is formed.

The last-handled configuration is also used to detect if there were any
essential changes at all -- i.e. not just the system or status fields.

The last-handled configuration storage can be configured
with ``settings.persistence.diffbase_storage``.
The default is an equivalent of:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(
            name='kopf.zalando.org/last-handled-configuration',
        )

The stored content is a JSON-serialised essence of the object (i.e., only
the important fields, with system fields and status stanza removed).

It is generally not a good idea to override this store, unless multiple
Kopf-based operators must handle the same resources, and they should not
collide with each other. In that case, they must take different names.


Storage transition
==================

.. warning::

    Changing a storage method for an existing operator with existing resources
    is dangerous: the operator will consider all those resources
    as not handled yet (due to absence of a diff-base key) or will loose
    their progress state (if some handlers are retried or slow). The operator
    will start handling each of them again -- which can lead to duplicated
    children or other side-effects.

To ensure smooth transition, use a composite multi-storage, with the
new storage as a first child, and the old storage as the second child
(both are used for writing, the first found value is used for reading).

For example, to eventually switch from Kopf's annotations to a status field
for diff-base storage, apply this configuration:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.diffbase_storage = kopf.MiltiDiffBaseStorage([
            kopf.StatusDiffBaseStorage(field='status.diff-base'),
            kopf.AnnotationsDiffBaseStorage('kopf.zalando.org/last-handled-configuration'),
        ])

Run the operator for some time. Let all resources to change or force this:
e.g. by arbitrarily labelling them, so that a new diff-base is generated:

.. code-block:: shell

    kubectl label kex -l somelabel=somevalue  ping=pong

Then, switch to the new storage alone, without the transitional setup.


Error throttling
================

To prevent uncontrollable flood of activities in case of errors that prevent
the resources being marked as handled, which could lead to Kubernetes API
flooding, it is possible to throttle the activities on a per-resource basis:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.batching.error_delays = [10, 20, 30]

In that case, all unhandled errors in the framework or in the Kubernetes API
would be backed-off by 10s after the 1st error, then by 20s after the 2nd one,
and then by 30s after the 3rd, 4th, 5th errors and so on. On a first success,
the backoff intervals will be reset and re-used again on the next error.

The default is a sequence of Fibonacci numbers from 1 second to 10 minutes.

The back-offs are not persisted, so they are lost on the operator restarts.

These back-offs do not cover errors in the handlers -- the handlers have their
own configurable per-handler back-off intervals. These back-offs are for Kopf
and for Kubernetes API mostly (and other environment issues).

To disable throttling (on your own risk!), set the error delays to
an empty list (``[]``) or an empty tuple (``()``).
Interpret as: no throttling delays set -- no throttling sleeps done.
