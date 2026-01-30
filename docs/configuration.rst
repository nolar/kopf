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

For more settings, see :class:`kopf.OperatorSettings` and :kwarg:`settings`.


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

``settings.posting`` allows to control which log messages should be posted as
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

These two settings also affect :func:`kopf.event` and related functions:
:func:`kopf.info`, :func:`kopf.warn`, :func:`kopf.exception` --
even if they are called explicitly in the code.

By default, all log messages made by the handlers on their ``logger`` are also
posted as Kubernetes events. This can be disabled if it is not desired,
e.g. to keep the events list clean, so that only the explicit event-posting
calls are posted:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.loggers = False


.. _configure-sync-handlers:

Synchronous handlers
====================

``settings.execution`` allows setting the number of synchronous workers used
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
If you expect a large number of synchronous daemons (e.g. for large clusters),
make sure to pre-scale the executor accordingly
(the default in Python is 5x times the CPU cores):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    async def configure(settings: kopf.OperatorSettings, **kwargs):
        settings.execution.max_workers = 1000


Networking timeouts
===================

Timeouts can be controlled when communicating with Kubernetes API:

``settings.networking.request_timeout`` (seconds) is how long a regular
request should take before failing. This applies to all atomic requests --
cluster scanning, resource patching, etc. -- except the watch-streams.
The default is 5 minutes (300 seconds).

``settings.networking.connect_timeout`` (seconds) is how long a TCP handshake
can take for regular requests before failing. There is no default (``None``),
meaning that there is no timeout specifically for this; however, the handshake
is limited by the overall time of the request.

``settings.watching.connect_timeout`` (seconds) is how long a TCP handshake
can take for watch-streams before failing. There is no default (``None``),
which means that ``settings.networking.connect_timeout`` is used if set.
If not set, then ``settings.networking.request_timeout`` is used.

.. note::

    With the current aiohttp-based implementation, both connection timeouts
    correspond to ``sock_connect=`` timeout, not to ``connect=`` timeout,
    which would also include the time for getting a connection from the pool.
    Kopf uses unlimited aiohttp pools, so this should not be a problem.

``settings.watching.server_timeout`` (seconds) is how long the session
with a watching request will exist before closing it from the **server** side.
This value is passed to the server-side in a query string, and the server
decides on how to follow it. The watch-stream is then gracefully closed.
The default is to use the server setup (``None``).

``settings.watching.client_timeout`` (seconds) is how long the session
with a watching request will exist before closing it from the **client** side.
This includes establishing the connection and event streaming.
The default is forever (``None``).

It makes no sense to set the client-side timeout shorter than the server-side
timeout, but it is given to the developers' responsibility to decide.

The server-side timeouts are unpredictable, they can be 10 seconds or
10 minutes. Yet, it feels wrong to assume any "good" values in a framework
(especially since it works without timeouts defined, just produces extra logs).

.. warning::
    Some setups that involve any kind of a load balancer (LB), such as
    the cloud-hosted Kubernetes clusters, have a well-known problem
    of freezing and going silent for no reason if nothing happens
    in the cluster for some time. The best guess is that the connection
    operator<>LB remains alive, while the connection LB<>K8s closes.
    Kopf-based operators remain unaware of this disruption.

    Setting either the client or the server timeout solves the problem
    of waking up from such freezes, but at the cost of regular reconnections
    in the normal flow of operations. There is no good default value either,
    you should guess it experimentally based on your operational requirements,
    cluster size and activity level, usually in the range 1-10 minutes.

``settings.watching.reconnect_backoff`` (seconds) is a backoff interval between
watching requests -- to prevent API flooding in case of errors or disconnects.
The default is 0.1 seconds (nearly instant, but not flooding).

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.networking.connect_timeout = 10
        settings.networking.request_timeout = 60
        settings.watching.server_timeout = 10 * 60


.. _consistency:

Consistency
===========

Generally, Kopf processes the resource events and updates streamed
from the Kubernetes API as soon as possible, with no delays or skipping.
However, high-level change-detection handlers (creation/resume/update/deletion)
require a consistent state of the resource. _Consistency_ means that all
patches applied by Kopf itself have arrived back via the watch-stream.
If Kopf did not patch the resource recently, it is consistent by definition.

The _inconsistent_ states can happen in relatively rare circumstances
on slow networks (with high latency between operator and api-servers)
or under high load (high number of resources or changes), especially when
an unrelated application or another operator patches the resources on their own.

Handling the _inconsistent_ states could cause double-processing
(i.e. double handler execution) and some other undesired side effects.
To prevent handling the inconsistent states, all state-dependent handlers
wait the _consistency_ is reached via one of the following two ways:

* The expected resource version from the PATCH API operation arrives
  via the watch-stream of the resource within the specified time window.
* The expected resource version from the PATCH API operation does not arrive
  via the watch-stream within the specified time window, in which case
  Kopf assumes the consistency after the time window ends,
  and the processing continues as if the version has arrived,
  possibly causing the mentioned side-effects.

The time window is measured relative to the time of the latest ``PATCH`` call.
The timeout should be long enough to assume that if the expected resource
version did not arrive within the specified time, it will never arrive.

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.consistency_timeout = 10

The default value (5 seconds) aims to the safest scenario out of the box.

The value of ``0`` will effectively disable the consistency tracking
and declare all resource states as consistent -- even if they are not.
Use this with care -- e.g., with self-made persistence storages instead of
Kopf's annotations (see :ref:`progress-storing` and :ref:`diffbase-storing`).

The consistency timeout does not affect low-level handlers with no persistence,
such as ``@kopf.on.event``, ``@kopf.index``, ``@kopf.daemon``, ``@kopf.timer``
-- these handlers run for each and every watch-event with no delay
(if they match the :doc:`filters <filters>`, of course).


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

To keep the handling state across multiple handling cycles, and to be resilient
to errors and tolerable to restarts and downtimes, the operator keeps its state
in a configured state storage. See more in :doc:`continuity`.

To store the state only in the annotations with a preferred prefix:

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
For this, inherit from :class:`kopf.ProgressStorage` and implement its abstract
methods (``fetch()``, ``store()``, ``purge()``, optionally ``flush()``).

.. note::

    The legacy behavior is an equivalent of
    ``kopf.StatusProgressStorage(field='status.kopf.progress')``.

    Starting with Kubernetes 1.16, both custom and built-in resources have
    strict structural schemas with the pruning of unknown fields
    (more information is in `Future of CRDs: Structural Schemas`__).

    __ https://kubernetes.io/blog/2019/06/20/crd-structural-schema/

    Long story short, unknown fields are silently pruned by Kubernetes API.
    As a result, Kopf's status storage will not be able to store
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


.. _diffbase-storing:

Change detection
================

For change-detecting handlers, Kopf keeps the last handled configuration --
i.e. the last state that has been successfully handled. New changes are compared
against the last handled configuration, and a diff list is formed.

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
            prefix='kopf.zalando.org',
            key='last-handled-configuration',
        )

The stored content is a JSON-serialised essence of the object (i.e., only
the important fields, with system fields and status stanza removed).

It is generally not a good idea to override this store unless multiple
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

To ensure a smooth transition, use a composite multi-storage, with the
new storage as a first child, and the old storage as the second child
(both are used for writing, the first found value is used for reading).

For example, to eventually switch from Kopf's annotations to a status field
for diff-base storage, apply this configuration:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.persistence.diffbase_storage = kopf.MultiDiffBaseStorage([
            kopf.StatusDiffBaseStorage(field='status.diff-base'),
            kopf.AnnotationsDiffBaseStorage(prefix='kopf.zalando.org', key='last-handled-configuration'),
        ])

Run the operator for some time. Let all resources change or force this:
e.g. by arbitrarily labelling them, so that a new diff-base is generated:

.. code-block:: shell

    kubectl label kex -l somelabel=somevalue  ping=pong

Then, switch to the new storage alone, without the transitional setup.


Cluster discovery
=================

``settings.scanning.disabled`` controls the cluster discovery at runtime.

If enabled (the default), then the operator will try to observe
the namespaces and custom resources, and will gracefully start/stop
the watch streams for them (also the peering activities, if applicable).
This requires the RBAC permissions to list/watch the V1 namespaces and CRDs.

If disabled or if enabled but the permission is not granted, then only
the specific namespaces will be served, with namespace patterns ignored;
and only the resources detected at startup will be served, with added CRDs
or CRD versions being ignored, and the deleted CRDs causing failures.

The default mode is good enough for most cases, unless the strict
(non-dynamic) mode is intended -- to prevent the warnings in the logs.

If you have very restrictive cluster permissions, disable the cluster discovery:

.. code-block:: python

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.scanning.disabled = True


.. _api-retrying:

Retrying of API errors
======================

In some cases, the Kubernetes API servers might be not ready on startup
or occasionally at runtime; the network might have issues too. In most cases,
these issues are of temporary nature and heal themselves withing seconds.

The framework retries the TCP/SSL networking errors and the HTTP 5xx errors
("the server is wrong") --- i.e. everything that is presumed to be temporary;
other errors -- those presumed to be permanent, including HTTP 4xx errors
("the client is wrong") -- escalate immediately without retrying.

The setting ``settings.networking.error_backoffs`` controls for how many times
and with which backoff interval (in seconds) the retries are performed.

It is a sequence of back-offs between attempts (in seconds):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.networking.error_backoffs = [10, 20, 30]

Note that the number of attempts is one more than the number of back-off
intervals (because the back-offs happen inbetween the attempts).

A single integer or float value means a single backoff, i.e. 2 attempts:
``(1.0)`` is equivalent to ``(1.0,)`` or ``[1.0]`` for convenience.

To have a uniform back-off delay D with N+1 attempts, set to ``[D] * N``.

To disable retrying (on your own risk), set it to ``[]`` or ``()``.

The default value covers roughly a minute of attempts before giving up.

Once the retries are over (if disabled, immediately on error), the API errors
escalate and are then handled according to :ref:`error-throttling`.

This value can be an arbitrary collection or an iterable object (even infinite):
only ``iter()`` is called on every new retrying cycle, no other protocols
are required; however, make sure that it is re-iterable for multiple uses:

.. code-block:: python

    import kopf
    import random

    class InfiniteBackoffsWithJitter:
        def __iter__(self):
            while True:
                yield 10 + random.randint(-5, +5)

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.networking.error_backoffs = InfiniteBackoffsWithJitter()


Retrying an API error blocks the task or the object's worker in which
the API error happens. However, other objects and tasks run normally
in parallel (unless they hit the same error in the same cluster).

Every further consecutive error leads to the next, typically bigger backoff.
Every success resets the backoff intervals, and it goes from the beginning
on the next error.

.. note::

    The format is the same as for ``settings.queueing.error_delays``.
    The only difference: if the API operation does not succeed by the end
    of the sequence, the error of the last attempt escalates instead of blocking
    and retrying forever with the last delay in the sequence.

.. seealso::
    These back-offs cover only the server-side and networking errors.
    For errors in handlers, see :doc:`/errors`.
    For errors in the framework, see :ref:`error-throttling`.


.. _error-throttling:

Throttling of unexpected errors
===============================

To prevent an uncontrollable flood of activities in case of errors that prevent
the resources being marked as handled, which could lead to the Kubernetes API
flooding, it is possible to throttle the activities on a per-resource basis:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.queueing.error_delays = [10, 20, 30]

In that case, all unhandled errors in the framework or in the Kubernetes API
would be backed-off by 10s after the 1st error, then by 20s after the 2nd one,
and then by 30s after the 3rd, 4th, 5th errors and so on. On the first success,
the backoff intervals will be reset and re-used again on the next error.

The default is a sequence of Fibonacci numbers from 1 second to 10 minutes.

The back-offs are not persisted, so they are lost on the operator restarts.

These back-offs do not cover errors in the handlers -- the handlers have their
own per-handler back-off intervals. These back-offs are for Kopf's own errors.

To disable throttling (on your own risk), set it to ``[]`` or ``()``.
Interpret it as: no throttling delays set --- no throttling sleeps done.

If needed, this value can be an arbitrary collection/iterator/object:
only ``iter()`` is called on every new throttling cycle, no other protocols
are required; but make sure that it is re-iterable for multiple uses.


Log levels & filters
====================

In case the logs of any component are too exessive, or contain secret data,
this can be controlled with the usual Python logging machinery.

For example, to disable the access logs of the probing server:

.. code-block:: python

    import logging

    @kopf.on.startup()
    async def configure(**_):
        logging.getLogger('aiohttp.access').propagate = False

To selectively filter only some log messages but not the others:

.. code-block:: python

    import logging
    import kopf

    class ExcludeProbesFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return 'GET /healthz ' not in record.getMessage()

    @kopf.on.startup()
    async def configure_access_logs(**_):
        logging.getLogger('aiohttp.access').addFilter(ExcludeProbesFilter())

For more information on the logging configuration, see:
`logging <https://docs.python.org/3/library/logging.html>`_.

In particular, you can use the special logger ``kopf.objects`` to filter
the object-related log messages coming from the :kwarg:`logger` and from
Kopf's internals, which are then posted as Kubernetes events (``v1/events``):

.. code-block:: python

    import logging
    import kopf

    class ExcludeKopfInternals(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return '/kopf/' not in record.pathname

    @kopf.on.startup()
    async def configure_kopf_logs(**_):
        logging.getLogger('kopf.objects').addFilter(ExcludeKopfInternals())

.. warning::
    Beware: the path names and module names of internal modules,
    so as the extra fields of ``logging.LogRecord`` added by Kopf,
    can change without warning, do not rely on their stability.
    They are not a public interface of Kopf.
