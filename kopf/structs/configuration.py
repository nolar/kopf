"""
All configuration flags, options, settings to fine-tune an operator.

All settings are grouped semantically just for convenience
(instead of a flat mega-object with all the values in it).

The individual groups or settings can eventually be moved or regrouped within
the root object, while keeping the legacy names for backward compatibility.

.. note::

    There is a discussion on usage of such words as "configuration",
    "preferences", "settings", "options", "properties" in the internet:

    * https://stackoverflow.com/q/2074384/857383
    * https://qr.ae/pNvt40
    * etc.

    In this framework, they are called *"settings"* (plural).
    Combined, they form a *"configuration"* (singular).

    Some of the settings are flags, some are scalars, some are optional,
    some are not (but all of them have reasonable defaults).

    Regardless of the exact class and module names, all of these terms can be
    used interchangeably -- but so that it is understandable what is meant.
"""
import concurrent.futures
import dataclasses
import logging
from typing import Iterable, Optional

from kopf.storage import diffbase, progress
from kopf.structs import reviews


@dataclasses.dataclass
class ProcessSettings:
    """
    Settings for Kopf's OS processes: e.g. when started via CLI as `kopf run`.
    """

    ultimate_exiting_timeout: Optional[float] = 10 * 60
    """
    How long to wait for the graceful exit before SIGKILL'ing the operator.

    This is the last resort to make the operator exit instead of getting stuck
    at exiting due to the framework's bugs, operator's bugs, threads left,
    daemons not exiting, etc.
    
    The countdown goes from when a graceful signal arrives (SIGTERM/SIGINT),
    regardless of what is happening in the graceful exiting routine.

    Measured in seconds. Set to `None` to disable (on your own risk).
    
    The default is 10 minutes -- high enough for all common sense cases,
    and higher than K8s pods' ``terminationGracePeriodSeconds`` -- 
    to let K8s kill the operator's pod instead, if it can.
    """


@dataclasses.dataclass
class PostingSettings:

    enabled: bool = True
    """
    Should the log messages be sent as Kubernetes Events for an object.
    The events can be seen in ``kubectl describe`` output for the object.

    This also affects ``kopf.event()`` and similar functions
    (``kopf.info()``, ``kopf.warn()``, ``kopf.exception()``).
    """

    level: int = logging.INFO
    """
    A minimal level of logging events that will be posted as K8s Events.
    The default is ``logging.INFO`` (i.e. all info, warning, errors are posted).

    This also affects ``kopf.event()`` and similar functions
    (``kopf.info()``, ``kopf.warn()``, ``kopf.exception()``).
    """


@dataclasses.dataclass
class PeeringSettings:

    name: str = 'default'
    """
    The name of the peering object to use.
    Distinct peering objects are isolated peering neighbourhoods,
    i.e. operators in one of them are not visible to operators in others.
    """

    stealth: bool = False
    """
    Should this operator log its keep-alives?
    
    In some cases, it might be undesired to log regular keep-alives while
    they actually happen (to keep the logs clean and readable).

    Note that some occasions are logged unconditionally: 
    
    * those affecting the operator's behaviour, such as pauses/resumes;
    * those requiring human intervention, such as absence of a peering object
      in the auto-detection mode (to make the peering mandatory or standalone). 
    """

    priority: int = 0
    """
    The operator's priority to use. The operators with lower priority pause
    when they see operators with higher or the same priority --
    to avoid double-processing and double-handling of the resources.
    """

    lifetime: int = 60
    """
    For how long (in seconds) the operator's record is considered actual
    by other operators before assuming that the corresponding operator
    is not functioning and the paused state should be re-evaluated.

    The peered operators will update their records as long as they are running,
    slightly faster than their records expires (5-10 seconds earlier).

    Note that it is the lifetime of the current operator. For operators that
    do not communicate their lifetime (broken?), it is always assumed to be
    60 seconds regardless of this operator's configuration (a hard-coded value).
    """

    mandatory: bool = False
    """
    Is peering mandatory for this operator, or optional? If it is mandatory,
    the operator will fail to run in the absence of the peering CRD or object.
    If optional, it will continue in the standalone mode (i.e. without peering).
    """

    standalone: bool = False
    """
    Should the operator be forced to run without peering even if it exists?
    Normally, operator automatically detect the peering objects named "default",
    and run with peering enabled if they exist, or standalone if absent.
    But they can be forced to either mode (standalone or mandatory peering).
    """

    clusterwide: bool = False
    """
    Should the peering be clusterwide or namespaced?

    Usually, the peering has the same mode as the operator, using the peering
    objects either in the served namespaces or cluster-wide accordingly.

    In exceptional cases, the peering can mismatch the operator's mode:
    e.g. using the cluster-wide peering while the operator is namespaced.
    """

    @property
    def namespaced(self) -> bool:
        """ An inverse of ``clusterwide``, for code readability. """
        return not self.clusterwide

    @namespaced.setter
    def namespaced(self, value: bool) -> None:
        self.clusterwide = not value


@dataclasses.dataclass
class WatchingSettings:

    server_timeout: Optional[float] = None
    """
    The maximum duration of one streaming request. Patched in some tests.
    If ``None``, then obey the server-side timeouts (they seem to be random).
    """

    client_timeout: Optional[float] = None
    """
    An HTTP/HTTPS session timeout to use in watch requests.
    """

    connect_timeout: Optional[float] = None
    """
    An HTTP/HTTPS connection timeout to use in watch requests.
    """
    
    reconnect_backoff: float = 0.1
    """
    How long should a pause be between watch requests (to prevent API flooding).
    """


@dataclasses.dataclass
class BatchingSettings:
    """
    Settings for how raw events are batched and processed.
    """

    worker_limit: Optional[int] = None
    """
    How many workers can be running simultaneously on per-object event queue.
    If ``None``, there is no limit to the number of workers (as many as needed).
    """

    idle_timeout: float = 5.0
    """
    How soon an idle worker is exited and garbage-collected if no events arrive.
    """

    batch_window: float = 0.1
    """
    How fast/slow does a worker deplete the queue when an event is received.
    All events arriving within this window will be ignored except the last one.
    """

    exit_timeout: float = 2.0
    """
    How soon a worker is cancelled when the parent watcher is going to exit.
    This is the time given to the worker to deplete and process the queue.
    """

    error_delays: Iterable[float] = (1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610)
    """
    Backoff intervals in case of unexpected errors in the framework (not the handlers).

    Per-resource workers freeze all activities for this number of seconds in case of errors.
    Once they are back to work, they process only the latest event seen (due to event batching).

    Every further error leads to the next, even bigger delay (10m is enough for a default maximum).
    Every success resets the backoff intervals, and it goes from the beginning on the next error.

    If needed, this value can be an arbitrary collection/iterator/object:
    only ``iter()`` is called on every new throttling cycle, no other protocols
    are required; but make sure that it is re-iterable for multiple uses.
    
    To disable throttling (on your own risk), set it to ``[]`` or ``()``.
    """


@dataclasses.dataclass
class ScanningSettings:
    """
    Settings for dynamic runtime observation of the cluster's setup.
    """

    disabled: bool = False
    """
    Should the cluster's dynamic monitoring for resources/namespaces be off?

    If enabled (the default), then the operator will try to observe
    the namespaces and custom resources, and will gracefully start/stop
    the watch streams for them (also the peering activities, if applicable).
    This requires RBAC permissions to list/watch the V1 namespaces and CRDs.

    If disabled or if enabled but the permission is not granted, then only
    the specific namespaces will be served, with namespace patterns ignored;
    and only the resources detected at startup will be served, with added CRDs
    or CRD versions being ignored, and the deleted CRDs causing failures.
    
    The default mode is good enough for most cases, unless the strict
    (non-dynamic) mode is intended -- to prevent the warnings in the logs.
    """


@dataclasses.dataclass
class AdmissionSettings:

    server: Optional[reviews.WebhookServerProtocol] = None
    """
    A way of accepting admission requests from Kubernetes.

    In production, only a `kopf.WebhookServer` is sufficient.
    If development, a tunnel from the cluster to the operator might be needed.

    If no server is configured (the default), then no server is started.
    If admission handlers are detected with no server configured,
    an error is raised and the operator fails to start (with a hint).

    Kopf provides several webhook configs, servers, and tunnels out of the box
    (they also serve as examples for implementing custom tunnels).
    `kopf.WebhookServer`,
    `kopf.WebhookK3dServer`, `kopf.WebhookMinikubeServer`,
    `kopf.WebhookNgrokTunnel`, `kopf.WebhookInletsTunnel`.
    
    .. seealso::
        :doc:`/admission`.
    """

    managed: Optional[str] = None
    """
    The names of managed ``[Validating/Mutating]WebhookConfiguration`` objects.

    If not set (the default), Kopf does not manage the configuration and
    expects that the requests come from a manually pre-created configuration.

    If set, Kopf creates the validating/mutating configurations objects with
    this name and continuously keeps them up to date with the currently served
    resources and client configs as they change at runtime.
    All existing webhooks in these configuration objects are overwritten.

    This feature requires the ``patch`` and ``create`` RBAC verbs
    for ``admissionregistration.k8s.io``'s resources (:doc:`/admission`).
    """


@dataclasses.dataclass
class ExecutionSettings:
    """
    Settings for synchronous handlers execution (e.g. thread-/process-pools).
    """

    executor: concurrent.futures.Executor = dataclasses.field(
        default_factory=concurrent.futures.ThreadPoolExecutor)
    """
    The executor to be used for synchronous handler invocation.

    It can be changed at runtime (e.g. to reset the pool size). Already running
    handlers (specific invocations) will continue with their original executors.
    """

    _max_workers: Optional[int] = None

    @property
    def max_workers(self) -> Optional[int]:
        """
        How many threads/processes is dedicated to handler execution.

        It can be changed at runtime (the threads/processes are not terminated).
        """
        return self._max_workers

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        if value < 1:
            raise ValueError("Can't set thread pool limit lower than 1.")
        self._max_workers = value

        if hasattr(self.executor, '_max_workers'):
            self.executor._max_workers = value  # type: ignore
        else:
            raise TypeError("Current executor does not support `max_workers`.")


@dataclasses.dataclass
class PersistenceSettings:

    finalizer: str = 'kopf.zalando.org/KopfFinalizerMarker'
    """
    A string marker to be put on a list of finalizers to block the object
    from being deleted without framework's/operator's permission.
    """

    progress_storage: progress.ProgressStorage = dataclasses.field(
        default_factory=progress.SmartProgressStorage)
    """
    How to persist the handlers' state between multiple handling cycles.
    """

    diffbase_storage: diffbase.DiffBaseStorage = dataclasses.field(
        default_factory=diffbase.AnnotationsDiffBaseStorage)
    """
    How the resource's essence (non-technical, contentful fields) are stored.
    """


@dataclasses.dataclass
class BackgroundSettings:
    """
    Settings for background routines in general, daemons & timers specifically.
    """

    cancellation_polling: float = 60
    """
    How often (in seconds) to poll the status of an exiting daemon/timer
    when it has no cancellation timeout set (i.e. when it is assumed to
    exit gracefully by its own, but it does not).
    """

    instant_exit_timeout: Optional[float] = None
    """
    For how long (in seconds) to wait for a daemon/timer to exit instantly.

    If they continue running after the stopper is set for longer than this time,
    then external polling is initiated via the resource's persistence storages,
    as for regular handlers.

    The "instant exit" timeout is neither combined with any other timeouts, nor
    deducted from any other timeouts, such as the daemon cancellation timeout.
    
    So, keep the timeout low: 0.001, 0.01, or 0.1 are good enough; 1.0 is risky.
    Big delays can cause slower operator reaction to the resource deletion
    or operator exiting, but can reduce the amount of unnecessary patches.

    If the timeout is not set (the default), then a limited amount of zero-time
    asyncio event loop cycles is used instead.
    """

    instant_exit_zero_time_cycles: Optional[int] = 10
    """
    How many asyncio cycles to give to a daemon/timer to exit instantly. 

    There is a speed-up hack to let the daemons/timers to exit instantly,
    without external patching & polling. For this, ``asyncio.sleep(0)`` is used
    to give control back to the event loop and their coroutines. However,
    the daemons/timers can do extra `await` calls (even zero-time) before
    actually exiting, which prematurely returns the control flow back
    to the daemon-stopper coroutine.

    This configuration value is a maximum amount of zero-time `await` statements
    that can happen before exiting: both in the daemon and in the framework.

    It the daemons/timers coroutines exit earlier, extra cycles are not used.
    If they continue running after that, then external polling is initiated
    via the resource's persistence storages, as for regular handlers.

    All of this happens with zero delays, so no slowdown is expected
    (but a bit of CPU will be consumed).

    If an "instant exit" timeout is set, the zero-time cycles are not used.

    PS: The default value is a rough guess on a typical code complexity.
    """


@dataclasses.dataclass
class OperatorSettings:
    process: ProcessSettings = dataclasses.field(default_factory=ProcessSettings)
    posting: PostingSettings = dataclasses.field(default_factory=PostingSettings)
    peering: PeeringSettings = dataclasses.field(default_factory=PeeringSettings)
    watching: WatchingSettings = dataclasses.field(default_factory=WatchingSettings)
    batching: BatchingSettings = dataclasses.field(default_factory=BatchingSettings)
    scanning: ScanningSettings = dataclasses.field(default_factory=ScanningSettings)
    admission: AdmissionSettings =dataclasses.field(default_factory=AdmissionSettings)
    execution: ExecutionSettings = dataclasses.field(default_factory=ExecutionSettings)
    background: BackgroundSettings = dataclasses.field(default_factory=BackgroundSettings)
    persistence: PersistenceSettings = dataclasses.field(default_factory=PersistenceSettings)
