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
from typing import Optional

from kopf import config  # for legacy defaults only
from kopf.storage import diffbase
from kopf.storage import progress


@dataclasses.dataclass
class LoggingSettings:
    pass


@dataclasses.dataclass
class PostingSettings:

    enabled: bool = True
    """
    Should the log messages be sent as Kubernetes Events for an object.
    The events can be seen in ``kubectl describe`` output for the object.

    This also affects ``kopf.event()`` and similar functions
    (``kopf.info()``, ``kopf.warn()``, ``kopf.exception()``).
    """

    level: int = dataclasses.field(
        default_factory=lambda: config.EventsConfig.events_loglevel)
    """
    A minimal level of logging events that will be posted as K8s Events.
    The default is ``logging.INFO`` (i.e. all info, warning, errors are posted).

    This also affects ``kopf.event()`` and similar functions
    (``kopf.info()``, ``kopf.warn()``, ``kopf.exception()``).
    """


@dataclasses.dataclass
class WatchingSettings:

    server_timeout: Optional[float] = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.default_stream_timeout)
    """
    The maximum duration of one streaming request. Patched in some tests.
    If ``None``, then obey the server-side timeouts (they seem to be random).
    """

    client_timeout: Optional[float] = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.session_timeout)
    """
    An HTTP/HTTPS session timeout to use in watch requests.
    """

    connect_timeout: Optional[float] = None
    """
    An HTTP/HTTPS connection timeout to use in watch requests.
    """
    
    reconnect_backoff: float = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.watcher_retry_delay)
    """
    How long should a pause be between watch requests (to prevent API flooding).
    """


@dataclasses.dataclass
class BatchingSettings:
    """
    Settings for how raw events are batched and processed.
    """

    worker_limit: Optional[int] = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.queue_workers_limit)
    """
    How many workers can be running simultaneously on per-object event queue.
    If ``None``, there is no limit to the number of workers (as many as needed).
    """

    idle_timeout: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_idle_timeout)
    """
    How soon an idle worker is exited and garbage-collected if no events arrive.
    """

    batch_window: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_batch_window)
    """
    How fast/slow does a worker deplete the queue when an event is received.
    All events arriving within this window will be ignored except the last one.
    """

    exit_timeout: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_exit_timeout)
    """
    How soon a worker is cancelled when the parent watcher is going to exit.
    This is the time given to the worker to deplete and process the queue.
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

    _max_workers: Optional[int] = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.synchronous_tasks_threadpool_limit)

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
class OperatorSettings:
    logging: LoggingSettings = dataclasses.field(default_factory=LoggingSettings)
    posting: PostingSettings = dataclasses.field(default_factory=PostingSettings)
    watching: WatchingSettings = dataclasses.field(default_factory=WatchingSettings)
    batching: BatchingSettings = dataclasses.field(default_factory=BatchingSettings)
    execution: ExecutionSettings = dataclasses.field(default_factory=ExecutionSettings)
    persistence: PersistenceSettings = dataclasses.field(default_factory=PersistenceSettings)
