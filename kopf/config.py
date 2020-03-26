import logging
from typing import Optional

# Deprecated: use ``logging.*`` constants instead. Kept here for backward-compatibility.
LOGLEVEL_INFO = logging.INFO
LOGLEVEL_WARNING = logging.WARNING
LOGLEVEL_ERROR = logging.ERROR
LOGLEVEL_CRITICAL = logging.CRITICAL


# DEPRECATED: Used for initial defaults for per-operator settings (see kopf.structs.configuration).
class EventsConfig:
    """
    Used to configure events sending behaviour.
    """

    events_loglevel: int = logging.INFO
    """ What events should be logged. """


# DEPRECATED: Used for initial defaults for per-operator settings (see kopf.structs.configuration).
class WorkersConfig:
    """
    Used as single point of configuration for kopf.reactor.
    """

    queue_workers_limit: Optional[int] = None  # if None, there is no limits to workers number
    """ How many workers can be running simultaneously on per-object event queue. """

    synchronous_tasks_threadpool_limit: Optional[int] = None  # if None, calculated by ThreadPoolExecutor based on cpu count
    """ How many threads in total can be running simultaneously to handle any non-async tasks."""

    worker_idle_timeout: float = 5.0
    """ How long does a worker can idle before exiting and garbage-collecting."""

    worker_batch_window: float = 0.1
    """ How fast/slow does a worker deplete the queue when an event is received."""

    worker_exit_timeout: float = 2.0
    """ How long does a worker can work on watcher exit before being cancelled. """

    @staticmethod
    def set_synchronous_tasks_threadpool_limit(new_limit: int) -> None:
        """
        Call this static method at any time to change synchronous_tasks_threadpool_limit in runtime.
        """
        if new_limit < 1:
            raise ValueError('Can`t set threadpool limit lower than 1')

        WorkersConfig.synchronous_tasks_threadpool_limit = new_limit

        # Also apply to the current runtime settings, if we are at runtime (not load-time).
        try:
            # Wherever we can find it; ignore "nice" architecture (this class is deprecated anyway).
            from kopf.engines import posting  # noqa
            from kopf.structs import configuration  # noqa  # cyclic imports, for type annotations
            settings: configuration.OperatorSettings = posting.settings_var.get()
        except LookupError:
            pass
        else:
            settings.execution.max_workers = new_limit


# DEPRECATED: Used for initial defaults for per-operator settings (see kopf.structs.configuration).
class WatchersConfig:
    """
    Used to configure the K8s API watchers and streams.
    """

    default_stream_timeout: Optional[float] = None
    """ The maximum duration of one streaming request. Patched in some tests. """

    watcher_retry_delay: float = 0.1
    """ How long should a pause be between watch requests (to prevent flooding). """

    session_timeout: Optional[float] = None
    """ The http session timeout to use in watch request. """
