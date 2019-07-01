
import asyncio
import concurrent.futures
import logging
from typing import Optional

import kubernetes
import kubernetes.client.rest

format = '[%(asctime)s] %(name)-20.20s [%(levelname)-8.8s] %(message)s'


LOGLEVEL_INFO = 20
""" Event loglevel to log all events. """
LOGLEVEL_WARNING = 30
""" Event loglevel to log all events except informational. """
LOGLEVEL_ERROR = 40
""" Event loglevel to log only errors and critical events. """
LOGLEVEL_CRITICAL = 50
""" Event loglevel to log only critical events(basically - no events). """


def configure(debug=None, verbose=None, quiet=None):
    log_level = 'DEBUG' if debug or verbose else 'WARNING' if quiet else 'INFO'

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    # Configure the Kubernetes client defaults according to our settings.
    config = kubernetes.client.configuration.Configuration()
    config.logger_format = format
    config.logger_file = None  # once again after the constructor to re-apply the formatter
    config.debug = debug
    kubernetes.client.configuration.Configuration.set_default(config)

    # Kubernetes client is as buggy as hell: it adds its own stream handlers even in non-debug mode,
    # does not respect the formatting, and dumps too much of the low-level info.
    if not debug:
        logger = logging.getLogger("urllib3")
        del logger.handlers[1:]  # everything except the default NullHandler

    # Prevent the low-level logging unless in the debug verbosity mode. Keep only the operator's messages.
    logging.getLogger('urllib3').propagate = debug
    logging.getLogger('asyncio').propagate = debug
    logging.getLogger('kubernetes').propagate = debug

    loop = asyncio.get_event_loop()
    loop.set_debug(debug)


class EventsConfig:
    """
    Used to configure events sending behaviour.
    """

    events_loglevel = LOGLEVEL_INFO
    """ What events should be logged. """


class WorkersConfig:
    """
    Used as single point of configuration for kopf.reactor.
    """

    threadpool_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

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
    def get_syn_executor() -> concurrent.futures.ThreadPoolExecutor:
        if not WorkersConfig.threadpool_executor:
            logging.debug('Setting up syn executor')
            WorkersConfig.threadpool_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=WorkersConfig.synchronous_tasks_threadpool_limit
            )
        return WorkersConfig.threadpool_executor

    @staticmethod
    def set_synchronous_tasks_threadpool_limit(new_limit: int):
        """
        Call this static method at any time to change synchronous_tasks_threadpool_limit in runtime.
        """
        if new_limit < 1:
            raise ValueError('Can`t set threadpool limit lower than 1')

        WorkersConfig.synchronous_tasks_threadpool_limit = new_limit
        if WorkersConfig.threadpool_executor:
            WorkersConfig.threadpool_executor._max_workers = new_limit
