
import asyncio
import logging

import click
import kubernetes
import kubernetes.client.rest
import urllib3.exceptions

logger = logging.getLogger(__name__)
format = '[%(asctime)s] %(name)-20.20s [%(levelname)-8.8s] %(message)s'


LOGLEVEL_INFO = 20
""" Event loglevel to log all events. """
LOGLEVEL_WARNING = 30
""" Event loglevel to log all events except informational. """
LOGLEVEL_ERROR = 40
""" Event loglevel to log only errors and critical events. """
LOGLEVEL_CRITICAL = 50
""" Event loglevel to log only critical events(basically - no events). """


class LoginError(click.ClickException):
    """ Raised when the operator cannot login to the API. """


def login():
    """
    Login the the Kubernetes cluster, locally or remotely.
    """

    # Configure the default client credentials for all possible environments.
    try:
        kubernetes.config.load_incluster_config()  # cluster env vars
        logger.debug("configured in cluster with service account")
    except kubernetes.config.ConfigException as e1:
        try:
            kubernetes.config.load_kube_config()  # developer's config files
            logger.debug("configured via kubeconfig file")
        except kubernetes.config.ConfigException as e2:
            raise LoginError(f"Cannot authenticate neither in-cluster, nor via kubeconfig.")

    # Make a sample API call to ensure the login is successful,
    # and convert some of the known exceptions to the CLI hints.
    try:
        api = kubernetes.client.CoreApi()
        api.get_api_versions()
    except urllib3.exceptions.HTTPError as e:
        raise LoginError("Cannot connect to the Kubernetes API. "
                         "Please configure the cluster access.")
    except kubernetes.client.rest.ApiException as e:
        if e.status == 401:
            raise LoginError("Cannot authenticate to the Kubernetes API. "
                             "Please login or configure the tokens.")
        else:
            raise


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

    synchronous_event_post_threadpool_limit = None
    """ How many workers can be running simultaneously on event creation operations. """

    synchronous_patch_threadpool_limit = None
    """ How many workers can be running simultaneously on patch operations. """

    queue_workers_limit = None  # if None, there is no limits to workers number
    """ How many workers can be running simultaneously on per-object event queue. """

    synchronous_handlers_threadpool_limit = None  # if None, calculated by ThreadPoolExecutor based on cpu count
    """ How many threads in total can be running simultaneously to handle non-async handler functions. """

    worker_idle_timeout = 5.0
    """ How long does a worker can idle before exiting and garbage-collecting."""

    worker_batch_window = 0.1
    """ How fast/slow does a worker deplete the queue when an event is received."""

    worker_exit_timeout = 2.0
    """ How long does a worker can work on watcher exit before being cancelled. """
