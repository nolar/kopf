
import asyncio
import logging

import click
import kubernetes
import kubernetes.client.rest
import urllib3.exceptions

logger = logging.getLogger(__name__)
format = '[%(asctime)s] %(name)-20.20s [%(levelname)-8.8s] %(message)s'


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
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()  # developer's config files
        logger.debug("configured via kubeconfig file")

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
    if not debug:
        logging.getLogger('asyncio').propagate = False
        logging.getLogger('kubernetes').propagate = False

    loop = asyncio.get_event_loop()
    loop.set_debug(debug)
