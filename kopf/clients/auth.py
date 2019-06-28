import logging

import kubernetes
import urllib3.exceptions

logger = logging.getLogger(__name__)


class LoginError(Exception):
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
