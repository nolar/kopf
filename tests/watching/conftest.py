import kubernetes
import pytest


@pytest.fixture(autouse=True)
def always_mocked_kubernetes_client(mocker):
    """
    We do not test the Kubernetes client, so everything there should be mocked.
    Also, no external calls must be made under any circumstances.
    """
    mocker.patch('kubernetes.watch')
    mocker.patch('kubernetes.client')


@pytest.fixture()
def stream_fn(always_mocked_kubernetes_client):
    return kubernetes.watch.Watch.return_value.stream


@pytest.fixture()
def apicls_fn(always_mocked_kubernetes_client):
    return kubernetes.client.CustomObjectsApi

