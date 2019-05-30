import pytest
from kubernetes.client import V1Event as V1Event
from kubernetes.client import V1EventSource as V1EventSource
from kubernetes.client import V1ObjectMeta as V1ObjectMeta
from kubernetes.client import V1beta1Event as V1beta1Event
from kubernetes.client.rest import ApiException  # to avoid mocking it


# We do not test the Kubernetes client, so everything there should be mocked.
# Also, no external calls must be made under any circumstances. Hence, auto-use.
@pytest.fixture(autouse=True)
def client_mock(mocker):
    client_mock = mocker.patch('kubernetes.client')
    client_mock.rest.ApiException = ApiException  # to be raises and caught
    client_mock.V1Event = V1Event
    client_mock.V1beta1Event = V1beta1Event
    client_mock.V1EventSource = V1EventSource
    client_mock.V1ObjectMeta = V1ObjectMeta
    return client_mock
