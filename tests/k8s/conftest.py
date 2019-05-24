import pytest
from kubernetes.client.rest import ApiException  # to avoid mocking it


# We do not test the Kubernetes client, so everything there should be mocked.
# Also, no external calls must be made under any circumstances. Hence, auto-use.
@pytest.fixture(autouse=True)
def client_mock(mocker):
    client_mock = mocker.patch('kubernetes.client')
    client_mock.rest.ApiException = ApiException  # to be raises and caught
    return client_mock
