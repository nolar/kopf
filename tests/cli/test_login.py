"""
Remember: We do not test the clients, we assume they work when used properly.
We test our own functions here, and check if the clients were called.
"""
import kubernetes.client.rest
import pytest
import requests

from kopf.clients.auth import login, verify, LoginError, AccessError


@pytest.fixture(autouse=True)
def _auto_clean_kubernetes_client(clean_kubernetes_client):
    pass


def test_direct_auth_works_incluster(login_mocks):

    login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_direct_auth_works_kubeconfig(login_mocks):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_direct_auth_fails_on_errors_in_pykube(login_mocks):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.pykube_from_file.side_effect = FileNotFoundError

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert not login_mocks.client_in_cluster.called     # because pykube failed
    assert not login_mocks.client_from_file.called      # because pykube failed


def test_direct_auth_fails_on_errors_in_client(login_mocks):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException
    login_mocks.client_from_file.side_effect = kubernetes.config.ConfigException

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_direct_check_fails_on_errors_in_pykube(login_mocks):
    response = requests.Response()
    response.status_code = 401
    login_mocks.pykube_checker.side_effect = requests.exceptions.HTTPError(response=response)

    login()
    with pytest.raises(AccessError):
        verify()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert not login_mocks.client_checker.called  # because pykube failed


def test_direct_check_fails_on_errors_in_client(login_mocks):
    login_mocks.client_checker.side_effect = kubernetes.client.rest.ApiException(status=401)

    login()
    with pytest.raises(AccessError):
        verify()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert login_mocks.client_checker.called


def test_clirun_auth_works_incluster(invoke, login_mocks, preload, real_run):

    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert login_mocks.client_checker.called


def test_clirun_auth_works_kubeconfig(invoke, login_mocks, preload, real_run):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert login_mocks.client_checker.called


def test_clirun_auth_fails_on_errors_in_pykube(invoke, login_mocks, preload, real_run):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.pykube_from_file.side_effect = FileNotFoundError

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'neither in-cluster, nor via kubeconfig' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert not login_mocks.client_in_cluster.called   # because pykube failed
    assert not login_mocks.client_from_file.called    # because pykube failed
    assert not login_mocks.pykube_checker.called
    assert not login_mocks.client_checker.called


def test_clirun_auth_fails_on_errors_in_client(invoke, login_mocks, preload, real_run):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException
    login_mocks.client_from_file.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'neither in-cluster, nor via kubeconfig' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
    assert not login_mocks.pykube_checker.called
    assert not login_mocks.client_checker.called


def test_clirun_check_fails_on_errors_in_pykube(invoke, login_mocks, preload, real_run):
    response = requests.Response()
    response.status_code = 401
    login_mocks.pykube_checker.side_effect = requests.exceptions.HTTPError(response=response)

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please login or configure the tokens' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert not login_mocks.client_checker.called  # because pykube failed


def test_clirun_check_fails_on_errors_in_client(invoke, login_mocks, preload, real_run):
    login_mocks.client_checker.side_effect = kubernetes.client.rest.ApiException(status=401)

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please login or configure the tokens' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.pykube_checker.called
    assert login_mocks.client_checker.called
