"""
Remember: We do not test the clients, we assume they work when used properly.
We test our own functions here, and check if the clients were called.
"""
import pytest
import requests
import urllib3

from kopf.clients.auth import login, LoginError, AccessError

RESPONSE_401 = requests.Response()
RESPONSE_401.status_code = 401


@pytest.fixture(autouse=True)
def _auto_clean_kubernetes_client(clean_kubernetes_client):
    pass


def test_kubernetes_uninstalled_has_effect(no_kubernetes):
    with pytest.raises(ImportError):
        import kubernetes

#
# Tests via the direct function invocation.
#

def test_direct_auth_works_incluster_without_client(login_mocks, no_kubernetes):
    login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_direct_auth_works_viaconfig_without_client(login_mocks, no_kubernetes):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_direct_auth_works_incluster_with_client(login_mocks, kubernetes):
    login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_direct_auth_works_viaconfig_with_client(login_mocks, kubernetes):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_direct_auth_fails_on_errors_in_pykube(login_mocks, any_kubernetes):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.pykube_from_file.side_effect = FileNotFoundError

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_direct_auth_fails_on_errors_in_client(login_mocks, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException
    login_mocks.client_from_file.side_effect = kubernetes.config.ConfigException

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_direct_check_fails_on_tcp_error_in_pykube(login_mocks, any_kubernetes):
    login_mocks.pykube_checker.side_effect = requests.exceptions.ConnectionError()

    with pytest.raises(AccessError):
        login(verify=True)

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called


def test_direct_check_fails_on_401_error_in_pykube(login_mocks, any_kubernetes):
    login_mocks.pykube_checker.side_effect = requests.exceptions.HTTPError(response=RESPONSE_401)

    with pytest.raises(AccessError):
        login(verify=True)

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called


def test_direct_check_fails_on_tcp_error_in_client(login_mocks, kubernetes):
    login_mocks.client_checker.side_effect = urllib3.exceptions.HTTPError()

    with pytest.raises(AccessError):
        login(verify=True)

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.client_checker.called


def test_direct_check_fails_on_401_error_in_client(login_mocks, kubernetes):
    login_mocks.client_checker.side_effect = kubernetes.client.rest.ApiException(status=401)

    with pytest.raises(AccessError):
        login(verify=True)

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.client_checker.called

#
# The same tests, but via the CLI command run.
#

def test_clirun_auth_works_incluster_without_client(login_mocks, no_kubernetes,
                                                    invoke, preload, real_run):
    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_clirun_auth_works_viaconfig_without_client(login_mocks, no_kubernetes,
                                                    invoke, preload, real_run):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_clirun_auth_works_incluster_with_client(login_mocks, kubernetes,
                                                 invoke, preload, real_run):

    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.client_checker.called


def test_clirun_auth_works_viaconfig_with_client(login_mocks, kubernetes,
                                                 invoke, preload, real_run):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code == 0

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
    assert login_mocks.client_checker.called


def test_clirun_auth_fails_on_config_error_in_pykube(login_mocks, any_kubernetes,
                                                     invoke, preload, real_run):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.pykube_from_file.side_effect = FileNotFoundError

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'neither in-cluster, nor via kubeconfig' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called
    assert not login_mocks.pykube_checker.called


def test_clirun_auth_fails_on_config_error_in_client(login_mocks, kubernetes,
                                                     invoke, preload, real_run):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException
    login_mocks.client_from_file.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'neither in-cluster, nor via kubeconfig' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
    assert not login_mocks.client_checker.called


def test_clirun_check_fails_on_tcp_error_in_pykube(login_mocks, any_kubernetes,
                                                   invoke, preload, real_run):
    login_mocks.pykube_checker.side_effect = requests.exceptions.ConnectionError()

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please configure the cluster access' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called


def test_clirun_check_fails_on_401_error_in_pykube(login_mocks, any_kubernetes,
                                                   invoke, preload, real_run):
    login_mocks.pykube_checker.side_effect = requests.exceptions.HTTPError(response=RESPONSE_401)

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please login or configure the tokens' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called


def test_clirun_check_fails_on_tcp_error_in_client(login_mocks, kubernetes,
                                                   invoke, preload, real_run):
    login_mocks.client_checker.side_effect = urllib3.exceptions.HTTPError()

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please configure the cluster access' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.client_checker.called


def test_clirun_check_fails_on_401_error_in_client(login_mocks, kubernetes,
                                                   invoke, preload, real_run):
    login_mocks.client_checker.side_effect = kubernetes.client.rest.ApiException(status=401)

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please login or configure the tokens' in result.stdout

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called
    assert login_mocks.pykube_checker.called

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called
    assert login_mocks.client_checker.called
