import kubernetes
import pytest

from kopf.clients.auth import login, LoginError


@pytest.fixture(autouse=True)
def _auto_clean_kubernetes_client(clean_kubernetes_client):
    pass


def test_direct_auth_works_incluster(mocker):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')

    login()

    assert load_incluster_config.called
    assert not load_kube_config.called
    assert core_api.called  # to verify that auth worked


def test_direct_auth_works_kubeconfig(mocker):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    load_incluster_config.side_effect = kubernetes.config.ConfigException

    login()

    assert load_incluster_config.called
    assert load_kube_config.called
    assert core_api.called  # to verify that auth worked


def test_direct_auth_fails(mocker):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    load_incluster_config.side_effect = kubernetes.config.ConfigException
    load_kube_config.side_effect = kubernetes.config.ConfigException

    with pytest.raises(LoginError):
        login()

    assert load_incluster_config.called
    assert load_kube_config.called
    assert not core_api.called  # to verify that auth worked


def test_direct_api_fails(mocker):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    core_api.side_effect = kubernetes.client.rest.ApiException(status=401)

    with pytest.raises(LoginError):
        login()

    assert load_incluster_config.called
    assert not load_kube_config.called
    assert core_api.called  # to verify that auth worked


def test_clirun_auth_works_incluster(invoke, mocker, preload, real_run):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')

    result = invoke(['run'])
    assert result.exit_code == 0

    assert load_incluster_config.called
    assert not load_kube_config.called
    assert core_api.called  # to verify that auth worked


def test_clirun_auth_works_kubeconfig(invoke, mocker, preload, real_run):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    load_incluster_config.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code == 0

    assert load_incluster_config.called
    assert load_kube_config.called
    assert core_api.called  # to verify that auth worked


def test_clirun_auth_fails(invoke, mocker, preload, real_run):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    load_incluster_config.side_effect = kubernetes.config.ConfigException
    load_kube_config.side_effect = kubernetes.config.ConfigException

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'neither in-cluster, nor via kubeconfig' in result.stdout

    assert load_incluster_config.called
    assert load_kube_config.called
    assert not core_api.called  # to verify that auth worked


def test_clirun_api_fails(invoke, mocker, preload, real_run):
    # We do not test the client, we assume it works when used properly.
    core_api = mocker.patch.object(kubernetes.client, 'CoreApi')
    load_kube_config = mocker.patch.object(kubernetes.config, 'load_kube_config')
    load_incluster_config = mocker.patch.object(kubernetes.config, 'load_incluster_config')
    core_api.side_effect = kubernetes.client.rest.ApiException(status=401)

    result = invoke(['run'])
    assert result.exit_code != 0
    assert 'Please login or configure the tokens' in result.stdout

    assert load_incluster_config.called
    assert not load_kube_config.called
    assert core_api.called  # to verify that auth worked
