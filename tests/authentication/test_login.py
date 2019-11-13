"""
Remember: We do not test the clients, we assume they work when used properly.
We test our own functions here, and check if the clients were called.
"""
import pytest

import pykube
from kopf import login, LoginError


def test_client_login_works_incluster(login_mocks, kubernetes):
    login()

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_client_login_works_viaconfig(login_mocks, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    login()

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_pykube_login_works_incluster(login_mocks, pykube):
    login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_login_works_viaconfig(login_mocks, pykube):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_monkeypatched_get_pykube_cfg_overrides_pykube(mocker, login_mocks):
    get_pykube_cfg = mocker.patch('kopf.clients.auth.get_pykube_cfg')
    get_pykube_cfg.return_value = pykube.KubeConfig({
        'current-context': 'self',
        'contexts': [{'name': 'self', 'context': {'cluster': 'self'}}],
        'clusters': [{'name': 'self', 'cluster': {'server': 'https://localhost'}}],
    })

    login()

    assert get_pykube_cfg.called
    assert not login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_is_independent_of_client_incluster(login_mocks, no_kubernetes, pykube):
    login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_is_independent_of_client_viaconfig(login_mocks, no_kubernetes, pykube):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_client_is_independent_of_pykube_incluster(login_mocks, no_pykube, kubernetes):
    login()

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_client_is_independent_of_pykube_viaconfig(login_mocks, no_pykube, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    login()

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


# TODO: do we actually fail on every method failure? Or should we use another one?
def test_login_fails_on_errors_in_pykube(login_mocks, any_kubernetes):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError
    login_mocks.pykube_from_file.side_effect = FileNotFoundError

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_login_fails_on_errors_in_client(login_mocks, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException
    login_mocks.client_from_file.side_effect = kubernetes.config.ConfigException

    with pytest.raises(LoginError):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
