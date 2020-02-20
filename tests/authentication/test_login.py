"""
Remember: We do not test the clients, we assume they work when used properly.
We test our own functions here, and check if the clients were called.
"""
import pytest

import pykube

from kopf import login


def test_client_login_works_incluster(login_mocks, kubernetes):
    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_client_login_works_viaconfig(login_mocks, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called


def test_pykube_login_works_incluster(login_mocks, pykube):
    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_login_works_viaconfig(login_mocks, pykube):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
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

    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert get_pykube_cfg.called
    assert not login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_is_independent_of_client_incluster(login_mocks, no_kubernetes, pykube):
    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert not login_mocks.pykube_from_file.called


def test_pykube_is_independent_of_client_viaconfig(login_mocks, no_kubernetes, pykube):
    login_mocks.pykube_in_cluster.side_effect = FileNotFoundError

    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.pykube_in_cluster.called
    assert login_mocks.pykube_from_file.called


def test_client_is_independent_of_pykube_incluster(login_mocks, no_pykube, kubernetes):
    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.client_in_cluster.called
    assert not login_mocks.client_from_file.called


def test_client_is_independent_of_pykube_viaconfig(login_mocks, no_pykube, kubernetes):
    login_mocks.client_in_cluster.side_effect = kubernetes.config.ConfigException

    with pytest.deprecated_call(match=r"cease using kopf.login\(\)"):
        login()

    assert login_mocks.client_in_cluster.called
    assert login_mocks.client_from_file.called
