import pytest

from kopf._cogs.structs.credentials import LoginError
from kopf._core.intents.piggybacking import PRIORITY_OF_CLIENT, login_via_client


@pytest.fixture()
def _mock_client(kubernetes, mocker):
    """Mock kubernetes config loading to succeed (in-cluster) and return a known Configuration."""
    mocker.patch.object(kubernetes.config, 'load_incluster_config')
    mocker.patch.object(kubernetes.config, 'load_kube_config')

    config = kubernetes.client.Configuration()
    config.host = 'https://localhost:443'
    config.ssl_ca_cert = '/path/to/ca.crt'
    config.verify_ssl = True
    config.username = 'admin'
    config.password = 'secret'
    config.cert_file = '/path/to/cert.pem'
    config.key_file = '/path/to/key.pem'
    config.api_key = {'authorization': 'Bearer some-token'}
    config.api_key_prefix = {}

    mocker.patch.object(kubernetes.client.Configuration, 'get_default_copy', return_value=config)

    return config


@pytest.mark.usefixtures('no_kubernetes')
def test_returns_none_when_library_is_absent(settings, logger):
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is None


def test_in_cluster_config_succeeds(kubernetes, settings, logger, _mock_client):
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert kubernetes.config.load_incluster_config.call_count == 1
    assert not kubernetes.config.load_kube_config.called


def test_kubeconfig_fallback_when_incluster_fails(kubernetes, settings, logger, _mock_client):
    kubernetes.config.load_incluster_config.side_effect = kubernetes.config.ConfigException()
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert kubernetes.config.load_incluster_config.call_count == 1
    assert kubernetes.config.load_kube_config.call_count == 1


def test_login_error_when_both_configs_fail(kubernetes, settings, logger, _mock_client):
    kubernetes.config.load_incluster_config.side_effect = kubernetes.config.ConfigException()
    kubernetes.config.load_kube_config.side_effect = kubernetes.config.ConfigException()
    with pytest.raises(LoginError, match="Cannot authenticate the client library"):
        login_via_client(logger=logger, settings=settings)


def test_full_credential_extraction(settings, logger, _mock_client):
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.server == 'https://localhost:443'
    assert credentials.ca_path == '/path/to/ca.crt'
    assert credentials.insecure is False  # not verify_ssl (True)
    assert credentials.username == 'admin'
    assert credentials.password == 'secret'
    assert credentials.scheme == 'Bearer'
    assert credentials.token == 'some-token'
    assert credentials.certificate_path == '/path/to/cert.pem'
    assert credentials.private_key_path == '/path/to/key.pem'
    assert credentials.priority == PRIORITY_OF_CLIENT
    assert credentials.default_namespace is None
    assert credentials.proxy_url is None


@pytest.mark.parametrize('header, expected_scheme, expected_token', [
    (None, None, None),
    ('tkn', None, 'tkn'),
    ('Bearer tkn', 'Bearer', 'tkn'),
], ids=['no-header', 'bare-token', 'scheme-and-token'])
def test_token_parsing(settings, logger, _mock_client, header, expected_scheme, expected_token):
    _mock_client.api_key = {'authorization': header} if header else {}
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.scheme == expected_scheme
    assert credentials.token == expected_token


def test_empty_username_and_password_become_none(settings, logger, _mock_client):
    _mock_client.username = ''
    _mock_client.password = ''
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.username is None
    assert credentials.password is None


def test_trust_env_is_propagated_from_settings(settings, logger, _mock_client):
    settings.networking.trust_env = True
    credentials = login_via_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.trust_env is True
