import pytest

from kopf._cogs.structs.credentials import LoginError
from kopf._core.intents.piggybacking import PRIORITY_OF_PYKUBE, login_via_pykube

KUBECONFIG = '''
    kind: Config
    current-context: self
    clusters:
      - name: self
        cluster:
          server: https://localhost:443
          certificate-authority: {ca_path}
          insecure-skip-tls-verify: true
          proxy-url: http://proxy:8080
    contexts:
      - name: self
        context:
          cluster: self
          user: self
          namespace: default
    users:
      - name: self
        user:
          username: admin
          password: secret
          token: some-token
          client-certificate: {cert_path}
          client-key: {key_path}
'''

MINICONFIG = '''
    kind: Config
    current-context: self
    clusters:
      - name: self
        cluster:
          server: https://localhost:443
    contexts:
      - name: self
        context:
          cluster: self
'''


@pytest.mark.usefixtures('no_pykube')
def test_returns_none_when_library_is_absent(settings, logger):
    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is None


@pytest.fixture()
def _mock_pykube(pykube, mocker, tmp_path):
    """Mock pykube config loading to succeed (in-cluster) and return a known KubeConfig."""
    ca_path = tmp_path / 'ca.crt'
    cert_path = tmp_path / 'cert.pem'
    key_path = tmp_path / 'key.pem'
    ca_path.write_text('fake')
    cert_path.write_text('fake')
    key_path.write_text('fake')
    config_path = tmp_path / 'config'
    config_path.write_text(KUBECONFIG.format(ca_path=ca_path, cert_path=cert_path, key_path=key_path))

    config = pykube.KubeConfig.from_file(str(config_path))
    mocker.patch.object(pykube.KubeConfig, 'from_service_account', return_value=config)
    mocker.patch.object(pykube.KubeConfig, 'from_file', return_value=config)
    return config


def test_in_cluster_config_succeeds(pykube, settings, logger, _mock_pykube):
    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is not None
    assert pykube.KubeConfig.from_service_account.call_count == 1
    assert not pykube.KubeConfig.from_file.called


def test_kubeconfig_fallback_when_incluster_fails(pykube, settings, logger, _mock_pykube):
    pykube.KubeConfig.from_service_account.side_effect = FileNotFoundError()
    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is not None
    assert pykube.KubeConfig.from_service_account.call_count == 1
    assert pykube.KubeConfig.from_file.call_count == 1


def test_login_error_when_both_configs_fail(pykube, settings, logger, _mock_pykube):
    pykube.KubeConfig.from_service_account.side_effect = FileNotFoundError()
    pykube.KubeConfig.from_file.side_effect = FileNotFoundError()
    with pytest.raises(LoginError, match="Cannot authenticate pykube"):
        login_via_pykube(logger=logger, settings=settings)


def test_full_credential_extraction(settings, logger, _mock_pykube, tmp_path):
    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.server == 'https://localhost:443'
    assert credentials.ca_path == str(tmp_path / 'ca.crt')
    assert credentials.insecure is True
    assert credentials.username == 'admin'
    assert credentials.password == 'secret'
    assert credentials.token == 'some-token'
    assert credentials.certificate_path == str(tmp_path / 'cert.pem')
    assert credentials.private_key_path == str(tmp_path / 'key.pem')
    assert credentials.default_namespace == 'default'
    assert credentials.proxy_url == 'http://proxy:8080'
    assert credentials.priority == PRIORITY_OF_PYKUBE


def test_minimal_config_has_none_for_optional_fields(pykube, mocker, settings, logger, tmp_path):
    config_path = tmp_path / 'config'
    config_path.write_text(MINICONFIG)
    config = pykube.KubeConfig.from_file(str(config_path))
    mocker.patch.object(pykube.KubeConfig, 'from_service_account', return_value=config)

    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.server == 'https://localhost:443'
    assert credentials.ca_path is None
    assert credentials.insecure is None
    assert credentials.username is None
    assert credentials.password is None
    assert credentials.token is None
    assert credentials.certificate_path is None
    assert credentials.private_key_path is None
    assert credentials.proxy_url is None


def test_trust_env_is_propagated_from_settings(settings, logger, _mock_pykube):
    settings.networking.trust_env = True
    credentials = login_via_pykube(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.trust_env is True
