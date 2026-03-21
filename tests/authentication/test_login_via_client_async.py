from unittest.mock import AsyncMock

import pytest

from kopf._cogs.structs.credentials import LoginError
from kopf._core.intents.piggybacking import PRIORITY_OF_ASYNC_CLIENT, login_via_async_client


@pytest.fixture()
def _mock_async_client(kubernetes_asyncio, mocker):
    """Mock kubernetes_asyncio config loading to succeed (in-cluster) and return a known Configuration."""
    mocker.patch.object(kubernetes_asyncio.config, 'load_incluster_config')
    mocker.patch.object(kubernetes_asyncio.config, 'load_kube_config', new_callable=AsyncMock)

    config = kubernetes_asyncio.client.Configuration()
    config.host = 'https://localhost:443'
    config.ssl_ca_cert = '/path/to/ca.crt'
    config.verify_ssl = True
    config.username = 'admin'
    config.password = 'secret'
    config.cert_file = '/path/to/cert.pem'
    config.key_file = '/path/to/key.pem'
    config.proxy = 'http://proxy:8080'
    config.api_key = {'BearerToken': 'Bearer some-token'}
    config.api_key_prefix = {}

    mocker.patch.object(kubernetes_asyncio.client.Configuration, 'get_default_copy', return_value=config)

    return config


@pytest.mark.usefixtures('no_kubernetes_asyncio')
async def test_returns_none_when_library_is_absent(settings, logger):
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is None


async def test_in_cluster_config_succeeds(kubernetes_asyncio, settings, logger, _mock_async_client):
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is not None
    assert kubernetes_asyncio.config.load_incluster_config.call_count == 1
    assert not kubernetes_asyncio.config.load_kube_config.called


async def test_kubeconfig_fallback_when_incluster_fails(kubernetes_asyncio, settings, logger, _mock_async_client):
    kubernetes_asyncio.config.load_incluster_config.side_effect = kubernetes_asyncio.config.ConfigException()
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is not None
    assert kubernetes_asyncio.config.load_incluster_config.call_count == 1
    assert kubernetes_asyncio.config.load_kube_config.call_count == 1


async def test_login_error_when_both_configs_fail(kubernetes_asyncio, settings, logger, _mock_async_client):
    kubernetes_asyncio.config.load_incluster_config.side_effect = kubernetes_asyncio.config.ConfigException()
    kubernetes_asyncio.config.load_kube_config.side_effect = kubernetes_asyncio.config.ConfigException()
    with pytest.raises(LoginError, match="Cannot authenticate the async client library"):
        await login_via_async_client(logger=logger, settings=settings)


async def test_full_credential_extraction(settings, logger, _mock_async_client):
    credentials = await login_via_async_client(logger=logger, settings=settings)
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
    assert credentials.proxy_url == 'http://proxy:8080'
    assert credentials.priority == PRIORITY_OF_ASYNC_CLIENT
    assert credentials.default_namespace is None


@pytest.mark.parametrize('header, expected_scheme, expected_token', [
    (None, None, None),
    ('tkn', None, 'tkn'),
    ('Bearer tkn', 'Bearer', 'tkn'),
], ids=['no-header', 'bare-token', 'scheme-and-token'])
async def test_token_parsing(settings, logger, _mock_async_client, header, expected_scheme, expected_token):
    _mock_async_client.api_key = {'BearerToken': header} if header else {}
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.scheme == expected_scheme
    assert credentials.token == expected_token


async def test_empty_username_and_password_become_none(settings, logger, _mock_async_client):
    _mock_async_client.username = ''
    _mock_async_client.password = ''
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.username is None
    assert credentials.password is None


async def test_trust_env_is_propagated_from_settings(settings, logger, _mock_async_client):
    settings.networking.trust_env = True
    credentials = await login_via_async_client(logger=logger, settings=settings)
    assert credentials is not None
    assert credentials.trust_env is True
