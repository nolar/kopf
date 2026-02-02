import datetime
import pathlib
import ssl

import aiohttp
import pytest

from kopf._cogs.structs.credentials import AiohttpSession, ConnectionInfo, VaultKey


def test_key_as_string():
    key = VaultKey('some-key')
    assert isinstance(key, str)
    assert key == 'some-key'


def test_creation_with_minimal_fields():
    info = ConnectionInfo(
        server='https://localhost',
    )
    assert info.server == 'https://localhost'
    assert info.ca_path is None
    assert info.ca_data is None
    assert info.insecure is None
    assert info.username is None
    assert info.password is None
    assert info.scheme is None
    assert info.token is None
    assert info.certificate_path is None
    assert info.certificate_data is None
    assert info.private_key_path is None
    assert info.private_key_data is None
    assert info.default_namespace is None
    assert info.proxy_url is None
    assert info.priority == 0
    assert info.expiration is None


def test_creation_with_regular_fields():
    info = ConnectionInfo(
        server='https://localhost',
        insecure=True,
        username='username',
        password='password',
        scheme='scheme',
        token='token',
        default_namespace='default',
        proxy_url='https://username:password@localhost',
        priority=123,
        expiration=datetime.datetime.max,
    )
    assert info.server == 'https://localhost'
    assert info.insecure is True
    assert info.username == 'username'
    assert info.password == 'password'
    assert info.scheme == 'scheme'
    assert info.token == 'token'
    assert info.default_namespace == 'default'
    assert info.priority == 123
    assert info.proxy_url == 'https://username:password@localhost'
    assert info.expiration == datetime.datetime.max


def test_creation_with_ssl_data_without_files():
    info = ConnectionInfo(
        server='https://localhost',
        ca_data=b'ca_data',
        certificate_data=b'cert_data',
        private_key_data=b'pkey_data',
    )
    assert info.server == 'https://localhost'
    assert info.ca_path is None
    assert info.ca_data == b'ca_data'
    assert info.certificate_path is None
    assert info.certificate_data == b'cert_data'
    assert info.private_key_path is None
    assert info.private_key_data == b'pkey_data'


def test_creation_with_ssl_path_without_data():
    info = ConnectionInfo(
        server='https://localhost',
        ca_path='/ca/path',
        certificate_path='/cert/path',
        private_key_path='/pkey/path',
    )
    assert info.server == 'https://localhost'
    assert info.ca_path == '/ca/path'
    assert info.ca_data is None
    assert info.certificate_path == '/cert/path'
    assert info.certificate_data is None
    assert info.private_key_path == '/pkey/path'
    assert info.private_key_data is None


def test_conflicting_ca_data_and_path():
    with pytest.raises(ValueError, match="Both CA path & data"):
        ConnectionInfo(server='', ca_path='/path', ca_data=b'data')


def test_conflicting_certificate_data_and_path():
    with pytest.raises(ValueError, match="Both certificate path & data"):
        ConnectionInfo(server='', certificate_path='/path', certificate_data=b'data')


def test_conflicting_private_key_data_and_path():
    with pytest.raises(ValueError, match="Both private key path & data"):
        ConnectionInfo(server='', private_key_path='/path', private_key_data=b'data')


def test_connection_info_as_aiohttp_basic_auth():
    info = ConnectionInfo(
        server='https://localhost',
        username='username',
        password='password',
    )
    assert info.as_aiohttp_basic_auth() == aiohttp.BasicAuth('username', 'password')
    assert info.as_http_headers() == {}


def test_connection_info_as_http_headers():
    info = ConnectionInfo(
        server='https://localhost',
        scheme='Bearer',
        token='xyz'
    )
    assert info.as_aiohttp_basic_auth() is None
    assert info.as_http_headers() == {'Authorization': 'Bearer xyz'}


def test_connection_info_as_ssl_context_when_insecure():
    info = ConnectionInfo(
        server='https://localhost',
        insecure=True,
    )
    ssl_context = info.as_ssl_context()
    ca = ssl_context.get_ca_certs()
    assert ssl_context.verify_mode == ssl.CERT_NONE
    assert ssl_context.check_hostname is False
    assert ca  # at least some default CAs must be loaded, but we do not know which ones.


def test_connection_info_as_ssl_context_when_defined():
    info = ConnectionInfo(
        server='https://localhost',
        ca_path=pathlib.Path(__file__).parent / 'fixtures/ca.pem',
        certificate_path=pathlib.Path(__file__).parent / 'fixtures/cert.pem',
        private_key_path=pathlib.Path(__file__).parent / 'fixtures/pkey.pem',
    )
    ssl_context = info.as_ssl_context()
    ca = ssl_context.get_ca_certs()
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
    assert ssl_context.check_hostname is True
    assert ca[0]['issuer'] == ((('commonName', 'minikubeCA'),),)
    assert ca[0]['subject'] == ((('commonName', 'minikubeCA'),),)
    assert ca[0]['notAfter'] == 'May 19 09:18:36 2029 GMT'
    assert ca[0]['notBefore'] == 'May 21 09:18:36 2019 GMT'


async def test_creation_of_aiohttp_session():
    aiohttp_session = aiohttp.ClientSession()
    info = AiohttpSession(
        server='https://localhost',
        default_namespace='default',
        priority=123,
        expiration=datetime.datetime.max,
        aiohttp_session=aiohttp_session,
    )
    assert info.server == 'https://localhost'
    assert info.default_namespace == 'default'
    assert info.priority == 123
    assert info.expiration == datetime.datetime.max
    assert info.aiohttp_session is aiohttp_session
    await aiohttp_session.close()
