import datetime

import pytest

from kopf._cogs.structs.credentials import ConnectionInfo, VaultKey


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
        expiration=datetime.datetime.max,
    )
    assert info.server == 'https://localhost'
    assert info.insecure is True
    assert info.username == 'username'
    assert info.password == 'password'
    assert info.scheme == 'scheme'
    assert info.token == 'token'
    assert info.default_namespace == 'default'
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
