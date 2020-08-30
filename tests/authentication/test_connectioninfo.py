from kopf.structs.credentials import ConnectionInfo, VaultKey


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


def test_creation_with_maximal_fields():
    info = ConnectionInfo(
        server='https://localhost',
        ca_path='/ca/path',
        ca_data=b'ca_data',
        insecure=True,
        username='username',
        password='password',
        scheme='scheme',
        token='token',
        certificate_path='/cert/path',
        certificate_data=b'cert_data',
        private_key_path='/pkey/path',
        private_key_data=b'pkey_data',
        default_namespace='default',
    )
    assert info.server == 'https://localhost'
    assert info.ca_path == '/ca/path'
    assert info.ca_data == b'ca_data'
    assert info.insecure is True
    assert info.username == 'username'
    assert info.password == 'password'
    assert info.scheme == 'scheme'
    assert info.token == 'token'
    assert info.certificate_path == '/cert/path'
    assert info.certificate_data == b'cert_data'
    assert info.private_key_path == '/pkey/path'
    assert info.private_key_data == b'pkey_data'
    assert info.default_namespace == 'default'
