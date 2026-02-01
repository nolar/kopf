import base64
import os.path
import pathlib
import ssl

import aiohttp
import pytest

from kopf._cogs.clients.auth import APIContext, authenticated, vault_var
from kopf._cogs.structs.credentials import AiohttpSession, ConnectionInfo, Vault

# These are Minikube's locally generated certificates (CN=minikubeCA).
# They are not in any public use, and are regenerated regularly.
with open(os.path.join(os.path.dirname(__file__), 'fixtures/ca.pem')) as f:
    SAMPLE_MINIKUBE_CA = f.read()

with open(os.path.join(os.path.dirname(__file__), 'fixtures/cert.pem')) as f:
    SAMPLE_MINIKUBE_CERT = f.read()

with open(os.path.join(os.path.dirname(__file__), 'fixtures/pkey.pem')) as f:
    SAMPLE_MINIKUBE_PKEY = f.read()


@authenticated
async def fn(context: APIContext):
    return context.session


@pytest.fixture(autouse=True)
def vault():
    vault = Vault()
    vault_var.set(vault)
    return vault


async def test_basic_auth(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            username='username',
            password='password',
        ),
    })
    session = await fn()

    async with session:
        assert session._default_auth.login == 'username'
        assert session._default_auth.password == 'password'
        assert 'Authorization' not in session._default_headers


async def test_header_with_token_only(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            token='token',
        ),
    })
    session = await fn()

    async with session:
        assert session._default_auth is None
        assert session._default_headers['Authorization'] == 'Bearer token'


async def test_header_with_schema_only(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            scheme='Digest xyz',
        ),
    })
    session = await fn()

    async with session:
        assert session._default_auth is None
        assert session._default_headers['Authorization'] == 'Digest xyz'


async def test_header_with_schema_and_token(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            scheme='Digest',
            token='xyz',
        ),
    })
    session = await fn()

    async with session:
        assert session._default_auth is None
        assert session._default_headers['Authorization'] == 'Digest xyz'


async def test_ca_insecure(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            insecure=True,
        ),
    })
    session = await fn()

    async with session:
        ctx = session.connector._ssl
        assert ctx.verify_mode == ssl.CERT_NONE


# TODO: find a way to test that the client certificates/pkeys are indeed loaded.
# TODO: currently, we only test that the parsing/loading does not fail at all.
@pytest.mark.parametrize('pkey_path', [
    pytest.param(pathlib.Path(__file__).parent / 'fixtures' / 'pkey.pem', id='pkey-path'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/pkey.pem'), id='pkey-str'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/pkey.pem').encode(), id='pkey-bytes'),
])
@pytest.mark.parametrize('cert_path', [
    pytest.param(pathlib.Path(__file__).parent / 'fixtures' / 'cert.pem', id='cert-path'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/cert.pem'), id='cert-str'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/cert.pem').encode(), id='cert-bytes'),
])
@pytest.mark.parametrize('ca_path', [
    pytest.param(pathlib.Path(__file__).parent / 'fixtures' / 'ca.pem', id='ca-path'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/ca.pem'), id='ca-str'),
    pytest.param(os.path.join(os.path.dirname(__file__), 'fixtures/ca.pem').encode(), id='ca-bytes'),
])
async def test_ssl_context_from_path(vault, ca_path, cert_path, pkey_path):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_path=ca_path,
            certificate_path=cert_path,
            private_key_path=pkey_path,
        ),
    })
    session = await fn()
    async with session:
        # We can only get the CA, not the individual certs/pkeys, so we trust it is parsed properly.
        # The functionality of the real cert/pkeys is tested in the e2e tests with the real cluster.
        ca = session.connector._ssl.get_ca_certs()
        assert ca[0]['issuer'] == ((('commonName', 'minikubeCA'),),)
        assert ca[0]['subject'] == ((('commonName', 'minikubeCA'),),)
        assert ca[0]['notAfter'] == 'May 19 09:18:36 2029 GMT'
        assert ca[0]['notBefore'] == 'May 21 09:18:36 2019 GMT'


@pytest.mark.parametrize('pkey_data', [
    pytest.param(SAMPLE_MINIKUBE_PKEY, id='pkey-raw-str'),
    pytest.param(SAMPLE_MINIKUBE_PKEY.encode(), id='pkey-raw-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_PKEY.encode()), id='pkey-b64-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_PKEY.encode()).decode(), id='pkey-b64-str'),
])
@pytest.mark.parametrize('cert_data', [
    pytest.param(SAMPLE_MINIKUBE_CERT, id='cert-raw-str'),
    pytest.param(SAMPLE_MINIKUBE_CERT.encode(), id='cert-raw-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_CERT.encode()), id='cert-b64-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_CERT.encode()).decode(), id='cert-b64-str'),
])
@pytest.mark.parametrize('ca_data', [
    pytest.param(SAMPLE_MINIKUBE_CA, id='ca-raw-str'),
    pytest.param(SAMPLE_MINIKUBE_CA.encode(), id='ca-raw-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_CA.encode()), id='ca-b64-bytes'),
    pytest.param(base64.encodebytes(SAMPLE_MINIKUBE_CA.encode()).decode(), id='ca-b64-str'),
])
async def test_ssl_context_from_data(vault, ca_data, cert_data, pkey_data):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_data=ca_data,
            certificate_data=cert_data,
            private_key_data=pkey_data,
        ),
    })
    session = await fn()
    async with session:
        # We can only get the CA, not the individual certs/pkeys, so we trust it is parsed properly.
        # The functionality of the real cert/pkeys is tested in the e2e tests with the real cluster.
        ca = session.connector._ssl.get_ca_certs()
        assert ca[0]['issuer'] == ((('commonName', 'minikubeCA'),),)
        assert ca[0]['subject'] == ((('commonName', 'minikubeCA'),),)
        assert ca[0]['notAfter'] == 'May 19 09:18:36 2029 GMT'
        assert ca[0]['notBefore'] == 'May 21 09:18:36 2019 GMT'


async def test_custom_aiohttp_session(vault):
    sample_session = aiohttp.ClientSession()
    await vault.populate({
        'id': AiohttpSession(
            server='http://localhost',
            aiohttp_session=sample_session,
        ),
    })
    actual_session = await fn()
    async with actual_session:
        assert actual_session is sample_session


async def test_user_agent_with_a_default_session(vault):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
        ),
    })
    session = await fn()
    async with session:
        assert session.headers['User-Agent'].startswith('kopf/')


async def test_user_agent_with_a_custom_aiohttp_session(vault):
    await vault.populate({
        'id': AiohttpSession(
            server='http://localhost',
            aiohttp_session=aiohttp.ClientSession(),  # no headers, no user-agent
        ),
    })
    session = await fn()
    async with session:
        assert session.headers['User-Agent'].startswith('kopf/')  # injected regardless


async def test_custom_user_agent_preserved(vault):
    headers = {'User-Agent': 'myoperator/1.2.3'}
    await vault.populate({
        'id': AiohttpSession(
            server='http://localhost',
            aiohttp_session=aiohttp.ClientSession(headers=headers),
        ),
    })
    session = await fn()
    async with session:
        assert session.headers['User-Agent'] == 'myoperator/1.2.3'
