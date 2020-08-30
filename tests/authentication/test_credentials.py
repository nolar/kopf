import base64
import ssl
import textwrap

import pytest

from kopf.clients.auth import APIContext, reauthenticated_request, vault_var
from kopf.structs.credentials import ConnectionInfo, Vault

# These are Minikube's locally geenrated certificates (CN=minikubeCA).
# They are not in any public use, and are regenerated regularly.
SAMPLE_MINIKUBE_CA = textwrap.dedent('''
-----BEGIN CERTIFICATE-----
MIIC5zCCAc+gAwIBAgIBATANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDEwptaW5p
a3ViZUNBMB4XDTE5MDUyMTA5MTgzNloXDTI5MDUxOTA5MTgzNlowFTETMBEGA1UE
AxMKbWluaWt1YmVDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANNU
9eyjlDfWZNSTXbdM9uebYseWWo6KeGGQ/OISrnVc6+AjuIv/fxtdCc8nVyLXBWu2
dlDjBqOsG2WfY7m1RVwsF0L2G8pgNZJ4eOww/PyDZzzIcB911eWiry528YB2PZQu
sN6sUItSZrHsin3dkcEZMUKcvVOY3FaNqXukCoMywZBO7QlLZasHhCCaanMFjxBx
WiqB4gxcyTlGRBSoa49agSW2r45873xmJ+JglI/tNjeobGLynYwrDvRWmrhVOFAj
QeiMr5lkzVO5cC2t84WdEihXVqFcZQUe0jfRHmmgpUxJRtiJMv7yudgnK2ALdhOY
eDVtV1wIyWgLpF2lZk8CAwEAAaNCMEAwDgYDVR0PAQH/BAQDAgKkMB0GA1UdJQQW
MBQGCCsGAQUFBwMCBggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
DQEBCwUAA4IBAQBdsqvuvK+8RJ5xqwGkdpSAK1U2LrZ3Hm0MzXoEo8GH79F1yubv
Ig1VRLHDIDY1d/fKrK4a7uulSFTFvpt6AGSB/225wJVBQUAALH1lPkTXq5TDi5jE
NqoXk3d61qf9StUEc1YehL6ZgkSknNU7ksAe5Ht0lfJlSa3DmACkI4CZJ1F5cztk
m2p3RZYkxizY2i/9P34f59F3XCNUSOW52aJgLhnMugEM0baOTHN0mcYZRGmrunT6
fs/5eZq6ZrXBu0nIkEZkEWAM2WoqDGxMlUao5IOnf289HyBxJTFGte5tysg1sJF0
JCH4VcilJllzUki594R1Yv8O5qtxkXXfXQNR
-----END CERTIFICATE-----
''').strip()

SAMPLE_MINIKUBE_CERT = textwrap.dedent('''
-----BEGIN CERTIFICATE-----
MIIDADCCAeigAwIBAgIBAjANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDEwptaW5p
a3ViZUNBMB4XDTE5MTExMzA5NDMzNFoXDTIwMTExMzA5NDMzNFowMTEXMBUGA1UE
ChMOc3lzdGVtOm1hc3RlcnMxFjAUBgNVBAMTDW1pbmlrdWJlLXVzZXIwggEiMA0G
CSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCut0LegwHP4kJp9Uf89vjuslIMi6hv
BiBNhSr8wCZ9uuFUN2dvBnCPXX/xvxYxpBKh0WUKX7sYfKdaMjxVr1ndnwu63e5A
CW7919uRH3fhVV7rTntlO+rUeyHXNlSQue8oVlvO+8D+Qzlna02axt/5PwsPGD/G
lw7Ti0f/LjmmqTB0T6yCsyOH90d7pQ0yuiOyDK072Ns1vTf6hkrkiNQaRhUjEtqm
rMq/A4xpb7h+z6LWlRyv6/DBJsLmpDS99hqZbbj1U6IJ56r0JpQuq7CJxeE/F2t5
I1i0k8TJ4CQRrHvXovl6wI+zmOTmKMy5uDdKUEkfe6vck+x16gV2RK79AgMBAAGj
PzA9MA4GA1UdDwEB/wQEAwIFoDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUH
AwIwDAYDVR0TAQH/BAIwADANBgkqhkiG9w0BAQsFAAOCAQEAbo/PxDz7K9dCAcxi
S0RdZvOu2RYHqRmc/vN1B5JDHLcYcCet4iuJ44BuWZc/Oe1sXdy6DOZO2UQHAF0F
5cAaBnkqpz/yvUzkE3NvdXpU/9leo3O6XeKKi0Fi+hv09nhh/tJgh/XDxWfAAkeG
I3AQCkcHrqFrpBMxuWUXlnexwsvvbdEVkpVMVwSRfpsxLfxV62HCU90EU0823UKW
V1npcRXBtxK/jqWZbLd5buRul+V6PKa4KRY22d8it+9MDAMgPFQosG1ShhQropul
VJUvAQ14dPpiyQN4FRI3MljVykFe2cWI0rVwoboy9TEniaMPqr3MqujOlUv7KTpk
lKRP5w==
-----END CERTIFICATE-----
''').strip()

SAMPLE_MINIKUBE_PKEY = textwrap.dedent('''
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEArrdC3oMBz+JCafVH/Pb47rJSDIuobwYgTYUq/MAmfbrhVDdn
bwZwj11/8b8WMaQSodFlCl+7GHynWjI8Va9Z3Z8Lut3uQAlu/dfbkR934VVe6057
ZTvq1Hsh1zZUkLnvKFZbzvvA/kM5Z2tNmsbf+T8LDxg/xpcO04tH/y45pqkwdE+s
grMjh/dHe6UNMrojsgytO9jbNb03+oZK5IjUGkYVIxLapqzKvwOMaW+4fs+i1pUc
r+vwwSbC5qQ0vfYamW249VOiCeeq9CaULquwicXhPxdreSNYtJPEyeAkEax716L5
esCPs5jk5ijMubg3SlBJH3ur3JPsdeoFdkSu/QIDAQABAoIBAAk8FoS8V/Qs+WWw
WUW9qBq1wjB3kUeNA1gVmdgSL/alUhOpegYcSQbK4mBcwUeObI2xC64osTLyI8ZY
sWe2BQH5zhzqbhVkakFwj2J0T1nRsVquo0cOi7L/byJ49K9RpJp1NhUSqXjHBNm6
ijeMG3qJIoSBu507jsUPr5aFUvbEFCby+VvU/UljS1dK+5wm7QcGgRrcXc8ZCIuk
1P5YX6Tr9RYzYNjc/zB8czIyoISSSTk9uroYzvuMCgYTQ4WzWvX8UwchdfZyV7Gq
kdKjG6IkGxvlS0Lc8534LRlH8iJR+wPAlYKHBDa0Rc3qZFsvgjnbDCCa6YqLoNNn
ltsz7wECgYEA5IrnX8EHXyeaYnyiVI2xk6QTkmLrtovd1Ue2GwQ9BJgtaDrT0xil
UV5NV4VUu5Zid7cqmIyFyh+7jjIex+dpfXT94+wr1HXxNYbLHeCnCZWcomxaD8pU
Bh8B8wSRgjOFW33q6APDkFJPO92O96B+BczSgOFmEvuk8Kj7aYFahWUCgYEAw7Ta
YiD66I+eK8B2+lWfPNfpIddW7D3Dn9cSW2RVazMyinTsVBm3p63kpCQeVaaX5key
WiyY6phTvIfJ45pTzrS/kpA2zGcB1FFnB1xvM0bzpbIxTOQBGQUH1mSQmNT6+0VZ
+GdILRKedp4qg7BLh7VElSCYGVy1Yr62Rp01FbkCgYB3QZxWtQ05tBq1hb/XS1D8
b8PewUuqp/WL063NDzsf6KDZIMlkABpUCVdmciay9FhRi/zoOXue60wdeT3ipni/
hIrvok+EwD6r5bib0JyZPb7MaqncT4Hk581GmH2taWEPSveHNl+YMbsyy/xMby0T
rbuykOuIwFNjWWpHtb4cmQKBgGF4MUuuIUiyPpSLxrXm7ufeoL26AhCmskdpVjsu
PVymowVSNmGsbUuVz8nwMyt1TTHjg3BlxcMRGqNK/cHdmt/YJZFZQfGLW93irO19
m+Rt8esUVHl3FRTg7IZaj6mOaXG7mJOe3NOV8lYhcAsmQnfUT9P158q54ZzMXvvM
UCQBAoGBAMTbVdbybvzqOSIKGeqIVlX7L2Fp55lYC8MfTwQzjPeJMQ8YdeNquv+M
hweBrRg1DXxdaicfpCuqITU1WkqHd/NGNQX2h8VleiR6t22dZb8nBplO1l1e3XgY
lUXVsCYgw8yNCm10xGCelpJ4nxxPhf5apbz4F3nGORGfsv5C+x++
-----END RSA PRIVATE KEY-----
''').strip()


@reauthenticated_request
async def fn(context: APIContext):
    return context.session


@pytest.fixture(autouse=True)
def vault():
    vault = Vault()
    vault_var.set(vault)
    return vault


@pytest.fixture
def cabase64(tmpdir):
    return base64.encodebytes(SAMPLE_MINIKUBE_CA.encode('ascii'))


@pytest.fixture
def certbase64(tmpdir):
    return base64.encodebytes(SAMPLE_MINIKUBE_CERT.encode('ascii'))


@pytest.fixture
def pkeybase64(tmpdir):
    return base64.encodebytes(SAMPLE_MINIKUBE_PKEY.encode('ascii'))


@pytest.fixture
def cafile(tmpdir):
    path = tmpdir / 'ca.crt'
    path.write_text(SAMPLE_MINIKUBE_CA, encoding='utf-8')
    return str(path)


@pytest.fixture
def certfile(tmpdir):
    path = tmpdir / 'client.crt'
    path.write_text(SAMPLE_MINIKUBE_CERT, encoding='utf-8')
    return str(path)


@pytest.fixture
def pkeyfile(tmpdir):
    path = tmpdir / 'client.key'
    path.write_text(SAMPLE_MINIKUBE_PKEY, encoding='utf-8')
    return str(path)


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


async def test_ca_insecure(vault, cafile):
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


async def test_ca_as_path(vault, cafile):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_path=cafile,
        ),
    })
    session = await fn()

    async with session:
        ctx = session.connector._ssl
        assert len(ctx.get_ca_certs()) == 1
        assert ctx.cert_store_stats()['x509'] == 1
        assert ctx.cert_store_stats()['x509_ca'] == 1


async def test_ca_as_data(vault, cabase64):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_data=cabase64,
        ),
    })
    session = await fn()

    async with session:
        ctx = session.connector._ssl
        assert len(ctx.get_ca_certs()) == 1
        assert ctx.cert_store_stats()['x509'] == 1
        assert ctx.cert_store_stats()['x509_ca'] == 1


# TODO: find a way to test that the client certificates/pkeys are indeed loaded.
# TODO: currently, we only test that the parsing/loading does not fail at all.
async def test_clientcert_as_path(vault, cafile, certfile, pkeyfile):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_path=cafile,
            certificate_path=certfile,
            private_key_path=pkeyfile,
        ),
    })
    session = await fn()

    async with session:
        pass


async def test_clientcert_as_data(vault, cafile, certbase64, pkeybase64):
    await vault.populate({
        'id': ConnectionInfo(
            server='http://localhost',
            ca_path=cafile,
            certificate_data=certbase64,
            private_key_data=pkeybase64,
        ),
    })
    session = await fn()

    async with session:
        pass
