import base64
import json
import ssl

import aiohttp
import pytest

from kopf.reactor.admission import AmbiguousResourceError, MissingDataError, \
                                   UnknownResourceError, WebhookError
from kopf.toolkits.webhooks import WebhookK3dServer, WebhookMinikubeServer, WebhookServer


async def test_starts_as_http_ipv4(responder):
    server = WebhookServer(addr='127.0.0.1', port=22533, path='/p1/p2', insecure=True)
    async for client_config in server(responder.fn):
        assert client_config['url'] == 'http://127.0.0.1:22533/p1/p2'
        assert 'caBundle' not in client_config
        break  # do not sleep


async def test_starts_as_http_ipv6(responder):
    server = WebhookServer(addr='::1', port=22533, path='/p1/p2', insecure=True)
    async for client_config in server(responder.fn):
        assert client_config['url'] == 'http://[::1]:22533/p1/p2'
        assert 'caBundle' not in client_config
        break  # do not sleep


async def test_unspecified_port_allocates_a_random_port(responder):
    server1 = WebhookServer(addr='127.0.0.1', path='/p1/p2', insecure=True)
    server2 = WebhookServer(addr='127.0.0.1', path='/p1/p2', insecure=True)
    async for client_config1 in server1(responder.fn):
        async for client_config2 in server2(responder.fn):
            assert client_config1['url'] != client_config2['url']
            break  # do not sleep
        break  # do not sleep


async def test_unspecified_addr_uses_all_interfaces(responder, caplog, assert_logs):
    caplog.set_level(0)
    server = WebhookServer(port=22533, path='/p1/p2', insecure=True)
    async for client_config in server(responder.fn):
        assert client_config['url'] == 'http://localhost:22533/p1/p2'
        break  # do not sleep
    assert_logs([r"Listening for webhooks at http://\*:22533/p1/p2"])


async def test_webhookserver_starts_as_https_with_selfsigned_cert(
        responder):
    server = WebhookServer(addr='127.0.0.1', port=22533, path='/p1/p2', host='somehost')
    async for client_config in server(responder.fn):
        assert client_config['url'] == 'https://somehost:22533/p1/p2'
        assert 'caBundle' in client_config  # regardless of the value
        break  # do not sleep


async def test_webhookserver_starts_as_https_with_provided_cert(
        certfile, pkeyfile, certpkey, responder):
    server = WebhookServer(port=22533, certfile=certfile, pkeyfile=pkeyfile)
    async for client_config in server(responder.fn):
        assert client_config['url'] == 'https://localhost:22533'
        assert base64.b64decode(client_config['caBundle']) == certpkey[0]
        break  # do not sleep


@pytest.mark.parametrize('cls, url', [
    (WebhookK3dServer, 'https://host.k3d.internal:22533/p1/p2'),
    (WebhookMinikubeServer, 'https://host.minikube.internal:22533/p1/p2'),
])
async def test_webhookserver_flavours_inject_hostnames(
        certfile, pkeyfile, certpkey, responder, cls, url):
    server = cls(port=22533, certfile=certfile, pkeyfile=pkeyfile, path='/p1/p2')
    async for client_config in server(responder.fn):
        assert client_config['url'] == url
        break  # do not sleep


@pytest.mark.usefixtures('no_sslproto_warnings')
async def test_webhookserver_serves(
        certfile, pkeyfile, responder, adm_request):
    responder.fut.set_result({'hello': 'world'})
    server = WebhookServer(certfile=certfile, pkeyfile=pkeyfile)
    async for client_config in server(responder.fn):
        cadata = base64.b64decode(client_config['caBundle']).decode('ascii')
        sslctx = ssl.create_default_context(cadata=cadata)
        async with aiohttp.ClientSession() as client:
            async with client.post(client_config['url'], ssl=sslctx, json=adm_request) as resp:
                text = await resp.text()
                assert text == '{"hello": "world"}'
                assert resp.status == 200
        break  # do not sleep


@pytest.mark.parametrize('code, error', [
    (500, Exception),
    (400, WebhookError),
    (400, WebhookError),
    (400, MissingDataError),
    (404, UnknownResourceError),
    (409, AmbiguousResourceError),
    (400, lambda: json.JSONDecodeError('...', '...', 0)),
])
@pytest.mark.usefixtures('no_sslproto_warnings')
async def test_webhookserver_errors(
        certfile, pkeyfile, responder, adm_request, code, error):
    responder.fut.set_exception(error())
    server = WebhookServer(certfile=certfile, pkeyfile=pkeyfile)
    async for client_config in server(responder.fn):
        cadata = base64.b64decode(client_config['caBundle']).decode('ascii')
        sslctx = ssl.create_default_context(cadata=cadata)
        async with aiohttp.ClientSession() as client:
            async with client.post(client_config['url'], ssl=sslctx, json=adm_request) as resp:
                assert resp.status == code
        break  # do not sleep
