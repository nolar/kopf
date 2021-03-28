import asyncio

import pytest

from kopf.toolkits.webhooks import WebhookNgrokTunnel


async def test_missing_pyngrok(no_pyngrok, responder):
    with pytest.raises(ImportError) as err:
        server = WebhookNgrokTunnel()
        async for _ in server(responder.fn):
            break  # do not sleep
    assert "pip install pyngrok" in str(err.value)


async def test_ngrok_tunnel(
        certfile, pkeyfile, responder, pyngrok_mock):

    responder.fut.set_result({'hello': 'world'})
    server = WebhookNgrokTunnel(port=54321, path='/p1/p2',
                                region='xx', token='xyz', binary='/bin/ngrok')
    async for client_config in server(responder.fn):
        assert 'caBundle' not in client_config  # trust the default CA
        assert client_config['url'] == 'https://nowhere/p1/p2'
        break  # do not sleep

    assert pyngrok_mock.conf.get_default.called
    assert pyngrok_mock.conf.get_default.return_value.ngrok_path == '/bin/ngrok'
    assert pyngrok_mock.conf.get_default.return_value.region == 'xx'
    assert pyngrok_mock.ngrok.set_auth_token.called
    assert pyngrok_mock.ngrok.set_auth_token.call_args_list[0][0][0] == 'xyz'
    assert pyngrok_mock.ngrok.connect.called
    assert pyngrok_mock.ngrok.connect.call_args_list[0][0][0] == '54321'
    assert pyngrok_mock.ngrok.connect.call_args_list[0][1]['bind_tls'] == True
    assert not pyngrok_mock.ngrok.disconnect.called

    await asyncio.get_running_loop().shutdown_asyncgens()
