import json
from typing import Any

import pytest

from kopf._cogs.clients.auth import APIContext, authenticated


def test_package_version():
    import kopf
    assert hasattr(kopf, '__version__')
    assert kopf.__version__  # not empty, not null


@pytest.mark.parametrize('version, useragent', [
    ('1.2.3', 'kopf/1.2.3'),
    ('1.2rc', 'kopf/1.2rc'),
    (None, 'kopf/unknown'),
])
async def test_http_user_agent_version(
        aresponses, hostname, fake_vault, mocker, version, useragent):

    mocker.patch('kopf._cogs.helpers.versions.version', version)

    @authenticated
    async def get_it(url: str, *, context: APIContext) -> dict[str, Any]:
        response = await context.session.get(url)
        return await response.json()

    async def responder(request):
        return aresponses.Response(
            content_type='application/json',
            text=json.dumps(dict(request.headers)))

    aresponses.add(hostname, '/', 'get', responder)
    returned_headers = await get_it(f"http://{hostname}/")
    assert returned_headers['User-Agent'] == useragent
    await fake_vault.close()  # to prevent ResourceWarnings for unclosed connectors
