from kopf._cogs.clients.api import get_default_namespace


async def test_default_namespace_when_unset(mocker, enforced_context):
    mocker.patch.object(enforced_context, 'default_namespace', None)
    ns = await get_default_namespace()
    assert ns is None


async def test_default_namespace_when_set(mocker, enforced_context):
    mocker.patch.object(enforced_context, 'default_namespace', 'xyz')
    ns = await get_default_namespace()
    assert ns == 'xyz'
