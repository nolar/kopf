import pytest

from kopf._cogs.structs.credentials import ConnectionInfo, LoginError, Vault
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.activities import authenticate
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import Activity
from kopf._core.intents.handlers import ActivityHandler
from kopf._core.intents.registries import OperatorRegistry


async def test_empty_registry_produces_no_credentials(settings):
    vault = Vault()
    registry = OperatorRegistry()

    await authenticate(
        registry=registry,
        settings=settings,
        vault=vault,
        memo=Memo(),
        indices=OperatorIndexers().indices,
    )

    assert vault.is_empty()
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_noreturn_handler_produces_no_credentials(settings):
    vault = Vault()
    registry = OperatorRegistry()

    def login_fn(**_):
        pass

    # NB: id auto-detection does not work, as it is local to the test function.
    registry._activities.append(ActivityHandler(
        fn=login_fn, id='login_fn', activity=Activity.AUTHENTICATION,
        param=None, errors=None, timeout=None, retries=None, backoff=None,
    ))

    await authenticate(
        registry=registry,
        settings=settings,
        vault=vault,
        memo=Memo(),
        indices=OperatorIndexers().indices,
    )

    assert vault.is_empty()
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_single_credentials_provided_to_vault(settings):
    info = ConnectionInfo(server='https://expected/')
    vault = Vault()
    registry = OperatorRegistry()

    def login_fn(**_):
        return info

    # NB: id auto-detection does not work, as it is local to the test function.
    registry._activities.append(ActivityHandler(
        fn=login_fn, id='login_fn', activity=Activity.AUTHENTICATION,
        param=None, errors=None, timeout=None, retries=None, backoff=None,
    ))

    await authenticate(
        registry=registry,
        settings=settings,
        vault=vault,
        memo=Memo(),
        indices=OperatorIndexers().indices,
    )

    assert not vault.is_empty()

    items = []
    async for key, info in vault:
        items.append((key, info))

    assert len(items) == 1
    assert items[0][0] == 'login_fn'
    assert items[0][1] is info
