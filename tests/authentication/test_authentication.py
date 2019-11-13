import pytest

from kopf.reactor.causation import Activity
from kopf.reactor.activities import authenticate
from kopf.reactor.registries import OperatorRegistry
from kopf.structs.credentials import Vault, ConnectionInfo, LoginError


async def test_empty_registry_produces_no_credentials():
    vault = Vault()
    registry = OperatorRegistry()

    await authenticate(
        registry=registry,
        vault=vault,
    )

    assert vault.readiness.is_set()
    assert not vault.emptiness.is_set()

    assert not vault
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_noreturn_handler_produces_no_credentials():
    vault = Vault()
    registry = OperatorRegistry()

    def login_fn(**_):
        pass

    registry.register_activity_handler(
        fn=login_fn,
        id='login_fn',  # auto-detection does not work, as it is local to the test function.
        activity=Activity.AUTHENTICATION,
    )

    await authenticate(
        registry=registry,
        vault=vault,
    )

    assert vault.readiness.is_set()
    assert not vault.emptiness.is_set()

    assert not vault
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_single_credentials_provided_to_vault():
    info = ConnectionInfo(server='https://expected/')
    vault = Vault()
    registry = OperatorRegistry()

    def login_fn(**_):
        return info

    registry.register_activity_handler(
        fn=login_fn,
        id='login_fn',  # auto-detection does not work, as it is local to the test function.
        activity=Activity.AUTHENTICATION,
    )

    await authenticate(
        registry=registry,
        vault=vault,
    )

    assert vault.readiness.is_set()
    assert not vault.emptiness.is_set()

    assert vault

    items = []
    async for key, info in vault:
        items.append((key, info))

    assert len(items) == 1
    assert items[0][0] == 'login_fn'
    assert items[0][1] is info
