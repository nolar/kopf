import datetime

import freezegun
import iso8601
import pytest

from kopf._cogs.structs.credentials import ConnectionInfo, LoginError, Vault, VaultKey


async def test_probits_evaluating_as_boolean():
    vault = Vault()
    with pytest.raises(NotImplementedError):
        bool(vault)


async def test_empty_at_creation():
    vault = Vault()
    assert vault.is_empty()


async def test_not_empty_when_populated():
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    await vault.populate({key1: info1})
    assert not vault.is_empty()


async def test_yielding_after_creation(mocker):
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass

    assert vault._guard.wait_for.called


async def test_yielding_after_population(mocker):
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})

    results = []
    async for key, info in vault:
        results.append((key, info))

    assert len(results) == 1
    assert results[0][0] == key1
    assert results[0][1] is info1


@freezegun.freeze_time('2020-01-01T00:00:00')
async def test_yielding_items_before_expiration(mocker):
    future = iso8601.parse_date('2020-01-01T00:00:00.000001')
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/', expiration=future)
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    results = []
    await vault.populate({key1: info1})
    async for key, info in vault:
        results.append((key, info))

    assert len(results) == 1
    assert results[0][0] == key1
    assert results[0][1] is info1


@pytest.mark.parametrize('delta', [0, 1])
@freezegun.freeze_time('2020-01-01T00:00:00')
async def test_yielding_ignores_expired_items(mocker, delta):
    future = iso8601.parse_date('2020-01-01T00:00:00.000001')
    past = iso8601.parse_date('2020-01-01') - datetime.timedelta(microseconds=delta)
    key1 = VaultKey('some-key')
    key2 = VaultKey('other-key')
    info1 = ConnectionInfo(server='https://expected/', expiration=past)
    info2 = ConnectionInfo(server='https://expected/', expiration=future)
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    results = []
    await vault.populate({key1: info1, key2: info2})
    async for key, info in vault:
        results.append((key, info))

    assert len(results) == 1
    assert results[0][0] == key2
    assert results[0][1] is info2


@pytest.mark.parametrize('delta', [0, 1])
@freezegun.freeze_time('2020-01-01T00:00:00')
async def test_yielding_when_everything_is_expired(mocker, delta):
    past = iso8601.parse_date('2020-01-01') - datetime.timedelta(microseconds=delta)
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/', expiration=past)
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_invalidation_reraises_if_nothing_is_left_with_exception(mocker):
    exc = Exception("Sample error.")
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    with pytest.raises(Exception) as e:
        await vault.invalidate(key1, info1, exc=exc)

    assert isinstance(e.value, LoginError)
    assert e.value.__cause__ is exc
    assert vault._guard.wait_for.called


async def test_invalidation_continues_if_nothing_is_left_without_exception(mocker):
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    await vault.invalidate(key1, info1)

    assert vault._guard.wait_for.called


async def test_invalidation_continues_if_something_is_left():
    exc = Exception("Sample error.")
    key1 = VaultKey('key1')
    key2 = VaultKey('key2')
    info1 = ConnectionInfo(server='https://server1/')
    info2 = ConnectionInfo(server='https://server2/')
    vault = Vault()

    await vault.populate({key1: info1})
    await vault.populate({key2: info2})
    await vault.invalidate(key1, info1, exc=exc)  # no exception!

    results = []
    async for key, info in vault:
        results.append((key, info))

    assert len(results) == 1
    assert results[0][0] == key2
    assert results[0][1] is info2


async def test_invalidation_continues_if_items_is_replaced(mocker):
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://newer-valid-credentials/')
    info2 = ConnectionInfo(server='https://older-expired-credentials/')
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    await vault.invalidate(key1, info2)

    results = []
    async for key, info in vault:
        results.append((key, info))

    assert len(results) == 1
    assert results[0][0] == key1
    assert results[0][1] is info1


async def test_yielding_after_invalidation(mocker):
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    await vault.invalidate(key1, info1)

    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_duplicates_are_remembered(mocker):
    key1 = VaultKey('some-key')
    info1 = ConnectionInfo(server='https://expected/')
    info2 = ConnectionInfo(server='https://expected/')  # another instance, same fields
    vault = Vault()
    mocker.patch.object(vault._guard, 'wait_for')

    await vault.populate({key1: info1})
    await vault.invalidate(key1, info1)
    await vault.populate({key1: info2})

    # There should be nothing to yield, despite the second populate() call.
    with pytest.raises(LoginError):
        async for _, _ in vault:
            pass


async def test_caches_from_factory(mocker):
    key1 = VaultKey('some-key')
    obj1 = object()
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    await vault.populate({key1: info1})

    def factory(_: ConnectionInfo) -> object:
        return obj1

    factory_spy = mocker.MagicMock(spec=factory, wraps=factory)

    results = []
    async for key, info, obj in vault.extended(factory_spy):
        results.append((key, info, obj))

    assert len(results) == 1
    assert results[0][0] == key1
    assert results[0][1] is info1
    assert results[0][2] is obj1

    assert factory_spy.called


async def test_caches_with_same_purpose(mocker):
    key1 = VaultKey('some-key')
    obj1 = object()
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    await vault.populate({key1: info1})

    def factory(_: ConnectionInfo) -> object:
        return obj1

    factory_spy = mocker.MagicMock(spec=factory, wraps=factory)

    async for _, _, _ in vault.extended(factory_spy, purpose='A'):
        pass

    async for _, _, _ in vault.extended(factory_spy, purpose='A'):
        pass

    assert factory_spy.call_count == 1  # called only once, not twice!


async def test_caches_with_different_purposes(mocker):
    key1 = VaultKey('some-key')
    obj1 = object()
    info1 = ConnectionInfo(server='https://expected/')
    vault = Vault()
    await vault.populate({key1: info1})

    def factory(_: ConnectionInfo) -> object:
        return obj1

    factory_spy = mocker.MagicMock(spec=factory, wraps=factory)

    async for _, _, _ in vault.extended(factory_spy, purpose='A'):
        pass

    async for _, _, _ in vault.extended(factory_spy, purpose='B'):
        pass

    assert factory_spy.call_count == 2  # once per purpose.
