import pytest

import kopf
from kopf.structs.handlers import Activity, Reason, HANDLER_REASONS
from kopf.structs.resources import Resource


def test_on_startup_with_cooldown():
    registry = kopf.get_default_registry()

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.startup(cooldown=78)
        def fn(**_):
            pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.STARTUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


def test_on_cleanup_with_cooldown():
    registry = kopf.get_default_registry()

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.cleanup(cooldown=78)
        def fn(**_):
            pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.CLEANUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


def test_on_probe_with_cooldown():
    registry = kopf.get_default_registry()

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.probe(cooldown=78)
        def fn(**_):
            pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.PROBE)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


# Resume handlers are mixed-in into all resource-changing reactions with initial listing.
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_on_resume_with_cooldown(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.resume('group', 'version', 'plural', cooldown=78)
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


def test_on_create_with_cooldown(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.create('group', 'version', 'plural', cooldown=78)
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


def test_on_update_with_cooldown(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.update('group', 'version', 'plural', cooldown=78)
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_cooldown(mocker, optional):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.delete('group', 'version', 'plural', cooldown=78)
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78


def test_on_field_with_cooldown(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    with pytest.deprecated_call(match=r"use backoff="):
        @kopf.on.field('group', 'version', 'plural', 'field.subfield', cooldown=78)
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].backoff == 78

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handlers[0].cooldown == 78
