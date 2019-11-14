import pytest

import kopf
from kopf.reactor.causation import Reason, Activity, HANDLER_REASONS
from kopf.reactor.handling import subregistry_var
from kopf.reactor.registries import ErrorsMode, OperatorRegistry, ResourceChangingRegistry
from kopf.structs.resources import Resource


def test_on_startup_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.startup()
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.STARTUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.STARTUP
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None


def test_on_cleanup_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.cleanup()
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.CLEANUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.CLEANUP
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None


def test_on_probe_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.probe()
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.PROBE)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.PROBE
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None


# Resume handlers are mixed-in into all resource-changing reactions with initial listing.
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_on_resume_minimal(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_create_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)

    @kopf.on.create('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_update_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)

    @kopf.on.update('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_delete_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)

    @kopf.on.delete('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_field_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield')
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].cooldown is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_field_fails_without_field():
    with pytest.raises(TypeError):
        @kopf.on.field('group', 'version', 'plural')
        def fn(**_):
            pass


def test_on_startup_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.startup(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78)
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.STARTUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.STARTUP
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78


def test_on_cleanup_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.cleanup(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78)
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.CLEANUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.CLEANUP
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78


def test_on_probe_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.probe(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78)
    def fn(**_):
        pass

    handlers = registry.get_activity_handlers(activity=Activity.PROBE)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.PROBE
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78


# Resume handlers are mixed-in into all resource-changing reactions with initial listing.
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_on_resume_with_all_kwargs(mocker, reason):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.resume('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78,
                    deleted=True,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78
    assert handlers[0].deleted == True
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_on_create_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.create('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_on_update_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.update('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_all_kwargs(mocker, optional):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.delete('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78,
                    optional=optional,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_on_field_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield',
                   id='id', registry=registry,
                   errors=ErrorsMode.PERMANENT, timeout=123, retries=456, cooldown=78,
                   labels={'somelabel': 'somevalue'},
                   annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field ==('field', 'subfield')
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].cooldown == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_subhandler_declaratively(mocker):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = ResourceChangingRegistry()
    subregistry_var.set(registry)

    @kopf.on.this()
    def fn(**_):
        pass

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(mocker):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = ResourceChangingRegistry()
    subregistry_var.set(registry)

    def fn(**_):
        pass
    kopf.register(fn)

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
