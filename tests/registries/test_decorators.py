import pytest

import kopf
from kopf.reactor.causation import CREATE, UPDATE, DELETE
from kopf.reactor.handling import subregistry_var
from kopf.reactor.registries import Resource, SimpleRegistry, GlobalRegistry


def test_on_create_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=CREATE)

    @kopf.on.create('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == CREATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None


def test_on_update_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=UPDATE)

    @kopf.on.update('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == UPDATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None


def test_on_delete_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=DELETE)

    @kopf.on.delete('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == DELETE
    assert handlers[0].field is None
    assert handlers[0].timeout is None


def test_on_field_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, event=UPDATE, diff=diff)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].timeout is None


def test_on_field_fails_without_field():
    with pytest.raises(TypeError):
        @kopf.on.field('group', 'version', 'plural')
        def fn(**_):
            pass


def test_on_create_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=CREATE)

    @kopf.on.create('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry)
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == CREATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123


def test_on_update_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=UPDATE)

    @kopf.on.update('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry)
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == UPDATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123


def test_on_delete_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, event=DELETE)

    @kopf.on.delete('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry)
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event == DELETE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123


def test_on_field_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, event=UPDATE, diff=diff)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield',
                   id='id', timeout=123, registry=registry)
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].event is None
    assert handlers[0].field ==('field', 'subfield')
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].timeout == 123


def test_subhandler_declaratively(mocker):
    cause = mocker.MagicMock(event=UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    @kopf.on.this()
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(mocker):
    cause = mocker.MagicMock(event=UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    def fn(**_):
        pass
    kopf.register(fn)

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
