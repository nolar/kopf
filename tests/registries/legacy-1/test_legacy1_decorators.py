import pytest

import kopf
from kopf import SimpleRegistry, GlobalRegistry
from kopf.reactor.handling import handler_var
from kopf.reactor.handling import subregistry_var
from kopf.reactor.invocation import context
from kopf.structs.handlers import Reason
from kopf.structs.resources import Resource


def test_on_create_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)

    @kopf.on.create('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_update_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)

    @kopf.on.update('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_delete_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)

    @kopf.on.delete('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_field_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_field_fails_without_field():
    with pytest.raises(TypeError):
        @kopf.on.field('group', 'version', 'plural')
        def fn(**_):
            pass


def test_on_create_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.create('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when

def test_on_update_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.update('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_all_kwargs(mocker, optional):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.delete('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry, optional=optional,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_on_field_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.field('group', 'version', 'plural', 'field.subfield',
                   id='id', timeout=123, registry=registry,
                   labels={'somelabel': 'somevalue'},
                   annotations={'someanno': 'somevalue'},
                   when=when)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use OperatorRegistry.get_resource_changing_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field ==('field', 'subfield')
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_subhandler_fails_with_no_parent_handler():

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    # Check if the contextvar is indeed not set (as a prerequisite).
    with pytest.raises(LookupError):
        handler_var.get()

    # Check the actual behaviour of the decorator.
    with pytest.raises(LookupError):
        @kopf.on.this()
        def fn(**_):
            pass


def test_subhandler_declaratively(mocker, parent_handler):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    with context([(handler_var, parent_handler)]):
        @kopf.on.this()
        def fn(**_):
            pass

    with pytest.deprecated_call(match=r"use ResourceChangingRegistry.get_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(mocker, parent_handler):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    def fn(**_):
        pass

    with context([(handler_var, parent_handler)]):
        kopf.register(fn)

    with pytest.deprecated_call(match=r"use ResourceChangingRegistry.get_handlers\(\)"):
        handlers = registry.get_cause_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].fn is fn
