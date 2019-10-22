import pytest

import kopf
from kopf import SimpleRegistry, GlobalRegistry
from kopf.reactor.causation import Reason
from kopf.reactor.handling import subregistry_var
from kopf.structs.resources import Resource


def test_on_create_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)

    @kopf.on.create('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_update_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)

    @kopf.on.update('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


def test_on_delete_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)

    @kopf.on.delete('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].timeout is None
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

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].timeout is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None


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

    @kopf.on.create('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_on_update_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.update('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_all_kwargs(mocker, optional):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.delete('group', 'version', 'plural',
                    id='id', timeout=123, registry=registry, optional=optional,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_on_field_with_all_kwargs(mocker):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @kopf.on.field('group', 'version', 'plural', 'field.subfield',
                   id='id', timeout=123, registry=registry,
                   labels={'somelabel': 'somevalue'},
                   annotations={'someanno': 'somevalue'})
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field ==('field', 'subfield')
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].timeout == 123
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}


def test_subhandler_declaratively(mocker):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    @kopf.on.this()
    def fn(**_):
        pass

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(mocker):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = SimpleRegistry()
    subregistry_var.set(registry)

    def fn(**_):
        pass
    kopf.register(fn)

    handlers = registry.get_cause_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
