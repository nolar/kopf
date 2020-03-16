import pytest

import kopf
from kopf.reactor.handling import subregistry_var, handler_var
from kopf.reactor.invocation import context
from kopf.reactor.registries import OperatorRegistry, ResourceChangingRegistry
from kopf.structs.handlers import ErrorsMode, Activity, Reason, HANDLER_REASONS
from kopf.structs.resources import Resource


def test_on_startup_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.startup()
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.STARTUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.STARTUP
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None


def test_on_cleanup_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.cleanup()
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.CLEANUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.CLEANUP
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None


def test_on_probe_minimal():
    registry = kopf.get_default_registry()

    @kopf.on.probe()
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.PROBE)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.PROBE
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None


# Resume handlers are mixed-in into all resource-changing reactions with initial listing.
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_on_resume_minimal(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_create_minimal(mocker):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)

    @kopf.on.create('group', 'version', 'plural')
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
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

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
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

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
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

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None


def test_on_field_fails_without_field():
    with pytest.raises(TypeError):
        @kopf.on.field('group', 'version', 'plural')
        def fn(**_):
            pass


def test_on_startup_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.startup(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78)
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.STARTUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.STARTUP
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78


def test_on_cleanup_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.cleanup(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78)
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.CLEANUP)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.CLEANUP
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78


def test_on_probe_with_all_kwargs(mocker):
    registry = OperatorRegistry()

    @kopf.on.probe(
        id='id', registry=registry,
        errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78)
    def fn(**_):
        pass

    handlers = registry.activity_handlers.get_handlers(activity=Activity.PROBE)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].activity == Activity.PROBE
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78


# Resume handlers are mixed-in into all resource-changing reactions with initial listing.
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_on_resume_with_all_kwargs(mocker, reason):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.resume('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78,
                    deleted=True,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].deleted == True
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_on_create_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.CREATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.create('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.CREATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_on_update_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.update('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.UPDATE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_all_kwargs(mocker, optional):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=Reason.DELETE)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.delete('group', 'version', 'plural',
                    id='id', registry=registry,
                    errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78,
                    optional=optional,
                    labels={'somelabel': 'somevalue'},
                    annotations={'someanno': 'somevalue'},
                    when=when)
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason == Reason.DELETE
    assert handlers[0].field is None
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_on_field_with_all_kwargs(mocker):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    diff = [('op', ('field', 'subfield'), 'old', 'new')]
    cause = mocker.MagicMock(resource=resource, reason=Reason.UPDATE, diff=diff)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.field('group', 'version', 'plural', 'field.subfield',
                   id='id', registry=registry,
                   errors=ErrorsMode.PERMANENT, timeout=123, retries=456, backoff=78,
                   labels={'somelabel': 'somevalue'},
                   annotations={'someanno': 'somevalue'},
                   when=when)
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].field ==('field', 'subfield')
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when


def test_subhandler_fails_with_no_parent_handler():

    registry = ResourceChangingRegistry()
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

    registry = ResourceChangingRegistry()
    subregistry_var.set(registry)

    with context([(handler_var, parent_handler)]):
        @kopf.on.this()
        def fn(**_):
            pass

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(mocker, parent_handler):
    cause = mocker.MagicMock(reason=Reason.UPDATE, diff=None)

    registry = ResourceChangingRegistry()
    subregistry_var.set(registry)

    def fn(**_):
        pass

    with context([(handler_var, parent_handler)]):
        kopf.register(fn)

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


@pytest.mark.parametrize('decorator, kwargs', [
    (kopf.on.event, {}),
    (kopf.on.resume, {}),
    (kopf.on.create, {}),
    (kopf.on.update, {}),
    (kopf.on.delete, {}),
    (kopf.on.field, dict(field='x')),
])
def test_labels_filter_with_nones(resource, decorator, kwargs):

    with pytest.deprecated_call(match=r"`None` for label filters is deprecated"):
        @decorator(resource.group, resource.version, resource.plural, **kwargs,
                   labels={'x': None})
        def fn(**_):
            pass


@pytest.mark.parametrize('decorator, kwargs', [
    (kopf.on.event, {}),
    (kopf.on.resume, {}),
    (kopf.on.create, {}),
    (kopf.on.update, {}),
    (kopf.on.delete, {}),
    (kopf.on.field, dict(field='f')),
])
def test_annotations_filter_with_nones(resource, decorator, kwargs):

    with pytest.deprecated_call(match=r"`None` for annotation filters is deprecated"):
        @decorator(resource.group, resource.version, resource.plural, **kwargs,
                   annotations={'x': None})
        def fn(**_):
            pass
