import pytest

import kopf
from kopf.reactor.handling import handler_var, subregistry_var
from kopf.reactor.invocation import context
from kopf.reactor.registries import OperatorRegistry, ResourceChangingRegistry
from kopf.structs.handlers import HANDLER_REASONS, Activity, ErrorsMode, Reason
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
def test_on_resume_minimal(reason, cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=True)

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
    assert handlers[0].field is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_create_minimal(cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.CREATE)

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
    assert handlers[0].field is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_update_minimal(cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.UPDATE)

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
    assert handlers[0].field is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_delete_minimal(cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.DELETE)

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
    assert handlers[0].field is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_field_minimal(cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    old = {'field': {'subfield': 'old'}}
    new = {'field': {'subfield': 'new'}}
    cause = cause_factory(resource=resource, reason=Reason.UPDATE, old=old, new=new, body=new)

    @kopf.on.field('group', 'version', 'plural', field='field.subfield')
    def fn(**_):
        pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
    assert handlers[0].reason is None
    assert handlers[0].errors is None
    assert handlers[0].timeout is None
    assert handlers[0].retries is None
    assert handlers[0].backoff is None
    assert handlers[0].labels is None
    assert handlers[0].annotations is None
    assert handlers[0].when is None
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_field_warns_with_positional(cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    old = {'field': {'subfield': 'old'}}
    new = {'field': {'subfield': 'new'}}
    cause = cause_factory(resource=resource, reason=Reason.UPDATE, old=old, new=new, body=new)

    with pytest.deprecated_call(match=r"Positional field name is deprecated"):
        @kopf.on.field('group', 'version', 'plural', 'field.subfield')
        def fn(**_):
            pass

    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].field == ('field', 'subfield')


def test_on_field_fails_without_field():
    with pytest.raises(TypeError):
        @kopf.on.field('group', 'version', 'plural')
        def fn(**_):
            pass


def test_on_startup_with_all_kwargs():
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
def test_on_resume_with_most_kwargs(mocker, reason, cause_factory):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=True)
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
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].deleted == True
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when
    assert handlers[0].field is None
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_create_with_most_kwargs(mocker, cause_factory):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.CREATE)
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
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when
    assert handlers[0].field is None
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_update_with_most_kwargs(mocker, cause_factory):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.UPDATE)
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
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when
    assert handlers[0].field is None
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_on_delete_with_most_kwargs(mocker, cause_factory, optional):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=Reason.DELETE)
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
    assert handlers[0].id == 'id'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when
    assert handlers[0].field is None
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


def test_on_field_with_most_kwargs(mocker, cause_factory):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    old = {'field': {'subfield': 'old'}}
    new = {'field': {'subfield': 'new'}}
    cause = cause_factory(resource=resource, reason=Reason.UPDATE, old=old, new=new, body=new)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    when = lambda **_: False

    @kopf.on.field('group', 'version', 'plural', field='field.subfield',
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
    assert handlers[0].id == 'id/field.subfield'
    assert handlers[0].errors == ErrorsMode.PERMANENT
    assert handlers[0].timeout == 123
    assert handlers[0].retries == 456
    assert handlers[0].backoff == 78
    assert handlers[0].labels == {'somelabel': 'somevalue'}
    assert handlers[0].annotations == {'someanno': 'somevalue'}
    assert handlers[0].when == when
    assert handlers[0].field == ('field', 'subfield')
    assert handlers[0].value is None
    assert handlers[0].old is None
    assert handlers[0].new is None


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


def test_subhandler_declaratively(parent_handler, cause_factory):
    cause = cause_factory(reason=Reason.UPDATE)

    registry = ResourceChangingRegistry()
    subregistry_var.set(registry)

    with context([(handler_var, parent_handler)]):
        @kopf.on.this()
        def fn(**_):
            pass

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


def test_subhandler_imperatively(parent_handler, cause_factory):
    cause = cause_factory(reason=Reason.UPDATE)

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


@pytest.mark.parametrize('decorator, causeargs, handlers_prop', [
    pytest.param(kopf.on.event, dict(), 'resource_watching_handlers', id='on-event'),
    pytest.param(kopf.on.resume, dict(reason=None, initial=True), 'resource_changing_handlers', id='on-resume'),
    pytest.param(kopf.on.create, dict(reason=Reason.CREATE), 'resource_changing_handlers', id='on-create'),
    pytest.param(kopf.on.update, dict(reason=Reason.UPDATE), 'resource_changing_handlers', id='on-update'),
    pytest.param(kopf.on.delete, dict(reason=Reason.DELETE), 'resource_changing_handlers', id='on-delete'),
    pytest.param(kopf.on.field, dict(reason=Reason.UPDATE), 'resource_changing_handlers', id='on-field'),
    pytest.param(kopf.daemon, dict(), 'resource_spawning_handlers', id='on-daemon'),
    pytest.param(kopf.timer, dict(), 'resource_spawning_handlers', id='on-timer'),
])
def test_field_with_value(mocker, cause_factory, decorator, causeargs, handlers_prop):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    old = {'field': {'subfield': 'old'}}
    new = {'field': {'subfield': 'new'}}
    cause = cause_factory(resource=resource, old=old, new=new, body=new, **causeargs)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @decorator('group', 'version', 'plural', registry=registry,
               field='spec.field', value='value')
    def fn(**_):
        pass

    handlers_registry = getattr(registry, handlers_prop)
    handlers = handlers_registry[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].field == ('spec', 'field')
    assert handlers[0].value == 'value'


@pytest.mark.parametrize('decorator, causeargs, handlers_prop', [
    pytest.param(kopf.on.update, dict(reason=Reason.UPDATE), 'resource_changing_handlers', id='on-update'),
    pytest.param(kopf.on.field, dict(reason=Reason.UPDATE), 'resource_changing_handlers', id='on-field'),
])
def test_field_with_oldnew(mocker, cause_factory, decorator, causeargs, handlers_prop):
    registry = OperatorRegistry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, **causeargs)
    mocker.patch('kopf.reactor.registries.match', return_value=True)

    @decorator('group', 'version', 'plural', registry=registry,
               field='spec.field', old='old', new='new')
    def fn(**_):
        pass

    handlers_registry = getattr(registry, handlers_prop)
    handlers = handlers_registry[resource].get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].field == ('spec', 'field')
    assert handlers[0].value is None
    assert handlers[0].old == 'old'
    assert handlers[0].new == 'new'


@pytest.mark.parametrize('decorator', [
    pytest.param(kopf.on.event, id='on-event'),
    pytest.param(kopf.on.resume, id='on-resume'),
    pytest.param(kopf.on.create, id='on-create'),
    pytest.param(kopf.on.update, id='on-update'),
    pytest.param(kopf.on.delete, id='on-delete'),
    pytest.param(kopf.daemon, id='on-daemon'),
    pytest.param(kopf.timer, id='on-timer'),
])
def test_missing_field_with_specified_value(resource, decorator):
    with pytest.raises(TypeError, match="without a mandatory field"):
        @decorator(resource.group, resource.version, resource.plural, value='v')
        def fn(**_):
            pass


@pytest.mark.parametrize('kwargs', [
    pytest.param(dict(field='f', value='v', old='x'), id='value-vs-old'),
    pytest.param(dict(field='f', value='v', new='x'), id='value-vs-new'),
])
@pytest.mark.parametrize('decorator', [
    pytest.param(kopf.on.update, id='on-update'),
    pytest.param(kopf.on.field, id='on-field'),
])
def test_conflicts_of_values_vs_oldnew(resource, decorator, kwargs):
    with pytest.raises(TypeError, match="Either value= or old=/new="):
        @decorator(resource.group, resource.version, resource.plural, **kwargs)
        def fn(**_):
            pass


@pytest.mark.parametrize('decorator', [
    pytest.param(kopf.on.resume, id='on-resume'),
    pytest.param(kopf.on.create, id='on-create'),
    pytest.param(kopf.on.delete, id='on-delete'),
])
def test_invalid_oldnew_for_inappropriate_subhandlers(resource, decorator, registry):

    @decorator(resource.group, resource.version, resource.plural)
    def fn(**_):
        @kopf.on.this(field='f', old='x')
        def fn2(**_):
            pass

    subregistry = ResourceChangingRegistry()
    handler = registry.resource_changing_handlers[resource].get_all_handlers()[0]
    with context([(handler_var, handler), (subregistry_var, subregistry)]):
        with pytest.raises(TypeError, match="can only be used in update handlers"):
            handler.fn()
