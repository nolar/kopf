from unittest.mock import Mock

import pytest

import kopf
from kopf import OperatorRegistry
from kopf.reactor.causation import ResourceChangingCause, Reason, ALL_REASONS
from kopf.reactor.handlers import ResourceHandler
from kopf.structs.dicts import parse_field


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


def _never(**_):
    return False


def _always(**_):
    return True


matching_reason_and_decorator = pytest.mark.parametrize('reason, decorator', [
    (Reason.CREATE, kopf.on.create),
    (Reason.UPDATE, kopf.on.update),
    (Reason.DELETE, kopf.on.delete),
])

matching_reason_and_decorator_with_field = pytest.mark.parametrize('reason, decorator', [
    (Reason.CREATE, kopf.on.field),
    (Reason.UPDATE, kopf.on.field),
    (Reason.DELETE, kopf.on.field),
])

mismatching_reason_and_decorator = pytest.mark.parametrize('reason, decorator', [
    (Reason.CREATE, kopf.on.update),
    (Reason.CREATE, kopf.on.delete),
    (Reason.UPDATE, kopf.on.create),
    (Reason.UPDATE, kopf.on.delete),
    (Reason.DELETE, kopf.on.create),
    (Reason.DELETE, kopf.on.update),
])


@pytest.fixture()
def registry():
    return OperatorRegistry()


@pytest.fixture()
def handler_factory(registry, resource):
    def factory(**kwargs):
        handler = ResourceHandler(**dict(dict(
            fn=some_fn, id='a',
            errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
            initial=None, deleted=None, requires_finalizer=None,
            annotations=None, labels=None, when=None, field=None,
            reason=None,
        ), **kwargs))
        registry.resource_changing_handlers[resource].append(handler)
        return handler
    return factory


@pytest.fixture(params=[
    pytest.param(None, id='without-diff'),
    pytest.param([], id='with-empty-diff'),
])
def cause_no_diff(request, resource):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue'}}}
    return Mock(resource=resource, reason='some-reason', diff=request.param, body=body)


@pytest.fixture(params=[
    pytest.param([('op', ('some-field',), 'old', 'new')], id='with-field-diff'),
])
def cause_with_diff(resource):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue'}}}
    diff = [('op', ('some-field',), 'old', 'new')]
    return Mock(resource=resource, reason='some-reason', diff=diff, body=body)


@pytest.fixture(params=[
    pytest.param(None, id='without-diff'),
    pytest.param([], id='with-empty-diff'),
    pytest.param([('op', ('some-field',), 'old', 'new')], id='with-field-diff'),
])
def cause_any_diff(resource, request):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue'}}}
    return Mock(resource=resource, reason='some-reason', diff=request.param, body=body)


#
# "Catch-all" handlers are those with event == None.
#

def test_catchall_handlers_without_field_found(
        cause_any_diff, registry, handler_factory):
    cause = cause_any_diff
    handler_factory(reason=None, field=None)
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_found(
        cause_with_diff, registry, handler_factory):
    cause = cause_with_diff
    handler_factory(reason=None, field=parse_field('some-field'))
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_ignored(
        cause_no_diff, registry, handler_factory):
    cause = cause_no_diff
    handler_factory(reason=None, field=parse_field('some-field'))
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_with_labels_satisfied(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'somelabel': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def test_catchall_handlers_with_labels_not_satisfied(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'somelabel': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_exist(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'somelabel': None})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def test_catchall_handlers_with_labels_not_exist(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'somelabel': None})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_without_labels(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels=None)
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def test_catchall_handlers_with_annotations_satisfied(
        registry, handler_factory, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'someannotation': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_annotations_not_satisfied(
        registry, handler_factory, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'someannotation': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_exist(
        registry, handler_factory, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'someannotation': None})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_annotations_not_exist(
        registry, handler_factory, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'someannotation': None})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def test_catchall_handlers_without_annotations(
        registry, handler_factory, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None)
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels, annotations', [
    pytest.param({'somelabel': 'somevalue'}, {'someannotation': 'somevalue'}, id='with-label-annotation'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, {'someannotation': 'somevalue'}, id='with-extra-label-annotation'),
    pytest.param({'somelabel': 'somevalue'}, {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-label-extra-annotation'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-label-extra-annotation'),
])
def test_catchall_handlers_with_labels_and_annotations_satisfied(
        registry, handler_factory, resource, labels, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels, 'annotations': annotations}})
    handler_factory(reason=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_with_labels_and_annotations_not_satisfied(
        registry, handler_factory, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('reason', ALL_REASONS)
@pytest.mark.parametrize('when', [
    pytest.param(None, id='without-when'),
    pytest.param(lambda body=None, **_: body['spec']['name'] == 'test', id='with-when'),
    pytest.param(lambda **_: True, id='with-other-when'),
])
def test_catchall_handlers_with_when_match(
        registry, handler_factory, resource, reason, when):
    cause = ResourceChangingCause(
        resource=resource,
        reason='some-reason',
        diff=None,
        body={'spec': {'name': 'test'}},
        logger=None,
        patch=None,
        memo=None,
        initial=None
    )
    handler_factory(reason=None, when=when)
    handlers = registry.resource_changing_handlers[resource].get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('when', [
    pytest.param(lambda body=None, **_: body['spec']['name'] != "test", id='with-when'),
    pytest.param(lambda **_: False, id='with-other-when'),
])
def test_catchall_handlers_with_when_not_match(
        registry, handler_factory, resource, when):
    cause = ResourceChangingCause(
        resource=resource,
        reason='some-reason',
        diff=None,
        body={'spec': {'name': 'test'}},
        logger=None,
        patch=None,
        memo=None,
        initial=None
    )
    handler_factory(reason=None, when=when)
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


#
# Relevant handlers are those with reason matching the cause's reason.
# In the per-field handlers, also with field == 'some-field' (not 'another-field').
# In the label filtered handlers, the relevant handlers are those that ask for 'somelabel'.
# In the annotation filtered handlers, the relevant handlers are those that ask for 'someannotation'.
#


@matching_reason_and_decorator_with_field
def test_relevant_handlers_without_field_found(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               field=None)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_relevant_handlers_with_field_found(
        cause_with_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               field='some-field')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_relevant_handlers_with_field_ignored(
        cause_no_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               field='some-field')
    def some_fn(**_): ...

    cause = cause_no_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_labels_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               labels={'somelabel': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_labels_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               labels={'otherlabel': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_annotations_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               annotations={'someannotation': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_annotations_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               annotations={'otherannotation': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_filter_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               when=_always)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_filter_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               when=_never)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_without_field_ignored(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator_with_field
def test_irrelevant_handlers_with_field_ignored(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               field='another-field')
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_labels_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               labels={'somelabel': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_labels_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               labels={'otherlabel': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_annotations_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               annotations={'someannotation': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_annotations_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               annotations={'otherannotation': None})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_when_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               when=_always)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_when_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(resource.group, resource.version, resource.plural, registry=registry,
               when=_never)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)
    assert not handlers

#
# The handlers must be returned in order of registration,
# even if they are mixed with-/without- * -event/-field handlers.
#

def test_order_persisted_a(cause_with_diff, registry, resource):

    @kopf.on.create(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_1(**_): ...  # used

    @kopf.on.update(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_2(**_): ...  # filtered out

    @kopf.on.create(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_3(**_): ...  # used

    @kopf.on.field(resource.group, resource.version, resource.plural, registry=registry, field='filtered-out-field')
    def some_fn_4(**_): ...  # filtered out

    @kopf.on.field(resource.group, resource.version, resource.plural, registry=registry, field='some-field')
    def some_fn_5(**_): ...  # used

    cause = cause_with_diff
    cause.reason = Reason.CREATE
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].fn is some_fn_1
    assert handlers[1].fn is some_fn_3
    assert handlers[2].fn is some_fn_5


def test_order_persisted_b(cause_with_diff, registry, resource):

    # TODO: add registering by just `resource` or `resource.name`
    # TODO: remake it to `registry.on.field(...)`, and make `kopf.on` decorators as aliases for a default registry.
    # @registry.on.field(resource.group, resource.version, resource.plural, field='some-field')
    @kopf.on.field(resource.group, resource.version, resource.plural, registry=registry, field='some-field')
    def some_fn_1(**_): ...  # used

    @kopf.on.field(resource.group, resource.version, resource.plural, registry=registry, field='filtered-out-field')
    def some_fn_2(**_): ...  # filtered out

    @kopf.on.create(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_3(**_): ...  # used

    @kopf.on.update(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_4(**_): ...  # filtered out

    @kopf.on.create(resource.group, resource.version, resource.plural, registry=registry)
    def some_fn_5(**_): ...  # used

    cause = cause_with_diff
    cause.reason = Reason.CREATE
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].fn is some_fn_1
    assert handlers[1].fn is some_fn_3
    assert handlers[2].fn is some_fn_5

#
# Same function should not be returned twice for the same event/cause.
# Only actual for the cases when the event/cause can match multiple handlers.
#

@matching_reason_and_decorator
def test_deduplicated(
        cause_with_diff, registry, resource, reason, decorator):

    # Note: the decorators are applied bottom-up -- hence, the order of ids:
    @decorator(resource.group, resource.version, resource.plural, registry=registry, id='b')
    @decorator(resource.group, resource.version, resource.plural, registry=registry, id='a')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry.resource_changing_handlers[cause.resource].get_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].id == 'a'  # the first found one is returned
