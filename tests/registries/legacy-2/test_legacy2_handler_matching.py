import functools
from unittest.mock import Mock

import pytest

from kopf import OperatorRegistry
from kopf.reactor.causation import ResourceChangingCause
from kopf.structs.bodies import Body


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


@pytest.fixture(params=[
    pytest.param(OperatorRegistry, id='in-global-registry'),
])
def registry(request):
    return request.param()


@pytest.fixture()
def register_fn(registry, resource):
    if isinstance(registry, OperatorRegistry):
        with pytest.deprecated_call(match=r"register_resource_changing_handler\(\) is deprecated"):
            yield functools.partial(registry.register_resource_changing_handler, resource.group, resource.version, resource.plural)
    else:
        raise Exception(f"Unsupported registry type: {registry}")


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

def test_catchall_handlers_without_field_found(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason=None, field=None)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert handlers


def test_catchall_handlers_with_field_found(cause_with_diff, registry, register_fn):
    register_fn(some_fn, reason=None, field='some-field')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_with_diff)
    assert handlers


def test_catchall_handlers_with_field_ignored(cause_no_diff, registry, register_fn):
    register_fn(some_fn, reason=None, field='some-field')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_no_diff)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_with_labels_satisfied(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def test_catchall_handlers_with_labels_not_satisfied(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_exist(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def test_catchall_handlers_with_labels_not_exist(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_without_labels(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels=None)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def test_catchall_handlers_with_annotations_satisfied(registry, register_fn, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, annotations={'someannotation': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_annotations_not_satisfied(registry, register_fn, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, annotations={'someannotation': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_exist(registry, register_fn, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, annotations={'someannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_annotations_not_exist(registry, register_fn, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, annotations={'someannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def test_catchall_handlers_without_annotations(registry, register_fn, resource, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, annotations=None)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels, annotations', [
    pytest.param({'somelabel': 'somevalue'}, {'someannotation': 'somevalue'}, id='with-label-annotation'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, {'someannotation': 'somevalue'}, id='with-extra-label-annotation'),
    pytest.param({'somelabel': 'somevalue'}, {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-label-extra-annotation'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-label-extra-annotation'),
])
def test_catchall_handlers_with_labels_and_annotations_satisfied(registry, register_fn, resource, labels, annotations):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels, 'annotations': annotations}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def test_catchall_handlers_with_labels_and_annotations_not_satisfied(registry, register_fn, resource, labels):
    cause = Mock(resource=resource, reason='some-reason', diff=None, body={'metadata': {'labels': labels}})
    register_fn(some_fn, reason=None, field=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('when', [
    pytest.param(None, id='without-when'),
    pytest.param(lambda body=None, **_: body['spec']['name'] == 'test', id='with-when'),
    pytest.param(lambda **_: True, id='with-other-when'),
])
def test_catchall_handlers_with_when_match(registry, register_fn, resource, when):
    cause = ResourceChangingCause(
        resource=resource,
        reason='some-reason',
        diff=None,
        body=Body({'spec': {'name': 'test'}}),
        logger=None,
        patch=None,
        memo=None,
        initial=None
    )
    register_fn(some_fn, reason=None, field=None, when=when)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert handlers


@pytest.mark.parametrize('when', [
    pytest.param(lambda body=None, **_: body['spec']['name'] != "test", id='with-when'),
    pytest.param(lambda **_: False, id='with-other-when'),
])
def test_catchall_handlers_with_when_not_match(registry, register_fn, resource, when):
    cause = ResourceChangingCause(
        resource=resource,
        reason='some-reason',
        diff=None,
        body=Body({'spec': {'name': 'test'}}),
        logger=None,
        patch=None,
        memo=None,
        initial=None
    )
    register_fn(some_fn, reason=None, field=None, when=when)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert not handlers


#
# Relevant handlers are those with event == 'some-reason' (but not 'another-reason').
# In the per-field handlers, also with field == 'some-field' (not 'another-field').
# In the label filtered handlers, the relevant handlers are those that ask for 'somelabel'.
# In the annotation filtered handlers, the relevant handlers are those that ask for 'someannotation'.
#

def test_relevant_handlers_without_field_found(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_field_found(cause_with_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', field='some-field')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_with_diff)
    assert handlers


def test_relevant_handlers_with_field_ignored(cause_no_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', field='some-field')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_no_diff)
    assert not handlers


def test_relevant_handlers_with_labels_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', labels={'somelabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_labels_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', labels={'otherlabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_relevant_handlers_with_annotations_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', annotations={'someannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_annotations_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', annotations={'otherannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_relevant_handlers_with_filter_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', when=lambda *_: True)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_filter_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='some-reason', when=lambda *_: False)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_without_field_ignored(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_field_ignored(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', field='another-field')
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_labels_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', labels={'somelabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_labels_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', labels={'otherlabel': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_annotations_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', annotations={'someannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_annotations_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', annotations={'otherannotation': None})
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_when_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', when=lambda *_: True)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_when_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, reason='another-reason', when=lambda *_: False)
    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_any_diff)
    assert not handlers

#
# The handlers must be returned in order of registration,
# even if they are mixed with-/without- * -event/-field handlers.
#

def test_order_persisted_a(cause_with_diff, registry, register_fn):
    register_fn(functools.partial(some_fn, 1), reason=None)
    register_fn(functools.partial(some_fn, 2), reason='some-reason')
    register_fn(functools.partial(some_fn, 3), reason='filtered-out-reason')
    register_fn(functools.partial(some_fn, 4), reason=None, field='filtered-out-reason')
    register_fn(functools.partial(some_fn, 5), reason=None, field='some-field')

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].reason is None
    assert handlers[0].field is None
    assert handlers[1].reason == 'some-reason'
    assert handlers[1].field is None
    assert handlers[2].reason is None
    assert handlers[2].field == ('some-field',)


def test_order_persisted_b(cause_with_diff, registry, register_fn):
    register_fn(functools.partial(some_fn, 1), reason=None, field='some-field')
    register_fn(functools.partial(some_fn, 2), reason=None, field='filtered-out-field')
    register_fn(functools.partial(some_fn, 3), reason='filtered-out-reason')
    register_fn(functools.partial(some_fn, 4), reason='some-reason')
    register_fn(functools.partial(some_fn, 5), reason=None)

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].reason is None
    assert handlers[0].field == ('some-field',)
    assert handlers[1].reason == 'some-reason'
    assert handlers[1].field is None
    assert handlers[2].reason is None
    assert handlers[2].field is None

#
# Same function should not be returned twice for the same event/cause.
# Only actual for the cases when the event/cause can match multiple handlers.
#

def test_deduplicated(cause_with_diff, registry, register_fn):
    register_fn(some_fn, reason=None, id='a')
    register_fn(some_fn, reason=None, id='b')

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause_with_diff)

    assert len(handlers) == 1
    assert handlers[0].id == 'a'  # the first found one is returned
