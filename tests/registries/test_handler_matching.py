import functools
from unittest.mock import Mock

import pytest

from kopf import SimpleRegistry, GlobalRegistry


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


@pytest.fixture(params=[
    pytest.param(SimpleRegistry, id='in-simple-registry'),
    pytest.param(GlobalRegistry, id='in-global-registry'),
])
def registry(request):
    return request.param()


@pytest.fixture()
def register_fn(registry, resource):
    if isinstance(registry, SimpleRegistry):
        return registry.register
    if isinstance(registry, GlobalRegistry):
        return functools.partial(registry.register_cause_handler, resource.group, resource.version, resource.plural)
    raise Exception(f"Unsupported registry type: {registry}")


@pytest.fixture(params=[
    pytest.param(None, id='without-diff'),
    pytest.param([], id='with-empty-diff'),
])
def cause_no_diff(request, resource):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}}}
    return Mock(resource=resource, event='some-event', diff=request.param, body=body)


@pytest.fixture(params=[
    pytest.param([('op', ('some-field',), 'old', 'new')], id='with-field-diff'),
])
def cause_with_diff(resource):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}}}
    diff = [('op', ('some-field',), 'old', 'new')]
    return Mock(resource=resource, event='some-event', diff=diff, body=body)


@pytest.fixture(params=[
    pytest.param(None, id='without-diff'),
    pytest.param([], id='with-empty-diff'),
    pytest.param([('op', ('some-field',), 'old', 'new')], id='with-field-diff'),
])
def cause_any_diff(resource, request):
    body = {'metadata': {'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue'}}}
    return Mock(resource=resource, event='some-event', diff=request.param, body=body)


@pytest.fixture(params=[
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def cause_with_label_value(resource, request):
    body = {'metadata': {'labels': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def cause_without_label_value(resource, request):
    body = {'metadata': {'labels': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
])
def cause_with_label_exists(resource, request):
    body = {'metadata': {'labels': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-label'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
])
def cause_without_label_exists(resource, request):
    body = {'metadata': {'labels': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-label'),
    pytest.param({'somelabel': 'somevalue'}, id='with-label'),
    pytest.param({'somelabel': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherlabel': 'othervalue'}, id='with-other-label'),
    pytest.param({'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, id='with-extra-label'),
])
def cause_any_labels(resource, request):
    body = {'metadata': {'labels': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def cause_with_annotation_value(resource, request):
    body = {'metadata': {'annotations': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def cause_without_annotation_value(resource, request):
    body = {'metadata': {'annotations': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
])
def cause_with_annotation_exists(resource, request):
    body = {'metadata': {'annotations': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-annotation'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
])
def cause_without_annotation_exists(resource, request):
    body = {'metadata': {'annotations': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({}, id='without-annotation'),
    pytest.param({'someannotation': 'somevalue'}, id='with-annotation'),
    pytest.param({'someannotation': 'othervalue'}, id='with-other-value'),
    pytest.param({'otherannotation': 'othervalue'}, id='with-other-annotation'),
    pytest.param({'someannotation': 'somevalue', 'otherannotation': 'othervalue'}, id='with-extra-annotation'),
])
def cause_any_annotations(resource, request):
    body = {'metadata': {'annotations': request.param}}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


@pytest.fixture(params=[
    pytest.param({'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue'}}, id='with-label-annotation'),
    pytest.param({'labels': {'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, 'annotations': {'someannotation': 'somevalue'}}, id='with-extra-label-annotation'),
    pytest.param({'labels': {'somelabel': 'somevalue'}, 'annotations': {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}}, id='with-label-extra-annotation'),
    pytest.param({'labels': {'somelabel': 'somevalue', 'otherlabel': 'othervalue'}, 'annotations': {'someannotation': 'somevalue', 'otherannotation': 'othervalue'}}, id='with-extra-label-extra-annotation'),
])
def cause_labels_and_annotations(resource, request):
    body = {'metadata': request.param}
    return Mock(resource=resource, event='some-event', diff=None, body=body)


#
# "Catch-all" handlers are those with event == None.
#

def test_catchall_handlers_without_field_found(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event=None, field=None)
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert handlers


def test_catchall_handlers_with_field_found(cause_with_diff, registry, register_fn):
    register_fn(some_fn, event=None, field='some-field')
    handlers = registry.get_cause_handlers(cause_with_diff)
    assert handlers


def test_catchall_handlers_with_field_ignored(cause_no_diff, registry, register_fn):
    register_fn(some_fn, event=None, field='some-field')
    handlers = registry.get_cause_handlers(cause_no_diff)
    assert not handlers


def test_catchall_handlers_with_labels_satisfied(cause_with_label_value, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_with_label_value)
    assert handlers


def test_catchall_handlers_with_labels_not_satisfied(cause_without_label_value, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_without_label_value)
    assert not handlers


def test_catchall_handlers_with_labels_exist(cause_with_label_exists, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': None})
    handlers = registry.get_cause_handlers(cause_with_label_exists)
    assert handlers


def test_catchall_handlers_with_labels_not_exist(cause_without_label_exists, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': None})
    handlers = registry.get_cause_handlers(cause_without_label_exists)
    assert not handlers


def test_catchall_handlers_without_labels(cause_any_labels, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels=None)
    handlers = registry.get_cause_handlers(cause_any_labels)
    assert handlers


def test_catchall_handlers_with_annotations_satisfied(cause_with_annotation_value, registry, register_fn):
    register_fn(some_fn, event=None, field=None, annotations={'someannotation': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_with_annotation_value)
    assert handlers


def test_catchall_handlers_with_annotations_not_satisfied(cause_without_annotation_value, registry, register_fn):
    register_fn(some_fn, event=None, field=None, annotations={'someannotation': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_without_annotation_value)
    assert not handlers


def test_catchall_handlers_with_annotations_exist(cause_with_annotation_exists, registry, register_fn):
    register_fn(some_fn, event=None, field=None, annotations={'someannotation': None})
    handlers = registry.get_cause_handlers(cause_with_annotation_exists)
    assert handlers


def test_catchall_handlers_with_annotations_not_exist(cause_without_annotation_exists, registry, register_fn):
    register_fn(some_fn, event=None, field=None, annotations={'someannotation': None})
    handlers = registry.get_cause_handlers(cause_without_annotation_exists)
    assert not handlers


def test_catchall_handlers_without_annotations(cause_any_annotations, registry, register_fn):
    register_fn(some_fn, event=None, field=None, annotations=None)
    handlers = registry.get_cause_handlers(cause_any_annotations)
    assert handlers


def test_catchall_handlers_with_labels_and_annotations_satisfied(cause_labels_and_annotations, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_labels_and_annotations)
    assert handlers


def test_catchall_handlers_with_labels_and_annotations_not_satisfied(cause_any_labels, registry, register_fn):
    register_fn(some_fn, event=None, field=None, labels={'somelabel': 'somevalue'}, annotations={'someannotation': 'somevalue'})
    handlers = registry.get_cause_handlers(cause_any_labels)
    assert not handlers


#
# Relevant handlers are those with event == 'some-event' (but not 'another-event').
# In the per-field handlers, also with field == 'some-field' (not 'another-field').
# In the label filtered handlers, the relevant handlers are those that ask for 'somelabel'.
# In the annotation filtered handlers, the relevant handlers are those that ask for 'someannotation'.
#

def test_relevant_handlers_without_field_found(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='some-event')
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_field_found(cause_with_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', field='some-field')
    handlers = registry.get_cause_handlers(cause_with_diff)
    assert handlers


def test_relevant_handlers_with_field_ignored(cause_no_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', field='some-field')
    handlers = registry.get_cause_handlers(cause_no_diff)
    assert not handlers


def test_relevant_handlers_with_labels_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', labels={'somelabel': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_labels_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', labels={'otherlabel': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_relevant_handlers_with_annotations_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', annotations={'someannotation': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert handlers


def test_relevant_handlers_with_annotations_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='some-event', annotations={'otherannotation': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_without_field_ignored(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event')
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_field_ignored(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event', field='another-field')
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers

def test_irrelevant_handlers_with_labels_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event', labels={'somelabel': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_labels_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event', labels={'otherlabel': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_annotations_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event', annotations={'someannotation': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


def test_irrelevant_handlers_with_annotations_not_satisfied(cause_any_diff, registry, register_fn):
    register_fn(some_fn, event='another-event', annotations={'otherannotation': None})
    handlers = registry.get_cause_handlers(cause_any_diff)
    assert not handlers


#
# The handlers must be returned in order of registration,
# even if they are mixed with-/without- * -event/-field handlers.
#

def test_order_persisted_a(cause_with_diff, registry, register_fn):
    register_fn(functools.partial(some_fn, 1), event=None)
    register_fn(functools.partial(some_fn, 2), event='some-event')
    register_fn(functools.partial(some_fn, 3), event='filtered-out-event')
    register_fn(functools.partial(some_fn, 4), event=None, field='filtered-out-field')
    register_fn(functools.partial(some_fn, 5), event=None, field='some-field')

    handlers = registry.get_cause_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].event is None
    assert handlers[0].field is None
    assert handlers[1].event == 'some-event'
    assert handlers[1].field is None
    assert handlers[2].event is None
    assert handlers[2].field == ('some-field',)


def test_order_persisted_b(cause_with_diff, registry, register_fn):
    register_fn(functools.partial(some_fn, 1), event=None, field='some-field')
    register_fn(functools.partial(some_fn, 2), event=None, field='filtered-out-field')
    register_fn(functools.partial(some_fn, 3), event='filtered-out-event')
    register_fn(functools.partial(some_fn, 4), event='some-event')
    register_fn(functools.partial(some_fn, 5), event=None)

    handlers = registry.get_cause_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].event is None
    assert handlers[0].field == ('some-field',)
    assert handlers[1].event == 'some-event'
    assert handlers[1].field is None
    assert handlers[2].event is None
    assert handlers[2].field is None

#
# Same function should not be returned twice for the same event/cause.
# Only actual for the cases when the event/cause can match multiple handlers.
#

def test_deduplicated(cause_with_diff, registry, register_fn):
    register_fn(some_fn, event=None, id='a')
    register_fn(some_fn, event=None, id='b')

    handlers = registry.get_cause_handlers(cause_with_diff)

    assert len(handlers) == 1
    assert handlers[0].id == 'a'  # the first found one is returned
