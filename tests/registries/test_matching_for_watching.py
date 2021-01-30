import copy

import pytest

import kopf
from kopf.reactor.causation import ResourceWatchingCause
from kopf.structs.dicts import parse_field
from kopf.structs.filters import ABSENT, PRESENT
from kopf.structs.handlers import ResourceWatchingHandler


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


def _never(*_, **__):
    return False


def _always(*_, **__):
    return True


@pytest.fixture()
def handler_factory(registry, selector):
    def factory(**kwargs):
        handler = ResourceWatchingHandler(**dict(dict(
            fn=some_fn, id='a', param=None,
            errors=None, timeout=None, retries=None, backoff=None,
            selector=selector, annotations=None, labels=None, when=None,
            field=None, value=None,
        ), **kwargs))
        registry._resource_watching.append(handler)
        return handler
    return factory


@pytest.fixture(params=[
    pytest.param(dict(body={}), id='no-field'),
])
def cause_no_field(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(cls=ResourceWatchingCause, **kwargs)
    return cause


@pytest.fixture(params=[
    pytest.param(dict(body={'known-field': 'new'}), id='with-field'),
])
def cause_with_field(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(cls=ResourceWatchingCause, **kwargs)
    return cause


@pytest.fixture(params=[
    # The original no-diff was equivalent to no-field until body/old/new were added to the check.
    pytest.param(dict(body={}, diff=[]), id='no-field'),
    pytest.param(dict(body={'known-field': 'new'}), id='with-field'),
])
def cause_any_field(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(cls=ResourceWatchingCause, **kwargs)
    return cause


#
# "Catch-all" handlers are those with event == None.
#

def test_catchall_handlers_without_field_found(
        cause_any_field, registry, handler_factory):
    cause = cause_any_field
    handler_factory(field=None)
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_found(
        cause_with_field, registry, handler_factory):
    cause = cause_with_field
    handler_factory(field=parse_field('known-field'))
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_ignored(
        cause_no_field, registry, handler_factory):
    cause = cause_no_field
    handler_factory(field=parse_field('known-field'))
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-label'),
])
def test_catchall_handlers_with_exact_labels_satisfied(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_exact_labels_not_satisfied(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_desired_labels_present(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': PRESENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_desired_labels_absent(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': PRESENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_undesired_labels_present(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': ABSENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_undesired_labels_absent(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': ABSENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_callback_says_true(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': _always})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_callback_says_false(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': _never})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-label'),
])
def test_catchall_handlers_without_labels(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels=None)
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-annotation'),
])
def test_catchall_handlers_with_exact_annotations_satisfied(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_exact_annotations_not_satisfied(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_desired_annotations_present(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': PRESENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_desired_annotations_absent(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': PRESENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_undesired_annotations_present(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': ABSENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_undesired_annotations_absent(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': ABSENT})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_callback_says_true(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': _always})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_callback_says_false(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(annotations={'known': _never})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-annotation'),
])
def test_catchall_handlers_without_annotations(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory()
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels, annotations', [
    pytest.param({'known': 'value'}, {'known': 'value'}, id='with-label-annotation'),
    pytest.param({'known': 'value', 'extra': 'other'}, {'known': 'value'}, id='with-extra-label-annotation'),
    pytest.param({'known': 'value'}, {'known': 'value', 'extra': 'other'}, id='with-label-extra-annotation'),
    pytest.param({'known': 'value', 'extra': 'other'}, {'known': 'value', 'extra': 'other'}, id='with-extra-label-extra-annotation'),
])
def test_catchall_handlers_with_labels_and_annotations_satisfied(
        cause_factory, registry, handler_factory, labels, annotations):
    cause = cause_factory(body={'metadata': {'labels': labels, 'annotations': annotations}})
    handler_factory(labels={'known': 'value'}, annotations={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-label'),
])
def test_catchall_handlers_with_labels_and_annotations_not_satisfied(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(labels={'known': 'value'}, annotations={'known': 'value'})
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('when', [
    pytest.param(None, id='without-when'),
    pytest.param(lambda body=None, **_: body['spec']['name'] == 'test', id='with-when'),
    pytest.param(lambda **_: True, id='with-other-when'),
])
def test_catchall_handlers_with_when_callback_matching(
        cause_factory, registry, handler_factory, when):
    cause = cause_factory(body={'spec': {'name': 'test'}})
    handler_factory(when=when)
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('when', [
    pytest.param(lambda body=None, **_: body['spec']['name'] != "test", id='with-when'),
    pytest.param(lambda **_: False, id='with-other-when'),
])
def test_catchall_handlers_with_when_callback_mismatching(
        cause_factory, registry, handler_factory, when):
    cause = cause_factory(body={'spec': {'name': 'test'}})
    handler_factory(when=when)
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


def test_decorator_without_field_found(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, field=None)
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_decorator_with_field_found(
        cause_with_field, registry, resource):

    @kopf.on.event(*resource, field='known-field')
    def some_fn(**_): ...

    cause = cause_with_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_decorator_with_field_ignored(
        cause_no_field, registry, resource):

    @kopf.on.event(*resource, field='known-field')
    def some_fn(**_): ...

    cause = cause_no_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


def test_decorator_with_labels_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, labels={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_decorator_with_labels_not_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, labels={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


def test_decorator_with_annotations_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, annotations={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_decorator_with_annotations_not_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, annotations={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers


def test_decorator_with_filter_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, when=_always)
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert handlers


def test_decorator_with_filter_not_satisfied(
        cause_any_field, registry, resource):

    @kopf.on.event(*resource, when=_never)
    def some_fn(**_): ...

    cause = cause_any_field
    handlers = registry._resource_watching.get_handlers(cause)
    assert not handlers
