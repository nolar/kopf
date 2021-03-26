import copy

import pytest

import kopf
from kopf.structs.bodies import Body
from kopf.structs.dicts import parse_field
from kopf.structs.filters import ABSENT, PRESENT
from kopf.structs.handlers import ALL_REASONS, Reason, ResourceChangingHandler


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


def _never(*_, **__):
    return False


def _always(*_, **__):
    return True


matching_reason_and_decorator = pytest.mark.parametrize('reason, decorator', [
    (Reason.CREATE, kopf.on.create),
    (Reason.UPDATE, kopf.on.update),
    (Reason.DELETE, kopf.on.delete),
])

matching_reason_and_decorator_with_field = pytest.mark.parametrize('reason, decorator', [
    (Reason.CREATE, kopf.on.create),
    (Reason.UPDATE, kopf.on.update),
    (Reason.DELETE, kopf.on.delete),
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
def handler_factory(registry, selector):
    def factory(**kwargs):
        handler = ResourceChangingHandler(**dict(dict(
            fn=some_fn, id='a', param=None,
            errors=None, timeout=None, retries=None, backoff=None,
            initial=None, deleted=None, requires_finalizer=None,
            selector=selector, annotations=None, labels=None, when=None,
            field=None, value=None, old=None, new=None, field_needs_change=None,
            reason=None,
        ), **kwargs))
        registry._resource_changing.append(handler)
        return handler
    return factory


@pytest.fixture(params=[
    # The original no-diff was equivalent to no-field until body/old/new were added to the check.
    pytest.param(dict(old={}, new={}, body={}, diff=[]), id='with-empty-diff'),
])
def cause_no_diff(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(**kwargs)
    return cause


@pytest.fixture(params=[
    pytest.param(dict(old={'known-field': 'old'},
                      new={'known-field': 'new'},
                      body={'known-field': 'new'},
                      diff=[('op', ('known-field',), 'old', 'new')]), id='with-field-diff'),
])
def cause_with_diff(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(**kwargs)
    return cause


@pytest.fixture(params=[
    # The original no-diff was equivalent to no-field until body/old/new were added to the check.
    pytest.param(dict(old={}, new={}, body={}, diff=[]), id='with-empty-diff'),
    pytest.param(dict(old={'known-field': 'old'},
                      new={'known-field': 'new'},
                      body={'known-field': 'new'},
                      diff=[('op', ('known-field',), 'old', 'new')]), id='with-field-diff'),
])
def cause_any_diff(request, cause_factory):
    kwargs = copy.deepcopy(request.param)
    kwargs['body'].update({'metadata': {'labels': {'known': 'value'},
                                        'annotations': {'known': 'value'}}})
    cause = cause_factory(**kwargs)
    return cause


#
# "Catch-all" handlers are those with event == None.
#

def test_catchall_handlers_without_field_found(
        cause_any_diff, registry, handler_factory):
    cause = cause_any_diff
    handler_factory(reason=None, field=None)
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_found(
        cause_with_diff, registry, handler_factory):
    cause = cause_with_diff
    handler_factory(reason=None, field=parse_field('known-field'))
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


def test_catchall_handlers_with_field_ignored(
        cause_no_diff, registry, handler_factory):
    cause = cause_no_diff
    handler_factory(reason=None, field=parse_field('known-field'))
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-label'),
])
def test_catchall_handlers_with_exact_labels_satisfied(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_exact_labels_not_satisfied(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_desired_labels_present(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': PRESENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_desired_labels_absent(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': PRESENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_undesired_labels_present(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': ABSENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'extra': 'other'}, id='with-other-label'),
])
def test_catchall_handlers_with_undesired_labels_absent(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': ABSENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_callback_says_true(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': _always})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('labels', [
    pytest.param({}, id='without-label'),
    pytest.param({'known': 'value'}, id='with-label'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_labels_callback_says_false(
        cause_factory, registry, handler_factory, labels):
    cause = cause_factory(body={'metadata': {'labels': labels}})
    handler_factory(reason=None, labels={'known': _never})
    handlers = registry._resource_changing.get_handlers(cause)
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
    handler_factory(reason=None, labels=None)
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'value', 'extra': 'other'}, id='with-extra-annotation'),
])
def test_catchall_handlers_with_exact_annotations_satisfied(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_exact_annotations_not_satisfied(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_desired_annotations_present(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': PRESENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_desired_annotations_absent(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': PRESENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_undesired_annotations_present(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': ABSENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'extra': 'other'}, id='with-other-annotation'),
])
def test_catchall_handlers_with_undesired_annotations_absent(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': ABSENT})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_callback_says_true(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': _always})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('annotations', [
    pytest.param({}, id='without-annotation'),
    pytest.param({'known': 'value'}, id='with-annotation'),
    pytest.param({'known': 'other'}, id='with-other-value'),
])
def test_catchall_handlers_with_annotations_callback_says_false(
        cause_factory, registry, handler_factory, annotations):
    cause = cause_factory(body={'metadata': {'annotations': annotations}})
    handler_factory(reason=None, annotations={'known': _never})
    handlers = registry._resource_changing.get_handlers(cause)
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
    handler_factory(reason=None)
    handlers = registry._resource_changing.get_handlers(cause)
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
    handler_factory(reason=None, labels={'known': 'value'}, annotations={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
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
    handler_factory(reason=None, labels={'known': 'value'}, annotations={'known': 'value'})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('reason', ALL_REASONS)
@pytest.mark.parametrize('when', [
    pytest.param(None, id='without-when'),
    pytest.param(lambda body=None, **_: body['spec']['name'] == 'test', id='with-when'),
    pytest.param(lambda **_: True, id='with-other-when'),
])
def test_catchall_handlers_with_when_callback_matching(
        cause_factory, registry, handler_factory, reason, when):
    cause = cause_factory(body={'spec': {'name': 'test'}})
    handler_factory(reason=None, when=when)
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@pytest.mark.parametrize('when', [
    pytest.param(lambda body=None, **_: body['spec']['name'] != "test", id='with-when'),
    pytest.param(lambda **_: False, id='with-other-when'),
])
def test_catchall_handlers_with_when_callback_mismatching(
        cause_factory, registry, handler_factory, when):
    cause = cause_factory(body={'spec': {'name': 'test'}})
    handler_factory(reason=None, when=when)
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


#
# Relevant handlers are those with reason matching the cause's reason.
# In the per-field handlers, also with field == 'known-field' (not 'extra-field').
# In the label filtered handlers, the relevant handlers are those that ask for 'known'.
# In the annotation filtered handlers, the relevant handlers are those that ask for 'known'.
#


@matching_reason_and_decorator_with_field
def test_relevant_handlers_without_field_found(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, field=None)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_relevant_handlers_with_field_found(
        cause_with_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='known-field')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_relevant_handlers_with_field_ignored(
        cause_no_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='known-field')
    def some_fn(**_): ...

    cause = cause_no_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_labels_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, labels={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_labels_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, labels={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_annotations_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, annotations={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_annotations_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, annotations={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_filter_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, when=_always)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator
def test_relevant_handlers_with_filter_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, when=_never)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_without_field_ignored(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@matching_reason_and_decorator_with_field
def test_irrelevant_handlers_with_field_ignored(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='extra-field')
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_labels_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, labels={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_labels_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, labels={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_annotations_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, annotations={'known': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_annotations_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, annotations={'extra': PRESENT})
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_when_callback_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, when=_always)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@mismatching_reason_and_decorator
def test_irrelevant_handlers_with_when_callback_not_satisfied(
        cause_any_diff, registry, resource, reason, decorator):

    @decorator(*resource, when=_never)
    def some_fn(**_): ...

    cause = cause_any_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


#
# Special case for nested fields with shorter/longer diffs.
#

@matching_reason_and_decorator_with_field
def test_field_same_as_diff(cause_with_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='level1.level2')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    cause.old = {'level1': {'level2': 'old'}}
    cause.new = {'level1': {'level2': 'new'}}
    cause.body = Body({'level1': {'level2': 'new'}})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_field_shorter_than_diff(cause_with_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='level1')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    cause.old = {'level1': {'level2': 'old'}}
    cause.new = {'level1': {'level2': 'new'}}
    cause.body = Body({'level1': {'level2': 'new'}})
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


@matching_reason_and_decorator_with_field
def test_field_longer_than_diff_for_wrong_field(cause_with_diff, registry, resource, reason, decorator):

    @decorator(*resource, field='level1.level2.level3')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    cause.old = {'level1': {'level2': 'old'}}
    cause.new = {'level1': {'level2': 'new'}}
    cause.body = Body({'level1': {'level2': 'new'}})
    handlers = registry._resource_changing.get_handlers(cause)
    assert not handlers


@pytest.mark.parametrize('old, new', [
    pytest.param({'level3': 'old'}, {'level3': 'new'}, id='struct2struct'),
    pytest.param({'level3': 'old'}, 'new', id='struct2scalar'),
    pytest.param('old', {'level3': 'new'}, id='scalar2struct'),
    pytest.param(None, {'level3': 'new'}, id='none2struct'),
    pytest.param({'level3': 'old'}, None, id='struct2none'),
    pytest.param({}, {'level3': 'new'}, id='empty2struct'),
    pytest.param({'level3': 'old'}, {}, id='struct2empty'),
])
@matching_reason_and_decorator_with_field
def test_field_longer_than_diff_for_right_field(cause_with_diff, registry, resource, old, new, reason, decorator):

    @decorator(*resource, field='level1.level2.level3')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    cause.old = {'level1': {'level2': old}} if old is not None else {'level1': {'level2': {}}}
    cause.new = {'level1': {'level2': new}} if new is not None else {'level1': {'level2': {}}}
    cause.body = Body(cause.new)
    handlers = registry._resource_changing.get_handlers(cause)
    assert handlers


#
# The handlers must be returned in order of registration,
# even if they are mixed with-/without- * -event/-field handlers.
#

def test_order_persisted_a(cause_with_diff, registry, resource):

    @kopf.on.create(*resource)
    def some_fn_1(**_): ...  # used

    @kopf.on.update(*resource)
    def some_fn_2(**_): ...  # filtered out

    @kopf.on.create(*resource)
    def some_fn_3(**_): ...  # used

    @kopf.on.field(*resource, field='filtered-out-field')
    def some_fn_4(**_): ...  # filtered out

    @kopf.on.field(*resource, field='known-field')
    def some_fn_5(**_): ...  # used

    cause = cause_with_diff
    cause.reason = Reason.CREATE
    handlers = registry._resource_changing.get_handlers(cause)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].fn is some_fn_1
    assert handlers[1].fn is some_fn_3
    assert handlers[2].fn is some_fn_5


def test_order_persisted_b(cause_with_diff, registry, resource):

    @kopf.on.field(*resource, field='known-field')
    def some_fn_1(**_): ...  # used

    @kopf.on.field(*resource, field='filtered-out-field')
    def some_fn_2(**_): ...  # filtered out

    @kopf.on.create(*resource)
    def some_fn_3(**_): ...  # used

    @kopf.on.update(*resource)
    def some_fn_4(**_): ...  # filtered out

    @kopf.on.create(*resource)
    def some_fn_5(**_): ...  # used

    cause = cause_with_diff
    cause.reason = Reason.CREATE
    handlers = registry._resource_changing.get_handlers(cause)

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
def test_deduplication_by_fn_and_id(
        cause_with_diff, registry, resource, reason, decorator):

    # Note: the decorators are applied bottom-up -- hence, the order of ids:
    @decorator(*resource, id='a')
    @decorator(*resource, id='a')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)

    assert len(handlers) == 1
    assert handlers[0].id == 'a'  # the first found one is returned


@matching_reason_and_decorator
def test_deduplication_distinguishes_different_fns(
        cause_with_diff, registry, resource, reason, decorator):

    # Note: the decorators are applied bottom-up -- hence, the order of ids:
    @decorator(*resource, id='a')
    def some_fn1(**_): ...

    @decorator(*resource, id='a')
    def some_fn2(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)

    assert len(handlers) == 2


@matching_reason_and_decorator
def test_deduplication_distinguishes_different_ids(
        cause_with_diff, registry, resource, reason, decorator):

    # Note: the decorators are applied bottom-up -- hence, the order of ids:
    @decorator(*resource, id='b')
    @decorator(*resource, id='a')
    def some_fn(**_): ...

    cause = cause_with_diff
    cause.reason = reason
    handlers = registry._resource_changing.get_handlers(cause)

    assert len(handlers) == 2
