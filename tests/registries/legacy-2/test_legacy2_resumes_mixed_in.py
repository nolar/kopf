import pytest

import kopf
from kopf.structs.handlers import HANDLER_REASONS, Reason
from kopf.structs.references import Resource


@pytest.mark.parametrize('deleted', [True, False])
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_resumes_ignored_for_non_initial_causes(reason, deleted, cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=False,
                          body={'metadata': {'deletionTimestamp': '...'} if deleted else {}})

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"cease using the internal registries"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', list(set(HANDLER_REASONS) - {Reason.DELETE}))
def test_resumes_selected_for_initial_non_deletions(reason, cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=True)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"cease using the internal registries"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_ignored_for_initial_deletions_by_default(reason, cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=True,
                          body={'metadata': {'deletionTimestamp': '...'}})

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"cease using the internal registries"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_selected_for_initial_deletions_when_explicitly_marked(reason, cause_factory):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = cause_factory(resource=resource, reason=reason, initial=True,
                          body={'metadata': {'deletionTimestamp': '...'}})

    @kopf.on.resume('group', 'version', 'plural', deleted=True)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"cease using the internal registries"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
