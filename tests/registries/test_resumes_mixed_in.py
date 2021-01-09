import pytest

import kopf
from kopf.structs.handlers import HANDLER_REASONS, Reason


@pytest.mark.parametrize('deleted', [True, False, None])
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_resumes_ignored_for_non_initial_causes(
        reason, deleted, cause_factory, resource):

    registry = kopf.get_default_registry()
    cause = cause_factory(resource=resource, reason=reason, initial=False,
                          body={'metadata': {'deletionTimestamp': '...'} if deleted else {}})

    @kopf.on.resume(*resource)
    def fn(**_):
        pass

    handlers = registry._resource_changing.get_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', list(set(HANDLER_REASONS) - {Reason.DELETE}))
def test_resumes_selected_for_initial_non_deletions(
        reason, cause_factory, resource):

    registry = kopf.get_default_registry()
    cause = cause_factory(resource=resource, reason=reason, initial=True)

    @kopf.on.resume(*resource)
    def fn(**_):
        pass

    handlers = registry._resource_changing.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_ignored_for_initial_deletions_by_default(
        reason, cause_factory, resource):

    registry = kopf.get_default_registry()
    cause = cause_factory(resource=resource, reason=reason, initial=True,
                          body={'metadata': {'deletionTimestamp': '...'}})

    @kopf.on.resume(*resource)
    def fn(**_):
        pass

    handlers = registry._resource_changing.get_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_selected_for_initial_deletions_when_explicitly_marked(
        reason, cause_factory, resource):

    registry = kopf.get_default_registry()
    cause = cause_factory(resource=resource, reason=reason, initial=True,
                          body={'metadata': {'deletionTimestamp': '...'}})

    @kopf.on.resume(*resource, deleted=True)
    def fn(**_):
        pass

    handlers = registry._resource_changing.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
