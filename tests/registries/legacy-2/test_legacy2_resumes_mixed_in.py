import pytest

import kopf
from kopf.structs.handlers import Reason, HANDLER_REASONS
from kopf.structs.resources import Resource


@pytest.mark.parametrize('deleted', [True, False, None])
@pytest.mark.parametrize('reason', HANDLER_REASONS)
def test_resumes_ignored_for_non_initial_causes(mocker, reason, deleted):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=False, deleted=deleted)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', list(set(HANDLER_REASONS) - {Reason.DELETE}))
def test_resumes_selected_for_initial_non_deletions(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=False)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_ignored_for_initial_deletions_by_default(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=True)

    @kopf.on.resume('group', 'version', 'plural')
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 0


@pytest.mark.parametrize('reason', [Reason.DELETE])
def test_resumes_selected_for_initial_deletions_when_explicitly_marked(mocker, reason):
    registry = kopf.get_default_registry()
    resource = Resource('group', 'version', 'plural')
    cause = mocker.MagicMock(resource=resource, reason=reason, initial=True, deleted=True)

    @kopf.on.resume('group', 'version', 'plural', deleted=True)
    def fn(**_):
        pass

    with pytest.deprecated_call(match=r"use registry.resource_changing_handlers"):
        handlers = registry.get_resource_changing_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is fn
