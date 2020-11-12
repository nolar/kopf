import logging

import pytest

from kopf import GlobalRegistry, SimpleRegistry  # deprecated, but tested
from kopf.reactor.causation import ActivityCause, ResourceCause, \
                                   ResourceChangingCause, ResourceWatchingCause
from kopf.reactor.registries import ActivityRegistry, OperatorRegistry, ResourceChangingRegistry, \
                                    ResourceRegistry, ResourceWatchingRegistry
from kopf.structs.bodies import Body
from kopf.structs.containers import Memo
from kopf.structs.diffs import Diff, DiffItem
from kopf.structs.handlers import HandlerId, ResourceChangingHandler
from kopf.structs.patches import Patch


@pytest.fixture(params=[
    pytest.param(ActivityRegistry, id='activity-registry'),
    pytest.param(ResourceWatchingRegistry, id='resource-watching-registry'),
    pytest.param(ResourceChangingRegistry, id='resource-changing-registry'),
    pytest.param(SimpleRegistry, id='simple-registry'),  # deprecated
])
def generic_registry_cls(request):
    return request.param


@pytest.fixture(params=[
    pytest.param(ActivityRegistry, id='activity-registry'),
])
def activity_registry_cls(request):
    return request.param


@pytest.fixture(params=[
    pytest.param(ResourceWatchingRegistry, id='resource-watching-registry'),
    pytest.param(ResourceChangingRegistry, id='resource-changing-registry'),
    pytest.param(SimpleRegistry, id='simple-registry'),  # deprecated
])
def resource_registry_cls(request):
    return request.param


@pytest.fixture(params=[
    pytest.param(OperatorRegistry, id='operator-registry'),
    pytest.param(GlobalRegistry, id='global-registry'),  # deprecated
])
def operator_registry_cls(request):
    return request.param


@pytest.fixture()
def parent_handler():

    def parent_fn(**_):
        pass

    return ResourceChangingHandler(
        fn=parent_fn, id=HandlerId('parent_fn'),
        errors=None, retries=None, timeout=None, backoff=None, cooldown=None,
        labels=None, annotations=None, when=None,
        initial=None, deleted=None, requires_finalizer=None,
        reason=None, field=None, status_prefix=None,
    )


@pytest.fixture()
def cause_factory(resource):
    """
    A simplified factory of causes.

    It assumes most of the parameters to be unused defaults, which is sufficient
    for testing. Some parameters are of improper types (e.g. Nones), others are
    converted from built-in types to proper types, the rest are passed as is.

    The cause class is selected based on the passed ``cls``, which is either
    directly the cause class to use, or the registry class. For the latter
    case, the best matching cause class is used (hard-coded mapping). Classes
    are used here as equivalents of enums, in order not to create actual enums.

    All is done for simplicity of testing. This factory is not supposed to be
    used outside of Kopf's own tests, is not packaged, is not delivered, and
    is not available to the users.
    """
    def make_cause(
            cls=ResourceChangingCause,
            *,
            resource=resource,
            type=None,
            raw=None,
            body=None,
            diff=(),
            reason='some-reason',
            initial=False,
            activity=None,
            settings=None,
    ):
        if cls is ActivityCause or cls is ActivityRegistry:
            return ActivityCause(
                logger=logging.getLogger('kopf.test.fake.logger'),
                activity=activity,
                settings=settings,
            )
        if cls is ResourceCause or cls is ResourceRegistry or cls is SimpleRegistry:
            return ResourceCause(
                logger=logging.getLogger('kopf.test.fake.logger'),
                resource=resource,
                patch=Patch(),
                memo=Memo(),
                body=Body(body if body is not None else {}),
            )
        if cls is ResourceWatchingCause or cls is ResourceWatchingRegistry:
            return ResourceWatchingCause(
                logger=logging.getLogger('kopf.test.fake.logger'),
                resource=resource,
                patch=Patch(),
                memo=Memo(),
                body=Body(body if body is not None else {}),
                type=type,
                raw=raw,
            )
        if cls is ResourceChangingCause or cls is ResourceChangingRegistry:
            return ResourceChangingCause(
                logger=logging.getLogger('kopf.test.fake.logger'),
                resource=resource,
                patch=Patch(),
                memo=Memo(),
                body=Body(body if body is not None else {}),
                diff=Diff(DiffItem(*d) for d in diff),
                initial=initial,
                reason=reason,
            )
        raise TypeError(f"Cause/registry type {cls} is not supported by this fixture.")
    return make_cause
