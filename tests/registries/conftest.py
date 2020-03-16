import pytest

from kopf import ActivityRegistry
from kopf import OperatorRegistry
from kopf import ResourceWatchingRegistry, ResourceChangingRegistry
from kopf import SimpleRegistry, GlobalRegistry  # deprecated, but tested
from kopf.structs.handlers import HandlerId, ResourceChangingHandler


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
        reason=None, field=None,
    )
