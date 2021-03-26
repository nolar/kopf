"""
Testing the handling of events on the top level.

As input:

* Mocked cause detection, with the cause artificially simulated for each test.
  The proper cause detection is tested elsewhere (see ``test_detection.py``).
* Registered handlers in a global registry. Each handler is a normal function,
  which calls a mock -- to ease the assertions.

As output, we check mocked calls on the following:

* ``asyncio.sleep()`` -- for delays.
* ``kopf.clients.patching.patch_obj()`` -- for patch content.
* ``kopf.clients.events.post_event()`` -- for events posted.
* Handler mocks -- whether they were or were not called with specific arguments.
* Captured logs.

The above inputs & outputs represent the expected user scenario
rather than the specific implementation of it.
Therefore, we do not mock/spy/intercept anything within the handling routines
(except for cause detection), leaving it as the implementation details.
Specifically, this internal chain of calls happens on every event:

* ``causation.detect_*_cause()`` -- tested separately in ``/tests/causation/``.
* ``handle_cause()``
* ``execute()``
* ``_execute()``
* ``_call_handler()``
* ``invocation.invoke()`` -- tested separately in ``/tests/invocations/``.

Some of these aspects are tested separately to be sure they indeed execute
all possible cases properly. In the top-level event handling, we assume they do,
and only check for the upper-level behaviour, not all of the input combinations.
"""
import dataclasses
from typing import Callable
from unittest.mock import Mock

import pytest

import kopf
from kopf.reactor.causation import ResourceChangingCause


@dataclasses.dataclass(frozen=True, eq=False)
class K8sMocks:
    patch_obj: Mock
    post_event: Mock
    sleep_or_wait: Mock


@pytest.fixture(autouse=True)
def k8s_mocked(mocker, resp_mocker):
    # We mock on the level of our own K8s API wrappers, not the K8s client.
    return K8sMocks(
        patch_obj=mocker.patch('kopf.clients.patching.patch_obj', return_value={}),
        post_event=mocker.patch('kopf.clients.events.post_event'),
        sleep_or_wait=mocker.patch('kopf.structs.primitives.sleep_or_wait', return_value=None),
    )


@dataclasses.dataclass(frozen=True, eq=False, order=False)
class HandlersContainer:
    index_mock: Mock
    event_mock: Mock
    create_mock: Mock
    update_mock: Mock
    delete_mock: Mock
    resume_mock: Mock
    event_fn: Callable
    create_fn: Callable
    update_fn: Callable
    delete_fn: Callable
    resume_fn: Callable


@pytest.fixture()
def handlers(registry):
    index_mock = Mock(return_value=None)
    event_mock = Mock(return_value=None)
    create_mock = Mock(return_value=None)
    update_mock = Mock(return_value=None)
    delete_mock = Mock(return_value=None)
    resume_mock = Mock(return_value=None)

    @kopf.index('kopfexamples', id='index_fn')
    async def index_fn(**kwargs):
        return index_mock(**kwargs)

    @kopf.on.event('kopfexamples', id='event_fn')
    async def event_fn(**kwargs):
        return event_mock(**kwargs)

    # Keep on-resume on top, to catch any issues with the test design (where it could be skipped).
    @kopf.on.resume('kopfexamples', id='resume_fn', timeout=600, retries=100,
                    deleted=True)  # only for resuming handles, to cover the resource being deleted.
    async def resume_fn(**kwargs):
        return resume_mock(**kwargs)

    @kopf.on.create('kopfexamples', id='create_fn', timeout=600, retries=100)
    async def create_fn(**kwargs):
        return create_mock(**kwargs)

    @kopf.on.update('kopfexamples', id='update_fn', timeout=600, retries=100)
    async def update_fn(**kwargs):
        return update_mock(**kwargs)

    @kopf.on.delete('kopfexamples', id='delete_fn', timeout=600, retries=100)
    async def delete_fn(**kwargs):
        return delete_mock(**kwargs)

    return HandlersContainer(
        index_mock=index_mock,
        event_mock=event_mock,
        create_mock=create_mock,
        update_mock=update_mock,
        delete_mock=delete_mock,
        resume_mock=resume_mock,
        event_fn=event_fn,
        create_fn=create_fn,
        update_fn=update_fn,
        delete_fn=delete_fn,
        resume_fn=resume_fn,
    )


@pytest.fixture()
def extrahandlers(registry, handlers):
    index_mock = Mock(return_value=None)
    event_mock = Mock(return_value=None)
    create_mock = Mock(return_value=None)
    update_mock = Mock(return_value=None)
    delete_mock = Mock(return_value=None)
    resume_mock = Mock(return_value=None)

    @kopf.index('kopfexamples', id='index_fn2')
    async def index_fn2(**kwargs):
        return index_mock(**kwargs)

    @kopf.on.event('kopfexamples', id='event_fn2')
    async def event_fn2(**kwargs):
        return event_mock(**kwargs)

    # Keep on-resume on top, to catch any issues with the test design (where it could be skipped).
    # Note: deleted=True -- only for resuming handles, to cover the resource being deleted.
    @kopf.on.resume('kopfexamples', id='resume_fn2', deleted=True)
    async def resume_fn2(**kwargs):
        return resume_mock(**kwargs)

    @kopf.on.create('kopfexamples', id='create_fn2')
    async def create_fn2(**kwargs):
        return create_mock(**kwargs)

    @kopf.on.update('kopfexamples', id='update_fn2')
    async def update_fn2(**kwargs):
        return update_mock(**kwargs)

    @kopf.on.delete('kopfexamples', id='delete_fn2')
    async def delete_fn2(**kwargs):
        return delete_mock(**kwargs)

    return HandlersContainer(
        index_mock=index_mock,
        event_mock=event_mock,
        create_mock=create_mock,
        update_mock=update_mock,
        delete_mock=delete_mock,
        resume_mock=resume_mock,
        event_fn=event_fn2,
        create_fn=create_fn2,
        update_fn=update_fn2,
        delete_fn=delete_fn2,
        resume_fn=resume_fn2,
    )


@pytest.fixture()
def cause_mock(mocker, settings):
    """
    Mock the resulting _cause_ of the resource change detection logic.

    The change detection is complex, depends on many fields and values, and it
    is difficult to simulate by artificial event bodies, especially its reason.

    Instead, we patch a method which detects the resource changing causes, and
    return a cause with the mocked reason (also, diff, and some other fields).

    The a value of this fixture, a mock is provided with few fields to mock.
    The default is to no mock anything, unless defined in the test, and to use
    the original arguments to the detection method.
    """

    # Use everything from a mock, but use the passed `patch` dict as is.
    # The event handler passes its own accumulator, and checks/applies it later.
    def new_detect_fn(*, finalizer, diff, new, old, **kwargs):

        # For change detection, we ensure that there is no extra cycle of adding a finalizer.
        raw_event = kwargs.pop('raw_event', None)
        raw_body = raw_event['object']
        raw_body.setdefault('metadata', {}).setdefault('finalizers', [finalizer])

        # Pass through kwargs: resource, logger, patch, diff, old, new.
        # I.e. everything except what we mock -- for them, use the mocked values (if not None).
        return ResourceChangingCause(
            reason=mock.reason,
            diff=mock.diff if mock.diff is not None else diff,
            new=mock.new if mock.new is not None else new,
            old=mock.old if mock.old is not None else old,
            **kwargs)

    # Substitute the real cause detector with out own mock-based one.
    mocker.patch('kopf.reactor.causation.detect_resource_changing_cause', new=new_detect_fn)

    # The mock object stores some values later used by the factory substitute.
    # Note: ONLY those fields we mock in the tests. Other kwargs should be passed through.
    mock = mocker.Mock(spec_set=['reason', 'diff', 'new', 'old'])
    mock.reason = None
    mock.diff = None
    mock.new = None
    mock.old = None
    return mock
