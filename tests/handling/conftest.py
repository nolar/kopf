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
import copy
import dataclasses
from typing import Callable
from unittest.mock import Mock

import pytest

import kopf
from kopf.reactor.causation import ResourceChangingCause, Reason


@dataclasses.dataclass(frozen=True, eq=False)
class K8sMocks:
    patch_obj: Mock
    post_event: Mock
    asyncio_sleep: Mock
    sleep_or_wait: Mock


@pytest.fixture(autouse=True)
def k8s_mocked(mocker, resp_mocker):
    # We mock on the level of our own K8s API wrappers, not the K8s client.
    return K8sMocks(
        patch_obj=mocker.patch('kopf.clients.patching.patch_obj'),
        post_event=mocker.patch('kopf.clients.events.post_event'),
        asyncio_sleep=mocker.patch('asyncio.sleep'),
        sleep_or_wait=mocker.patch('kopf.engines.sleeping.sleep_or_wait', return_value=None),
    )


@pytest.fixture()
def registry(clear_default_registry):
    return clear_default_registry


@dataclasses.dataclass(frozen=True, eq=False, order=False)
class HandlersContainer:
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
def handlers(clear_default_registry):
    event_mock = Mock(return_value=None)
    create_mock = Mock(return_value=None)
    update_mock = Mock(return_value=None)
    delete_mock = Mock(return_value=None)
    resume_mock = Mock(return_value=None)

    @kopf.on.event('zalando.org', 'v1', 'kopfexamples', id='event_fn')
    async def event_fn(**kwargs):
        return event_mock(**kwargs)

    # Keep on-resume on top, to catch any issues with the test design (where it could be skipped).
    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples', id='resume_fn', timeout=600, retries=100)
    async def resume_fn(**kwargs):
        return resume_mock(**kwargs)

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', id='create_fn', timeout=600, retries=100)
    async def create_fn(**kwargs):
        return create_mock(**kwargs)

    @kopf.on.update('zalando.org', 'v1', 'kopfexamples', id='update_fn', timeout=600, retries=100)
    async def update_fn(**kwargs):
        return update_mock(**kwargs)

    @kopf.on.delete('zalando.org', 'v1', 'kopfexamples', id='delete_fn', timeout=600, retries=100)
    async def delete_fn(**kwargs):
        return delete_mock(**kwargs)

    return HandlersContainer(
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
def extrahandlers(clear_default_registry, handlers):
    event_mock = Mock(return_value=None)
    create_mock = Mock(return_value=None)
    update_mock = Mock(return_value=None)
    delete_mock = Mock(return_value=None)
    resume_mock = Mock(return_value=None)

    @kopf.on.event('zalando.org', 'v1', 'kopfexamples', id='event_fn2')
    async def event_fn2(**kwargs):
        return event_mock(**kwargs)

    # Keep on-resume on top, to catch any issues with the test design (where it could be skipped).
    @kopf.on.resume('zalando.org', 'v1', 'kopfexamples', id='resume_fn2')
    async def resume_fn2(**kwargs):
        return resume_mock(**kwargs)

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples', id='create_fn2')
    async def create_fn2(**kwargs):
        return create_mock(**kwargs)

    @kopf.on.update('zalando.org', 'v1', 'kopfexamples', id='update_fn2')
    async def update_fn2(**kwargs):
        return update_mock(**kwargs)

    @kopf.on.delete('zalando.org', 'v1', 'kopfexamples', id='delete_fn2')
    async def delete_fn2(**kwargs):
        return delete_mock(**kwargs)

    return HandlersContainer(
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
def cause_mock(mocker, resource):

    # Use everything from a mock, but use the passed `patch` dict as is.
    # The event handler passes its own accumulator, and checks/applies it later.
    def new_detect_fn(**kwargs):

        # Avoid collision of our mocked values with the passed kwargs.
        original_event = kwargs.pop('event', None)
        original_reason = kwargs.pop('reason', None)
        original_memo = kwargs.pop('memo', None)
        original_body = kwargs.pop('body', None)
        original_diff = kwargs.pop('diff', None)
        original_new = kwargs.pop('new', None)
        original_old = kwargs.pop('old', None)
        reason = mock.reason if mock.reason is not None else original_reason
        memo = copy.deepcopy(mock.memo) if mock.memo is not None else original_memo
        body = copy.deepcopy(mock.body) if mock.body is not None else original_body
        diff = copy.deepcopy(mock.diff) if mock.diff is not None else original_diff
        new = copy.deepcopy(mock.new) if mock.new is not None else original_new
        old = copy.deepcopy(mock.old) if mock.old is not None else original_old

        # Remove requires_finalizer from kwargs as it shouldn't be passed to the cause.
        kwargs.pop('requires_finalizer', None)

        # Pass through kwargs: resource, logger, patch, diff, old, new.
        # I.e. everything except what we mock: reason & body.
        cause = ResourceChangingCause(
            reason=reason,
            memo=memo,
            body=body,
            diff=diff,
            new=new,
            old=old,
            **kwargs)

        # Needed for the k8s-event creation, as they are attached to objects.
        body.setdefault('apiVersion', f'{resource.group}/{resource.version}')
        body.setdefault('kind', 'KopfExample')  # TODO: resource.???
        body.setdefault('metadata', {}).setdefault('namespace', 'some-namespace')
        body.setdefault('metadata', {}).setdefault('name', 'some-name')
        body.setdefault('metadata', {}).setdefault('uid', 'some-uid')

        return cause

    # Substitute the real cause detector with out own mock-based one.
    mocker.patch('kopf.reactor.causation.detect_resource_changing_cause', new=new_detect_fn)

    # The mock object stores some values later used by the factory substitute.
    mock = mocker.Mock(spec_set=['reason', 'memo', 'body', 'diff', 'new', 'old'])
    mock.reason = None
    mock.memo = None
    mock.body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    mock.diff = None
    mock.new = None
    mock.old = None
    return mock
