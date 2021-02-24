import functools
import logging
import traceback

import pytest
from asynctest import MagicMock

from kopf.reactor.causation import ResourceChangingCause
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.invocation import invoke, is_async_fn
from kopf.structs.bodies import Body
from kopf.structs.handlers import Reason
from kopf.structs.patches import Patch

STACK_TRACE_MARKER = object()


def _find_marker():
    marker_repr = repr(STACK_TRACE_MARKER)
    stack = traceback.StackSummary.extract(traceback.walk_stack(None), capture_locals=True)
    for frame in stack:
        if 'stack_trace_marker' in frame.locals:
            if frame.locals['stack_trace_marker'] == marker_repr:
                return True
    return False


def sync_fn(*args, **kwargs):
    return _find_marker()


async def async_fn(*args, **kwargs):
    return _find_marker()


def partials(fn, n):
    partial = fn
    for _ in range(n):
        partial = functools.partial(partial)
    return partial


def wrappers(fn, n):
    wrapper = fn
    for _ in range(n):
        @functools.wraps(wrapper)
        def wrapper(*args, wrapper=wrapper, **kwargs):
            return wrapper(*args, **kwargs)
    return wrapper


def awaiters(fn, n):
    wrapper = fn
    for _ in range(n):
        @functools.wraps(wrapper)
        async def wrapper(*args, wrapper=wrapper, **kwargs):
            return await wrapper(*args, **kwargs)
    return wrapper


def partials_wrappers(fn, n):
    wrapper = fn
    for _ in range(n):
        wrapper = functools.partial(wrapper)
        @functools.wraps(wrapper)
        def wrapper(*args, wrapper=wrapper, **kwargs):
            return wrapper(*args, **kwargs)
    return wrapper


def partials_awaiters(fn, n):
    wrapper = fn
    for _ in range(n):
        wrapper = functools.partial(wrapper)
        @functools.wraps(wrapper)
        async def wrapper(*args, wrapper=wrapper, **kwargs):
            return await wrapper(*args, **kwargs)
    return wrapper


fns = pytest.mark.parametrize(
    'fn', [
        (sync_fn),
        (async_fn),
    ])

# Every combination of partials, sync & async wrappers possible.
syncasyncparams = pytest.mark.parametrize(
    'fn, expected', [
        (sync_fn, False),
        (async_fn, True),
        (partials(sync_fn, 1), False),
        (partials(async_fn, 1), True),
        (partials(sync_fn, 9), False),
        (partials(async_fn, 9), True),
        (wrappers(sync_fn, 1), False),
        (wrappers(async_fn, 1), True),
        (wrappers(sync_fn, 9), False),
        (wrappers(async_fn, 9), True),
        (awaiters(async_fn, 1), True),
        (awaiters(async_fn, 9), True),
        (partials_wrappers(sync_fn, 9), False),
        (partials_wrappers(async_fn, 9), True),
        (partials_awaiters(async_fn, 9), True),
    ], ids=[
        'sync-direct',
        'async-direct',
        'sync-partial-once',
        'async-partial-once',
        'sync-partial-many',
        'async-partial-many',
        'sync-wrapper-once',
        'async-wrapper-once',
        'sync-wrapper-many',
        'async-wrapper-many',
        'async-awaiter-once',
        'async-awaiter-many',
        'sync-mixed-partials-wrappers',
        'async-mixed-partials-wrappers',
        'async-mixed-partials-awaiters',
    ])


async def test_detection_for_none():
    is_async = is_async_fn(None)
    assert not is_async


@syncasyncparams
async def test_async_detection(fn, expected):
    is_async = is_async_fn(fn)
    assert is_async is expected


@syncasyncparams
async def test_stacktrace_visibility(fn, expected):
    stack_trace_marker = STACK_TRACE_MARKER  # searched by fn
    found = await invoke(fn)
    assert found is expected


@fns
async def test_result_returned(fn):
    fn = MagicMock(fn, return_value=999)
    result = await invoke(fn)
    assert result == 999


@fns
async def test_explicit_args_passed_properly(fn):
    fn = MagicMock(fn)
    await invoke(fn, 100, 200, kw1=300, kw2=400)

    assert fn.called
    assert fn.call_count == 1

    assert len(fn.call_args[0]) == 2
    assert fn.call_args[0][0] == 100
    assert fn.call_args[0][1] == 200

    assert len(fn.call_args[1]) >= 2  # also the magic kwargs
    assert fn.call_args[1]['kw1'] == 300
    assert fn.call_args[1]['kw2'] == 400


@fns
async def test_special_kwargs_added(fn, resource):
    body = {'metadata': {'uid': 'uid', 'name': 'name', 'namespace': 'ns'},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}

    # Values can be any.
    cause = ResourceChangingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        initial=False,
        reason=Reason.NOOP,
        memo=object(),
        body=Body(body),
        diff=object(),
        old=object(),
        new=object(),
    )

    fn = MagicMock(fn)
    await invoke(fn, cause=cause)

    assert fn.called
    assert fn.call_count == 1

    # Only check that kwargs are passed at all. The exact kwargs per cause are tested separately.
    assert 'logger' in fn.call_args[1]
    assert 'resource' in fn.call_args[1]
