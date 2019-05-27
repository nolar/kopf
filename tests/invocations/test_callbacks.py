import functools
import traceback

import pytest
from asynctest import Mock, MagicMock

from kopf.reactor.invocation import invoke, is_async_fn

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
    assert is_async is None


@syncasyncparams
async def test_async_detection(fn, expected):
    is_async = is_async_fn(fn)
    assert is_async is expected


@syncasyncparams
async def test_stacktrace_visibility(fn, expected):
    stack_trace_marker = STACK_TRACE_MARKER  # searched by fn
    cause = Mock()
    found = await invoke(fn, cause=cause)
    assert found is expected


@fns
async def test_result_returned(fn):
    fn = MagicMock(fn, return_value=999)
    cause = Mock()
    result = await invoke(fn, cause=cause)
    assert result == 999


@fns
async def test_explicit_args_passed_properly(fn):
    fn = MagicMock(fn)
    cause = Mock()
    await invoke(fn, 100, 200, cause=cause, kw1=300, kw2=400)

    assert fn.called
    assert fn.call_count == 1

    assert len(fn.call_args[0]) == 2
    assert fn.call_args[0][0] == 100
    assert fn.call_args[0][1] == 200

    assert len(fn.call_args[1]) >= 2  # also the magic kwargs
    assert fn.call_args[1]['kw1'] == 300
    assert fn.call_args[1]['kw2'] == 400


@fns
async def test_special_kwargs_added(fn):
    fn = MagicMock(fn)
    cause = MagicMock(body={'metadata': {'uid': 'uid', 'name': 'name', 'namespace': 'ns'}})
    await invoke(fn, cause=cause)

    assert fn.called
    assert fn.call_count == 1

    assert len(fn.call_args[1]) >= 2
    assert fn.call_args[1]['cause'] is cause
    assert fn.call_args[1]['event'] is cause.event
    assert fn.call_args[1]['body'] is cause.body
    assert fn.call_args[1]['spec'] is cause.body['spec']
    assert fn.call_args[1]['meta'] is cause.body['metadata']
    assert fn.call_args[1]['status'] is cause.body['status']
    assert fn.call_args[1]['diff'] is cause.diff
    assert fn.call_args[1]['old'] is cause.old
    assert fn.call_args[1]['new'] is cause.new
    assert fn.call_args[1]['patch'] is cause.patch
    assert fn.call_args[1]['logger'] is cause.logger
    assert fn.call_args[1]['uid'] is cause.body['metadata']['uid']
    assert fn.call_args[1]['name'] is cause.body['metadata']['name']
    assert fn.call_args[1]['namespace'] is cause.body['metadata']['namespace']
