import functools
from unittest.mock import Mock

import pytest

from kopf import SimpleRegistry, GlobalRegistry
from kopf.reactor.registry import FIELD


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn():
    pass


@pytest.fixture(params=['simple', 'global'])
def registry(request):
    if request.param == 'simple':
        return SimpleRegistry()
    if request.param == 'global':
        return GlobalRegistry()
    raise Exception(f"Unsupported registry type: {request.param}")


@pytest.fixture()
def register_fn(registry, resource):
    if isinstance(registry, SimpleRegistry):
        return registry.register
    if isinstance(registry, GlobalRegistry):
        return functools.partial(registry.register, resource.group, resource.version, resource.plural)
    raise Exception(f"Unsupported registry type: {registry}")


@pytest.fixture()
def cause_no_diff(resource):
    return Mock(resource=resource, event='some-event', diff=None)


@pytest.fixture()
def cause_with_diff(resource):
    diff = [('op', ('some-field',), 'old', 'new')]
    return Mock(resource=resource, event='some-event', diff=diff)


def test_catch_all_handlers_found(registry, register_fn, cause_no_diff):
    register_fn(some_fn, event=None)
    handlers = registry.get_handlers(cause_no_diff)
    assert handlers


def test_relevant_event_handlers_found(registry, register_fn, cause_no_diff):
    register_fn(some_fn, event='some-event')
    handlers = registry.get_handlers(cause_no_diff)
    assert handlers


def test_relevant_field_handlers_found(registry, register_fn, cause_with_diff):
    register_fn(some_fn, event=FIELD, field='some-field')
    handlers = registry.get_handlers(cause_with_diff)
    assert handlers


def test_irrelevant_event_handlers_ignored(registry, register_fn, cause_no_diff):
    register_fn(some_fn, event='another-event')
    handlers = registry.get_handlers(cause_no_diff)
    assert not handlers


def test_irrelevant_field_handlers_ignored(registry, register_fn, cause_with_diff):
    register_fn(some_fn, event=FIELD, field='another-field')
    handlers = registry.get_handlers(cause_with_diff)
    assert not handlers


def test_order_persisted_a(registry, register_fn, cause_with_diff):
    register_fn(some_fn, event=None)
    register_fn(some_fn, event='some-event')
    register_fn(some_fn, event=FIELD, field='another-field')
    register_fn(some_fn, event=FIELD, field='some-field')

    handlers = registry.get_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].event is None
    assert handlers[1].field is None
    assert handlers[2].field == ('some-field',)


def test_order_persisted_b(registry, register_fn, cause_with_diff):
    register_fn(some_fn, event=FIELD, field='some-field')
    register_fn(some_fn, event=FIELD, field='another-field')
    register_fn(some_fn, event='some-event')
    register_fn(some_fn, event=None)

    handlers = registry.get_handlers(cause_with_diff)

    # Order must be preserved -- same as registered.
    assert len(handlers) == 3
    assert handlers[0].field == ('some-field',)
    assert handlers[1].field is None
    assert handlers[2].event is None
