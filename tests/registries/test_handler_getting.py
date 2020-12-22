import collections.abc

import pytest

from kopf.reactor.causation import ResourceChangingCause, ResourceWatchingCause
from kopf.structs.handlers import Activity


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn():
    pass


def test_generic_registry_via_iter(
        generic_registry_cls, cause_factory):

    cause = cause_factory(generic_registry_cls)
    registry = generic_registry_cls()
    iterator = registry.iter_handlers(cause)

    assert isinstance(iterator, collections.abc.Iterator)
    assert not isinstance(iterator, collections.abc.Collection)
    assert not isinstance(iterator, collections.abc.Container)
    assert not isinstance(iterator, (list, tuple))

    handlers = list(iterator)
    assert not handlers


def test_generic_registry_via_list(
        generic_registry_cls, cause_factory):

    cause = cause_factory(generic_registry_cls)
    registry = generic_registry_cls()
    handlers = registry.get_handlers(cause)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers


@pytest.mark.parametrize('activity', list(Activity))
def test_operator_registry_with_activity_via_iter(
        operator_registry_cls, activity):

    registry = operator_registry_cls()
    iterator = registry._activities.iter_handlers(activity=activity)

    assert isinstance(iterator, collections.abc.Iterator)
    assert not isinstance(iterator, collections.abc.Collection)
    assert not isinstance(iterator, collections.abc.Container)
    assert not isinstance(iterator, (list, tuple))

    handlers = list(iterator)
    assert not handlers


def test_operator_registry_with_resource_watching_via_iter(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ResourceWatchingCause)
    registry = operator_registry_cls()
    iterator = registry._resource_watching.iter_handlers(cause)

    assert isinstance(iterator, collections.abc.Iterator)
    assert not isinstance(iterator, collections.abc.Collection)
    assert not isinstance(iterator, collections.abc.Container)
    assert not isinstance(iterator, (list, tuple))

    handlers = list(iterator)
    assert not handlers


def test_operator_registry_with_resource_changing_via_iter(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ResourceChangingCause)
    registry = operator_registry_cls()
    iterator = registry._resource_changing.iter_handlers(cause)

    assert isinstance(iterator, collections.abc.Iterator)
    assert not isinstance(iterator, collections.abc.Collection)
    assert not isinstance(iterator, collections.abc.Container)
    assert not isinstance(iterator, (list, tuple))

    handlers = list(iterator)
    assert not handlers


@pytest.mark.parametrize('activity', list(Activity))
def test_operator_registry_with_activity_via_list(
        operator_registry_cls, activity):

    registry = operator_registry_cls()
    handlers = registry._activities.get_handlers(activity=activity)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers


def test_operator_registry_with_resource_watching_via_list(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ResourceWatchingCause)
    registry = operator_registry_cls()
    handlers = registry._resource_watching.get_handlers(cause)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers


def test_operator_registry_with_resource_changing_via_list(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ResourceChangingCause)
    registry = operator_registry_cls()
    handlers = registry._resource_changing.get_handlers(cause)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers
