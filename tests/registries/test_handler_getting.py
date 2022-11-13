import collections.abc

import pytest

import kopf
from kopf._cogs.structs.references import Selector
from kopf._core.intents.causes import Activity, ChangingCause, WatchingCause


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


def test_operator_registry_watching_handlers_via_iter(
        operator_registry_cls, cause_factory):

    cause = cause_factory(WatchingCause)
    registry = operator_registry_cls()
    iterator = registry._watching.iter_handlers(cause)

    assert isinstance(iterator, collections.abc.Iterator)
    assert not isinstance(iterator, collections.abc.Collection)
    assert not isinstance(iterator, collections.abc.Container)
    assert not isinstance(iterator, (list, tuple))

    handlers = list(iterator)
    assert not handlers


def test_operator_registry_changing_handlers_via_iter(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ChangingCause)
    registry = operator_registry_cls()
    iterator = registry._changing.iter_handlers(cause)

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


def test_operator_registry_watching_handlers_via_list(
        operator_registry_cls, cause_factory):

    cause = cause_factory(WatchingCause)
    registry = operator_registry_cls()
    handlers = registry._watching.get_handlers(cause)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers


def test_operator_registry_changing_handlers_via_list(
        operator_registry_cls, cause_factory):

    cause = cause_factory(ChangingCause)
    registry = operator_registry_cls()
    handlers = registry._changing.get_handlers(cause)

    assert isinstance(handlers, collections.abc.Iterable)
    assert isinstance(handlers, collections.abc.Container)
    assert isinstance(handlers, collections.abc.Collection)
    assert not handlers


def test_all_handlers(operator_registry_cls):
    registry = operator_registry_cls()

    @kopf.index('resource', registry=registry)
    @kopf.on.event('resource', registry=registry)
    @kopf.on.event('resource', registry=registry)
    @kopf.on.create('resource', registry=registry)
    @kopf.on.update('resource', registry=registry)
    @kopf.on.delete('resource', registry=registry)
    @kopf.on.resume('resource', registry=registry)
    @kopf.on.timer('resource', registry=registry)
    @kopf.on.daemon('resource', registry=registry)
    def fn(**_): pass

    assert len(registry._indexing.get_all_handlers()) == 1
    assert len(registry._watching.get_all_handlers()) == 2
    assert len(registry._spawning.get_all_handlers()) == 2
    assert len(registry._changing.get_all_handlers()) == 4


def test_all_selectors(operator_registry_cls):
    registry = operator_registry_cls()

    @kopf.index('resource0', registry=registry)
    @kopf.on.event('resource1', registry=registry)
    @kopf.on.event('resource2', registry=registry)
    @kopf.on.create('resource1', registry=registry)
    @kopf.on.update('resource2', registry=registry)
    @kopf.on.delete('resource3', registry=registry)
    @kopf.on.resume('resource4', registry=registry)
    @kopf.on.timer('resource5', registry=registry)
    @kopf.on.daemon('resource6', registry=registry)
    def fn(**_): pass

    assert registry._indexing.get_all_selectors() == {Selector('resource0')}
    assert registry._watching.get_all_selectors() == {Selector('resource1'), Selector('resource2')}
    assert registry._spawning.get_all_selectors() == {Selector('resource5'), Selector('resource6')}
    assert registry._changing.get_all_selectors() == {Selector('resource1'), Selector('resource2'),
                                                      Selector('resource3'), Selector('resource4')}
