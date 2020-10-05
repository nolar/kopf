import collections.abc

import pytest

from kopf.structs.diffs import DiffOperation, DiffScope, diff


@pytest.mark.parametrize('scope', list(DiffScope))
def test_none_for_old(scope):
    a = None
    b = object()
    d = diff(a, b, scope=scope)
    assert d == (('add', (), None, b),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_none_for_new(scope):
    a = object()
    b = None
    d = diff(a, b, scope=scope)
    assert d == (('remove', (), a, None),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_nones_for_both(scope):
    a = None
    b = None
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', list(DiffScope))
def test_scalars_equal(scope):
    a = 100
    b = 100
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', list(DiffScope))
def test_scalars_unequal(scope):
    a = 100
    b = 200
    d = diff(a, b, scope=scope)
    assert d == (('change', (), 100, 200),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_strings_equal(scope):
    a = 'hello'
    b = 'hello'
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', list(DiffScope))
def test_strings_unequal(scope):
    a = 'hello'
    b = 'world'
    d = diff(a, b, scope=scope)
    assert d == (('change', (), 'hello', 'world'),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_lists_equal(scope):
    a = [100, 200, 300]
    b = [100, 200, 300]
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', list(DiffScope))
def test_lists_unequal(scope):
    a = [100, 200, 300]
    b = [100, 666, 300]
    d = diff(a, b, scope=scope)
    assert d == (('change', (), [100, 200, 300], [100, 666, 300]),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_dicts_equal(scope):
    a = {'hello': 'world', 'key': 'val'}
    b = {'key': 'val', 'hello': 'world'}
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', [DiffScope.FULL, DiffScope.RIGHT])
def test_dicts_with_keys_added_and_noticed(scope):
    a = {'hello': 'world'}
    b = {'hello': 'world', 'key': 'val'}
    d = diff(a, b, scope=scope)
    assert d == (('add', ('key',), None, 'val'),)


@pytest.mark.parametrize('scope', [DiffScope.LEFT])
def test_dicts_with_keys_added_but_ignored(scope):
    a = {'hello': 'world'}
    b = {'hello': 'world', 'key': 'val'}
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', [DiffScope.FULL, DiffScope.LEFT])
def test_dicts_with_keys_removed_and_noticed(scope):
    a = {'hello': 'world', 'key': 'val'}
    b = {'hello': 'world'}
    d = diff(a, b, scope=scope)
    assert d == (('remove', ('key',), 'val', None),)


@pytest.mark.parametrize('scope', [DiffScope.RIGHT])
def test_dicts_with_keys_removed_but_ignored(scope):
    a = {'hello': 'world', 'key': 'val'}
    b = {'hello': 'world'}
    d = diff(a, b, scope=scope)
    assert d == ()


@pytest.mark.parametrize('scope', list(DiffScope))
def test_dicts_with_keys_changed(scope):
    a = {'hello': 'world', 'key': 'old'}
    b = {'hello': 'world', 'key': 'new'}
    d = diff(a, b, scope=scope)
    assert d == (('change', ('key',), 'old', 'new'),)


@pytest.mark.parametrize('scope', list(DiffScope))
def test_dicts_with_subkeys_changed(scope):
    a = {'main': {'hello': 'world', 'key': 'old'}}
    b = {'main': {'hello': 'world', 'key': 'new'}}
    d = diff(a, b, scope=scope)
    assert d == (('change', ('main', 'key'), 'old', 'new'),)


def test_custom_mappings_are_recursed():

    class SampleMapping(collections.abc.Mapping):
        def __init__(self, data=(), **kwargs) -> None:
            super().__init__()
            self._items = dict(data, **kwargs)
        def __len__(self) -> int: return len(self._items)
        def __iter__(self): return iter(self._items)
        def __getitem__(self, item: str) -> str: return self._items[item]

    class MappingA(SampleMapping): pass
    class MappingB(SampleMapping): pass

    a = MappingA(a=100, b=200)
    b = MappingB(b=300, c=400)
    d = diff(a, b)
    assert (DiffOperation.REMOVE, ('a',), 100, None) in d
    assert (DiffOperation.CHANGE, ('b',), 200, 300) in d
    assert (DiffOperation.ADD, ('c',), None, 400) in d
    assert (DiffOperation.CHANGE, (), a, b) not in d


# A few examples of slightly more realistic non-abstracted use-cases below:
def test_dicts_adding_label():
    body_before_labelling = {'metadata': {}}
    body_after_labelling  = {'metadata': {'labels': 'LABEL'}}

    d = diff(body_before_labelling, body_after_labelling)
    assert d == (('add', ('metadata', 'labels'), None, 'LABEL'),)


def test_dicts_updating_storage_size():
    body_before_storage_size_update = {'spec': {'size': '42G'}}
    body_after_storage_size_update  = {'spec': {'size': '76G'}}

    d = diff(body_before_storage_size_update, body_after_storage_size_update)
    assert d == (('change', ('spec', 'size'), '42G', '76G'),)


def test_dicts_different_items_handled():
    body_before_storage_size_update = {'spec': {'items': ['task1', 'task2']}}
    body_after_storage_size_update  = {'spec': {'items': ['task3', 'task4']}}

    d = diff(body_before_storage_size_update, body_after_storage_size_update)
    assert d == (('change', ('spec', 'items'), ['task1', 'task2'], ['task3', 'task4']),)
