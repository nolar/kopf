import pytest

from kopf.structs.dicts import remove


def test_existing_key():
    d = {'abc': {'def': {'hij': 'val', 'hello': 'world'}}}
    remove(d, ['abc', 'def', 'hij'])
    assert d == {'abc': {'def': {'hello': 'world'}}}


def test_unexisting_key_in_existing_dict():
    d = {'abc': {'def': {'hello': 'world'}}}
    remove(d, ['abc', 'def', 'hij'])
    assert d == {'abc': {'def': {'hello': 'world'}}}


def test_unexisting_key_in_unexisting_dict():
    d = {}
    remove(d, ['abc', 'def', 'hij'])
    assert d == {}


def test_parent_cascaded_deletion_up_to_the_root():
    d = {'abc': {'def': {'hij': 'val'}}}
    remove(d, ['abc', 'def', 'hij'])
    assert d == {}


def test_parent_cascaded_deletion_up_to_a_middle():
    d = {'abc': {'def': {'hij': 'val'}, 'hello': 'world'}}
    remove(d, ['abc', 'def', 'hij'])
    assert d == {'abc': {'hello': 'world'}}


def test_nonmapping_key():
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        remove(d, ['key', 'sub'])


def test_empty_path():
    d = {}
    with pytest.raises(ValueError) as e:
        remove(d, [])
    assert "Removing a root of a dict is impossible" in str(e.value)
