import pytest

from kopf.structs.dicts import ensure


def test_existing_key():
    d = {'abc': {'def': {'hij': 'val'}}}
    ensure(d, ['abc', 'def', 'hij'], 'new')
    assert d == {'abc': {'def': {'hij': 'new'}}}


def test_unexisting_key_in_existing_dict():
    d = {'abc': {'def': {}}}
    ensure(d, ['abc', 'def', 'hij'], 'new')
    assert d == {'abc': {'def': {'hij': 'new'}}}


def test_unexisting_key_in_unexisting_dict():
    d = {}
    ensure(d, ['abc', 'def', 'hij'], 'new')
    assert d == {'abc': {'def': {'hij': 'new'}}}


def test_toplevel_key():
    d = {'key': 'val'}
    ensure(d, ['key'], 'new')
    assert d == {'key': 'new'}


def test_nonmapping_key():
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        ensure(d, ['key', 'sub'], 'new')


def test_empty_path():
    d = {}
    with pytest.raises(ValueError) as e:
        ensure(d, [], 'new')
    assert "Setting a root of a dict is impossible" in str(e.value)
