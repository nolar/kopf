import pytest

from kopf.structs.dicts import resolve


def test_existing_key():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'])
    assert r == 'val'


def test_unexisting_key_with_no_default():
    d = {'abc': {'def': {'hij': 'val'}}}
    with pytest.raises(KeyError):
        resolve(d, ['abc', 'def', 'xyz'])


def test_unexisting_key_with_default_none():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'xyz'], None)
    assert r is None


def test_unexisting_key_with_default_value():
    default = object()
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'xyz'], default)
    assert r is default


def test_nonmapping_key():
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        resolve(d, ['key', 'sub'])


def test_empty_path():
    d = {'key': 'val'}
    r = resolve(d, [])
    assert r == d
    assert r is d
