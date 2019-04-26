import pytest

from kopf.structs.diffs import resolve


def test_existing_key():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'])
    assert r == 'val'


def test_unexisting_key():
    d = {'abc': {'def': {'hij': 'val'}}}
    with pytest.raises(KeyError):
        resolve(d, ['abc', 'def', 'xyz'])


def test_nonmapping_key():
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        resolve(d, ['key', 'sub'])


def test_empty_path():
    d = {'key': 'val'}
    r = resolve(d, [])
    assert r == d
    assert r is d
