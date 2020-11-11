"""
The test design notes:

* The field "abc.def.hij" is existent.
* The field "rst.uvw.xyz" is inexistent.
* Its mixtures ("a.b.z", "a.y.z") are used to simulate partial inexistence.

* For the existent keys, kwargs should not matter.
* For the non-existent keys, the default is returned,
  or a ``KeyError`` raised -- as with regular mappings.

* For special cases with "wrong" values (``"value"["z"]``, ``None["z"]``, etc),
  either a ``TypeError`` should be raised normally. If "wrong" values are said
  to be ignored, then they are treated the same as inexistent values,
  and the default value is returned or a ``KeyError`` is raised.

"""
import pytest

from kopf.structs.dicts import resolve

default = object()


def test_existent_key_with_no_default():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'])
    assert r == 'val'


def test_existent_key_with_default():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'], default)
    assert r == 'val'


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'def', 'xyz'], id='3rdlvl'),
])
def test_inexistent_key_with_no_default(key):
    d = {'abc': {'def': {'hij': 'val'}}}
    with pytest.raises(KeyError):
        resolve(d, key)


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'def', 'xyz'], id='3rdlvl'),
])
def test_inexistent_key_with_default(key):
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, key, default)
    assert r is default


def test_nonmapping_with_no_default():
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        resolve(d, ['key', 'sub'])


def test_nonmapping_with_default():
    d = {'key': 'val'}
    r = resolve(d, ['key', 'sub'], default)
    assert r is default


def test_none_is_treated_as_a_regular_default_value():
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'xyz'], None)
    assert r is None


def test_empty_path():
    d = {'key': 'val'}
    r = resolve(d, [])
    assert r == d
    assert r is d
