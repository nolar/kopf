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
import types

import pytest

from kopf.structs.dicts import resolve, resolve_obj

default = object()


#
# Both resolve functions should behave exactly the same for dicts.
#
@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_existent_key_with_no_default(resolve):
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'])
    assert r == 'val'


@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_existent_key_with_default(resolve):
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'hij'], default)
    assert r == 'val'


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'def', 'xyz'], id='3rdlvl'),
])
@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_inexistent_key_with_no_default(resolve, key):
    d = {'abc': {'def': {'hij': 'val'}}}
    with pytest.raises(KeyError):
        resolve(d, key)


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'def', 'xyz'], id='3rdlvl'),
])
@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_inexistent_key_with_default(resolve, key):
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, key, default)
    assert r is default


@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_nonmapping_with_no_default(resolve):
    d = {'key': 'val'}
    with pytest.raises(TypeError):
        resolve(d, ['key', 'sub'])


@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_nonmapping_with_default(resolve):
    d = {'key': 'val'}
    r = resolve(d, ['key', 'sub'], default)
    assert r is default


@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_none_is_treated_as_a_regular_default_value(resolve):
    d = {'abc': {'def': {'hij': 'val'}}}
    r = resolve(d, ['abc', 'def', 'xyz'], None)
    assert r is None


@pytest.mark.parametrize('resolve', [resolve, resolve_obj])
def test_dict_with_empty_path(resolve):
    d = {'key': 'val'}
    r = resolve(d, [])
    assert r == d
    assert r is d


#
# Specialised drill-down for objects.
#
class FakeKubernetesModel:  # no bases!
    __module__ = 'kubernetes.client.models.fake-for-tests'

    @property
    def metadata(self):
        return None

    attribute_map = {
        'AbC': 'abc',
        'zzz': 'dez',
    }


@pytest.fixture(params=[FakeKubernetesModel, types.SimpleNamespace])
def obj(request):
    cls = request.param
    obj = cls()
    if cls is FakeKubernetesModel:
        # With attribute mapping in mind.
        obj.key = 'val'
        obj.AbC = cls()
        obj.AbC.zzz = cls()
        obj.AbC.zzz.hij = 'val'
    else:
        # Exactly as they will be requested.
        obj.key = 'val'
        obj.abc = cls()
        obj.abc.dez = cls()
        obj.abc.dez.hij = 'val'
    return obj


def test_object_with_existent_key_with_no_default(obj):
    r = resolve_obj(obj, ['abc', 'dez', 'hij'])
    assert r == 'val'


def test_object_with_existent_key_with_default(obj):
    r = resolve_obj(obj, ['abc', 'dez', 'hij'], default)
    assert r == 'val'


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'dez', 'xyz'], id='3rdlvl'),
])
def test_object_with_inexistent_key_with_no_default(obj, key):
    with pytest.raises(AttributeError):
        resolve_obj(obj, key)


@pytest.mark.parametrize('key', [
    pytest.param(['rst', 'uvw', 'xyz'], id='1stlvl'),
    pytest.param(['abc', 'uvw', 'xyz'], id='2ndlvl'),
    pytest.param(['abc', 'dez', 'xyz'], id='3rdlvl'),
])
def test_object_with_inexistent_key_with_default(obj, key):
    r = resolve_obj(obj, key, default)
    assert r is default


def test_object_with_nonmapping_with_no_default(obj):
    with pytest.raises(TypeError):
        resolve_obj(obj, ['key', 'sub'])


def test_object_with_nonmapping_with_default(obj):
    r = resolve_obj(obj, ['key', 'sub'], default)
    assert r is default


def test_object_with_none_is_treated_as_a_regular_default_value(obj):
    r = resolve_obj(obj, ['abc', 'dez', 'xyz'], None)
    assert r is None


def test_object_with_empty_path(obj):
    r = resolve_obj(obj, [])
    assert r == obj
    assert r is obj


#
# Some special cases.
#
@pytest.mark.parametrize('cls', (tuple, list, set, frozenset, str, bytes))
def test_raises_for_builtins(cls):
    obj = cls()
    with pytest.raises(TypeError):
        resolve_obj(obj, ['__class__'])
