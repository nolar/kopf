import collections.abc

import pytest

from kopf.structs.ephemera import Memo


def test_creation_with_defaults():
    obj = Memo()
    assert isinstance(obj, collections.abc.MutableMapping)
    assert not set(obj)


def test_creation_with_dict():
    obj = Memo({'xyz': 100})
    assert isinstance(obj, collections.abc.MutableMapping)
    assert set(obj) == {'xyz'}


def test_creation_with_list():
    obj = Memo([('xyz', 100)])
    assert isinstance(obj, collections.abc.MutableMapping)
    assert set(obj) == {'xyz'}


def test_creation_with_memo():
    obj = Memo(Memo({'xyz': 100}))
    assert isinstance(obj, collections.abc.MutableMapping)
    assert set(obj) == {'xyz'}


def test_fields_are_keys():
    obj = Memo()
    obj.xyz = 100
    assert obj['xyz'] == 100


def test_keys_are_fields():
    obj = Memo()
    obj['xyz'] = 100
    assert obj.xyz == 100


def test_keys_deleted():
    obj = Memo()
    obj['xyz'] = 100
    del obj['xyz']
    assert obj == {}


def test_fields_deleted():
    obj = Memo()
    obj.xyz = 100
    del obj.xyz
    assert obj == {}


def test_raises_key_errors_on_get():
    obj = Memo()
    with pytest.raises(KeyError):
        obj['unexistent']


def test_raises_attribute_errors_on_get():
    obj = Memo()
    with pytest.raises(AttributeError):
        obj.unexistent


def test_raises_key_errors_on_del():
    obj = Memo()
    with pytest.raises(KeyError):
        del obj['unexistent']


def test_raises_attribute_errors_on_del():
    obj = Memo()
    with pytest.raises(AttributeError):
        del obj.unexistent


def test_shallow_copied_keys():
    obj1 = Memo({'xyz': 100})
    obj2 = Memo(obj1)
    obj1['xyz'] = 200
    assert obj2['xyz'] == 100


def test_shallow_copied_values():
    obj1 = Memo({'xyz': 100})
    obj2 = Memo(dat=obj1)
    obj1['xyz'] = 200
    assert obj2['dat']['xyz'] == 200
