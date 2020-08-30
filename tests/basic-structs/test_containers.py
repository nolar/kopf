import collections.abc

import pytest

from kopf.structs.bodies import Body
from kopf.structs.containers import Memo, ResourceMemories, ResourceMemory

BODY: Body = {
    'metadata': {
        'uid': 'uid1',
    }
}


def test_creation_with_defaults():
    ResourceMemory()


async def test_recalling_creates_when_absent():
    memories = ResourceMemories()
    memory = await memories.recall(BODY)
    assert isinstance(memory, ResourceMemory)


async def test_recalling_reuses_when_present():
    memories = ResourceMemories()
    memory1 = await memories.recall(BODY)
    memory2 = await memories.recall(BODY)
    assert memory1 is memory2


async def test_forgetting_deletes_when_present():
    memories = ResourceMemories()
    memory1 = await memories.recall(BODY)
    await memories.forget(BODY)

    # Check by recalling -- it should be a new one.
    memory2 = await memories.recall(BODY)
    assert memory1 is not memory2


async def test_forgetting_ignores_when_absent():
    memories = ResourceMemories()
    await memories.forget(BODY)


def test_object_dict_creation():
    obj = Memo()
    assert isinstance(obj, collections.abc.MutableMapping)


def test_object_dict_fields_are_keys():
    obj = Memo()
    obj.xyz = 100
    assert obj['xyz'] == 100


def test_object_dict_keys_are_fields():
    obj = Memo()
    obj['xyz'] = 100
    assert obj.xyz == 100


def test_object_dict_keys_deleted():
    obj = Memo()
    obj['xyz'] = 100
    del obj['xyz']
    assert obj == {}


def test_object_dict_fields_deleted():
    obj = Memo()
    obj.xyz = 100
    del obj.xyz
    assert obj == {}


def test_object_dict_raises_key_errors_on_get():
    obj = Memo()
    with pytest.raises(KeyError):
        obj['unexistent']


def test_object_dict_raises_attribute_errors_on_get():
    obj = Memo()
    with pytest.raises(AttributeError):
        obj.unexistent


def test_object_dict_raises_key_errors_on_del():
    obj = Memo()
    with pytest.raises(KeyError):
        del obj['unexistent']


def test_object_dict_raises_attribute_errors_on_del():
    obj = Memo()
    with pytest.raises(AttributeError):
        del obj.unexistent
