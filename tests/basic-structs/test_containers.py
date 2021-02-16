from unittest.mock import Mock

from kopf.structs.bodies import Body
from kopf.structs.containers import ResourceMemories, ResourceMemory
from kopf.structs.ephemera import Memo

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


async def test_memo_is_shallow_copied():

    class MyMemo(Memo):
        def __copy__(self):
            mock()
            return MyMemo()

    mock = Mock()
    memo = MyMemo()
    memories = ResourceMemories()
    memory = await memories.recall(BODY, memo=memo)
    assert mock.call_count == 1
    assert memory.memo is not memo
