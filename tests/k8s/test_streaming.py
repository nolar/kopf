"""
Only the tests for async-streaming events from the sync-API calls.

Excluded: the watching routines and edge case handling, such as ERROR events
(see ``test_watching.py``).

Excluded: the queueing routines, including multiplexing and workers/handlers
(see ``test_queueing.py``).
"""

import collections.abc

import pytest

from kopf.clients.watching import StopStreaming, streaming_next, streaming_aiter


async def test_streaming_next_never_ends_with_stopiteration():
    lst = []
    src = iter(lst)

    with pytest.raises(StopStreaming) as e:
        streaming_next(src)

    assert not isinstance(e, StopIteration)
    assert not isinstance(e, StopAsyncIteration)


async def test_streaming_next_yields_and_ends():
    lst = [1, 2, 3]
    src = iter(lst)

    val1 = streaming_next(src)
    val2 = streaming_next(src)
    val3 = streaming_next(src)
    assert val1 == 1
    assert val2 == 2
    assert val3 == 3

    with pytest.raises(StopStreaming):
        streaming_next(src)


async def test_streaming_iterator_with_regular_next_yields_and_ends():
    lst = [1, 2, 3]
    src = iter(lst)

    itr = streaming_aiter(src)
    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    val1 = next(src)
    val2 = next(src)
    val3 = next(src)
    assert val1 == 1
    assert val2 == 2
    assert val3 == 3

    with pytest.raises(StopIteration):
        next(src)


async def test_streaming_iterator_with_asyncfor_works():
    lst = [1, 2, 3]
    src = iter(lst)

    itr = streaming_aiter(src)
    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    vals = []
    async for val in itr:
        vals.append(val)
    assert vals == lst


async def test_streaming_iterator_with_syncfor_fails():
    lst = [1, 2, 3]
    src = iter(lst)

    itr = streaming_aiter(src)
    assert isinstance(itr, collections.abc.AsyncIterator)
    assert isinstance(itr, collections.abc.AsyncGenerator)

    with pytest.raises(TypeError):
        for _ in itr:
            pass
