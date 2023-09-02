"""
Just to make sure that asyncio tests are configured properly.
"""
import asyncio

_async_was_executed = False


async def test_async_tests_are_enabled():
    global _async_was_executed
    _async_was_executed = True  # asserted in a sync-test below.


async def test_async_mocks_are_enabled(mocker, looptime):
    p = mocker.patch('asyncio.sleep')
    await asyncio.sleep(1.0)

    assert p.call_count > 0
    assert p.await_count > 0
    assert looptime == 0


def test_async_test_was_executed_and_awaited():
    assert _async_was_executed
