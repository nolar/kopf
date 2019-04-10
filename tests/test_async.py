"""
Just to make sure that asyncio tests are configured properly.
"""
import asyncio

_async_was_executed = False


async def test_async_tests_are_enabled(timer):
    global _async_was_executed
    _async_was_executed = True  # asserted in a sync-test below.

    with timer as t:
        await asyncio.sleep(0.5)

    assert t.seconds > 0.5  # real sleep


async def test_async_mocks_are_enabled(timer, mocker):
    p = mocker.patch('asyncio.sleep')
    with timer as t:
        await asyncio.sleep(1.0)

    assert p.called
    assert p.awaited
    assert t.seconds < 0.01  # mocked sleep


def test_async_test_was_executed_and_awaited():
    assert _async_was_executed
