import asyncio
import logging

import pytest

from kopf._cogs.aiokits.aiotoggles import ToggleSet
from kopf._cogs.clients.watching import streaming_block


async def test_pausing_is_ignored_if_turned_off(
        resource, namespace, looptime, assert_logs):
    operator_paused = ToggleSet(any)
    await operator_paused.make_toggle(False)

    async with streaming_block(
        resource=resource,
        namespace=namespace,
        operator_paused=operator_paused,
    ):
        pass

    assert looptime == 0
    assert_logs(prohibited=[
        r"Pausing the watch-stream for",
        r"Resuming the watch-stream for",
    ])


async def test_pausing_waits_forever_if_not_resumed(
        resource, namespace, looptime, assert_logs):
    operator_paused = ToggleSet(any)
    await operator_paused.make_toggle(True)

    async def do_it():
        async with streaming_block(
                resource=resource,
                namespace=namespace,
                operator_paused=operator_paused,
        ):
            pass

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(do_it(), timeout=1.23)

    assert looptime == 1.23
    assert_logs([
        r"Pausing the watch-stream for",
    ], prohibited=[
        r"Resuming the watch-stream for",
    ])


async def test_pausing_waits_until_resumed(
        resource, namespace, looptime, assert_logs):
    operator_paused = ToggleSet(any)
    conflicts_found = await operator_paused.make_toggle(True)

    async def delayed_resuming(delay: float):
        await asyncio.sleep(delay)
        await conflicts_found.turn_to(False)

    asyncio.create_task(delayed_resuming(1.23))
    async with streaming_block(
        resource=resource,
        namespace=namespace,
        operator_paused=operator_paused,
    ):
        pass

    assert looptime == 1.23
    assert_logs([
        r"Pausing the watch-stream for",
        r"Resuming the watch-stream for",
    ])
