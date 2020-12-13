import asyncio
import logging

import async_timeout
import pytest

from kopf.clients.watching import streaming_block
from kopf.structs.primitives import Toggle, ToggleSet


async def test_freezing_is_ignored_if_turned_off(
        resource, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    freeze_checker = ToggleSet()
    await freeze_checker.make_toggle(False)

    async with timer, async_timeout.timeout(0.5) as timeout:
        async with streaming_block(
            resource=resource,
            namespace=namespace,
            freeze_checker=freeze_checker,
        ):
            pass

    assert not timeout.expired
    assert timer.seconds < 0.2  # no waits, exits as soon as possible
    assert_logs([], prohibited=[
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_forever_if_not_resumed(
        resource, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    freeze_checker = ToggleSet()
    await freeze_checker.make_toggle(True)

    with pytest.raises(asyncio.TimeoutError):
        async with timer, async_timeout.timeout(0.5) as timeout:
            async with streaming_block(
                resource=resource,
                namespace=namespace,
                freeze_checker=freeze_checker,
            ):
                pass

    assert timeout.expired
    assert timer.seconds >= 0.5
    assert_logs([
        r"Freezing the watch-stream for",
    ], prohibited=[
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_until_resumed(
        resource, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    freeze_checker = ToggleSet()
    freeze_toggle = await freeze_checker.make_toggle(True)

    async def delayed_resuming(delay: float):
        await asyncio.sleep(delay)
        await freeze_toggle.turn_to(False)

    async with timer, async_timeout.timeout(1.0) as timeout:
        asyncio.create_task(delayed_resuming(0.2))
        async with streaming_block(
            resource=resource,
            namespace=namespace,
            freeze_checker=freeze_checker,
        ):
            pass

    assert not timeout.expired
    assert timer.seconds >= 0.2
    assert timer.seconds <= 0.5
    assert_logs([
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])
