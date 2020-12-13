import asyncio
import logging

import async_timeout
import pytest

from kopf.clients.watching import streaming_watch
from kopf.structs.primitives import Toggle

STREAM_WITH_NORMAL_EVENTS = [
    {'type': 'ADDED', 'object': {'spec': 'a'}},
    {'type': 'ADDED', 'object': {'spec': 'b'}},
]


async def test_freezing_is_ignored_if_turned_off(
        settings, resource, stream, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(False)
    events = []

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    async with timer, async_timeout.timeout(0.5) as timeout:
        await read_stream()

    assert len(events) == 2
    assert timer.seconds < 0.2  # no waits, exits as soon as possible
    assert not timeout.expired
    assert_logs([], prohibited=[
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_forever_if_not_resumed(
        settings, resource, stream, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(True)
    events = []

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    with pytest.raises(asyncio.TimeoutError):
        async with timer, async_timeout.timeout(0.5) as timeout:
            await read_stream()

    assert len(events) == 0
    assert timer.seconds >= 0.5
    assert timeout.expired
    assert_logs([
        r"Freezing the watch-stream for",
    ], prohibited=[
        r"Resuming the watch-stream for",
    ])


async def test_freezing_waits_until_resumed(
        settings, resource, stream, namespace, timer, caplog, assert_logs):
    caplog.set_level(logging.DEBUG)

    stream.feed(STREAM_WITH_NORMAL_EVENTS, namespace=namespace)
    stream.close(namespace=namespace)

    freeze_mode = Toggle(True)
    events = []

    async def delayed_resuming(delay: float):
        await asyncio.sleep(delay)
        await freeze_mode.turn_off()

    async def read_stream():
        async for event in streaming_watch(settings=settings,
                                           resource=resource,
                                           namespace=namespace,
                                           freeze_mode=freeze_mode):
            events.append(event)

    async with timer, async_timeout.timeout(0.5) as timeout:
        asyncio.create_task(delayed_resuming(0.2))
        await read_stream()

    assert len(events) == 2
    assert timer.seconds >= 0.2
    assert timer.seconds <= 0.5
    assert not timeout.expired
    assert_logs([
        r"Freezing the watch-stream for",
        r"Resuming the watch-stream for",
    ])
