"""
Watching & streaming of the watch-events.

Kubernetes client's watching streams are synchronous. To make them asynchronous,
we put them into a `concurrent.futures.ThreadPoolExecutor`,
and yield from there asynchronously.

All of this is a workaround for the standard Kubernetes client's limitations.
They are not needed if the client library is natively asynchronous.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


async def streaming_aiter(src, loop=None, executor=None):
    """
    Same as `iter`, but asynchronous and stops on `StopStreaming`, not on `StopIteration`.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    while True:
        yield await loop.run_in_executor(executor, next, src)
