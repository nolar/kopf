"""
Watching and streaming watch-events.

Kubernetes client's watching streams are synchronous. To make them asynchronous,
we put them into a `concurrent.futures.ThreadPoolExecutor`,
and yield from there asynchronously.

However, async/await coroutines misbehave with `StopIteration` exceptions
raised by the `next` method: see `PEP-479`_.

As a workaround, we replace `StopIteration` with our custom `StopStreaming`
inherited from `RuntimeError` (as suggested by `PEP-479`_),
and re-implement the generators to make them async.

All of this is a workaround for the standard Kubernetes client's limitations.
They would not be needed if the client library were natively asynchronous.

.. _PEP-479: https://www.python.org/dev/peps/pep-0479/
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


class StopStreaming(RuntimeError):
    """
    Raised when the watch-stream generator ends streaming.
    Replaces `StopIteration`.
    """


def streaming_next(src):
    """
    Same as `next`, but replaces the `StopIteration` with `StopStreaming`.
    """
    try:
        return next(src)
    except StopIteration as e:
        raise StopStreaming(str(e))


async def streaming_aiter(src, loop=None, executor=None):
    """
    Same as `iter`, but asynchronous and stops on `StopStreaming`, not on `StopIteration`.
    """
    loop = loop if loop is not None else asyncio.get_event_loop()
    while True:
        try:
            yield await loop.run_in_executor(executor, streaming_next, src)
        except StopStreaming:
            return
