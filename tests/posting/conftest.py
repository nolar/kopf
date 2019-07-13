import asyncio

import pytest

from kopf.engines.posting import event_queue_var


@pytest.fixture()
def event_queue(event_loop):
    queue = asyncio.Queue(loop=event_loop)
    token = event_queue_var.set(queue)
    try:
        yield queue
    finally:
        event_queue_var.reset(token)
