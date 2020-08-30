import asyncio

import pytest

from kopf.engines.posting import event_queue_loop_var, event_queue_var


@pytest.fixture()
def event_queue_loop(event_loop):
    token = event_queue_loop_var.set(event_loop)
    try:
        yield event_loop
    finally:
        event_queue_loop_var.reset(token)


@pytest.fixture()
def event_queue():
    queue = asyncio.Queue()
    token = event_queue_var.set(queue)
    try:
        yield queue
    finally:
        event_queue_var.reset(token)
