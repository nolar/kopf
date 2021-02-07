import asyncio

import pytest

from kopf.engines.posting import event_queue_loop_var, event_queue_var


@pytest.fixture(autouse=True)
def _caplog_all_levels(caplog):
    caplog.set_level(0)


@pytest.fixture(autouse=True)
def event_queue_loop(event_loop):
    token = event_queue_loop_var.set(event_loop)
    try:
        yield event_loop
    finally:
        event_queue_loop_var.reset(token)


@pytest.fixture(autouse=True)
def event_queue():
    queue = asyncio.Queue()
    token = event_queue_var.set(queue)
    try:
        yield queue
    finally:
        event_queue_var.reset(token)
