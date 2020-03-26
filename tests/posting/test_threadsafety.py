"""
A little note on how these tests work:

Almost all asyncio objects are not thread-safe, as per the official doc.
This includes `asyncio.Queue`. This queue is used for k8s-event posting.

K8s-events are posted via ``kopf.event()`` and similar calls,
and also via ``logger.info()`` for per-object logging messages.

The calls originate from various threads:

* Main thread where the framework's event-loop runs.
* Thread-pool executors for sync handlers.
* Explicitly started threads for object monitoring
  (e.g. from ``@kopf.on.resume`` handlers).

In the main thread, there is an event-loop running, and it has an asyncio task
to get the k8s-event events from the queue and to post them to the K8s API.

In the non-thread-safe mode, putting an object via `queue.put_nowait()``
does **NOT** wake up the pending ``queue.get()`` in the `poster` task
until something happens on the event-loop (not necessary on the queue).

In the thread-safe mode, putting an an object via `queue.put()``
(which is a coroutine and must be executed in the loop)
wakes the pending ``queue.get()`` call immediately.

These tests ensure that the thread-safe calls are used for k8s-event posting
by artificially reproducing the described situation. The delayed no-op task
(awakener) is used to wake up the event-loop after some time if the k8s-event
posting is not thread-safe. Otherwise, it wakes up on ``queue.get()`` instantly.

If thread safety is not ensured, the operators get sporadic errors regarding
thread-unsafe calls, which are difficult to catch and reproduce.
"""

import asyncio
import contextvars
import functools
import threading
import time

import pytest

from kopf import event

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}


@pytest.fixture()
def awakener(event_loop):
    handles = []

    def noop():
        pass

    def awaken_fn(delay, fn=noop):
        handle = event_loop.call_later(delay, fn)
        handles.append(handle)

    try:
        yield awaken_fn
    finally:
        for handle in handles:
            handle.cancel()


@pytest.fixture()
def threader():
    threads = []

    def start_fn(delay, fn):
        def thread_fn():
            time.sleep(delay)
            fn()

        target = functools.partial(contextvars.copy_context().run, thread_fn)
        thread = threading.Thread(target=target)
        thread.start()
        threads.append(thread)

    try:
        yield start_fn
    finally:
        for thread in threads:
            thread.join()


async def test_nonthreadsafe_indeed_fails(timer, awakener, threader, event_queue, event_queue_loop):

    def thread_fn():
        event_queue.put_nowait(object())

    awakener(0.7)
    threader(0.3, thread_fn)

    with timer:
        await event_queue.get()

    assert 0.6 <= timer.seconds <= 0.8


async def test_threadsafe_indeed_works(timer, awakener, threader, event_queue, event_queue_loop):

    def thread_fn():
        asyncio.run_coroutine_threadsafe(event_queue.put(object()), loop=event_queue_loop)

    awakener(0.7)
    threader(0.3, thread_fn)

    with timer:
        await event_queue.get()

    assert 0.2 <= timer.seconds <= 0.4


async def test_queueing_is_threadsafe(timer, awakener, threader, event_queue, event_queue_loop,
                                      settings_via_contextvar):

    def thread_fn():
        event(OBJ1, type='type1', reason='reason1', message='message1')

    awakener(0.7)
    threader(0.3, thread_fn)

    with timer:
        await event_queue.get()

    assert 0.2 <= timer.seconds <= 0.4
