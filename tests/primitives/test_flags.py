import asyncio
import concurrent.futures
import threading

import pytest

from kopf.structs.primitives import wait_flag, raise_flag


@pytest.fixture(params=[
    pytest.param(asyncio.Event, id='asyncio-event'),
    pytest.param(asyncio.Future, id='asyncio-future'),
    pytest.param(threading.Event, id='threading-event'),
    pytest.param(concurrent.futures.Future, id='concurrent-future'),
])
async def flag(request):
    """
    Fulfil the flag's waiting expectations, so that a threaded call can finish.
    Otherwise, the test runs freeze at exit while waiting for the executors.
    """
    flag = request.param()
    try:
        yield flag
    finally:
        if hasattr(flag, 'cancel'):
            flag.cancel()
        if hasattr(flag, 'set'):
            flag.set()


async def test_raising_of_unsupported_raises_an_error():
    with pytest.raises(TypeError):
        await raise_flag(object())


async def test_raising_of_none_does_nothing():
    await raise_flag(None)


async def test_raising_of_asyncio_event():
    event = asyncio.Event()
    await raise_flag(event)
    assert event.is_set()


async def test_raising_of_asyncio_future():
    future = asyncio.Future()
    await raise_flag(future)
    assert future.done()


async def test_raising_of_threading_event():
    event = threading.Event()
    await raise_flag(event)
    assert event.is_set()


async def test_raising_of_concurrent_future():
    future = concurrent.futures.Future()
    await raise_flag(future)
    assert future.done()


async def test_waiting_of_unsupported_raises_an_error():
    with pytest.raises(TypeError):
        await wait_flag(object())

async def test_waiting_of_none_does_nothing():
    await wait_flag(None)


async def test_waiting_for_unraised_times_out(flag, timer):
    with timer:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(wait_flag(flag), timeout=0.1)
    assert timer.seconds >= 0.1


async def test_waiting_for_preraised_is_instant(flag, timer):
    await raise_flag(flag)  # tested separately above
    with timer:
        await asyncio.wait_for(wait_flag(flag), timeout=1.0)
    assert timer.seconds < 0.5  # near-instant, plus code overhead


async def test_waiting_for_raised_during_the_wait(flag, timer):

    async def raise_delayed(delay: float) -> None:
        await asyncio.sleep(delay)
        await raise_flag(flag)  # tested separately above

    asyncio.create_task(raise_delayed(0.2))
    with timer:
        await asyncio.wait_for(wait_flag(flag), timeout=1.0)
    assert 0.2 <= timer.seconds < 0.5  # near-instant once raised
