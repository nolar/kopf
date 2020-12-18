import asyncio
import concurrent.futures
import threading

import async_timeout
import pytest

from kopf.structs.primitives import check_flag, raise_flag, wait_flag


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


async def test_checking_of_unsupported_raises_an_error():
    with pytest.raises(TypeError):
        check_flag(object())


async def test_checking_of_none_is_none():
    result = check_flag(None)
    assert result is None


async def test_checking_of_asyncio_event_when_raised():
    event = asyncio.Event()
    event.set()
    result = check_flag(event)
    assert result is True


async def test_checking_of_asyncio_event_when_unset():
    event = asyncio.Event()
    event.clear()
    result = check_flag(event)
    assert result is False


async def test_checking_of_asyncio_future_when_set():
    future = asyncio.Future()
    future.set_result(None)
    result = check_flag(future)
    assert result is True


async def test_checking_of_asyncio_future_when_empty():
    future = asyncio.Future()
    result = check_flag(future)
    assert result is False


async def test_checking_of_threading_event_when_set():
    event = threading.Event()
    event.set()
    result = check_flag(event)
    assert result is True


async def test_checking_of_threading_event_when_unset():
    event = threading.Event()
    event.clear()
    result = check_flag(event)
    assert result is False


async def test_checking_of_concurrent_future_when_set():
    future = concurrent.futures.Future()
    future.set_result(None)
    result = check_flag(future)
    assert result is True


async def test_checking_of_concurrent_future_when_unset():
    future = concurrent.futures.Future()
    result = check_flag(future)
    assert result is False


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
    with pytest.raises(asyncio.TimeoutError):
        async with timer, async_timeout.timeout(0.1) as timeout:
            await wait_flag(flag)
    assert timer.seconds >= 0.1
    assert timeout.expired


async def test_waiting_for_preraised_is_instant(flag, timer):
    await raise_flag(flag)  # tested separately above
    async with timer, async_timeout.timeout(1.0) as timeout:
        await wait_flag(flag)
    assert timer.seconds < 0.5  # near-instant, plus code overhead
    assert not timeout.expired


async def test_waiting_for_raised_during_the_wait(flag, timer):

    async def raise_delayed(delay: float) -> None:
        await asyncio.sleep(delay)
        await raise_flag(flag)  # tested separately above

    asyncio.create_task(raise_delayed(0.2))
    async with timer, async_timeout.timeout(1.0) as timeout:
        await wait_flag(flag)
    assert 0.2 <= timer.seconds < 0.5  # near-instant once raised
    assert not timeout.expired
