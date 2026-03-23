import asyncio
import contextlib
import signal

from kopf._core.reactor.running import stop_flag_checker


async def test_stop_via_stop_flag_with_none_result(assert_logs, looptime):
    signal_flag: asyncio.Future[object] = asyncio.Future()
    stop_flag: asyncio.Future[object] = asyncio.Future()

    task = asyncio.create_task(stop_flag_checker(
        signal_flag=signal_flag,
        stop_flag=stop_flag,
    ))

    await asyncio.sleep(3)
    stop_flag.set_result(None)
    await task

    assert_logs(["Stop-flag is raised. Operator is stopping."])
    assert looptime == 3


async def test_stop_via_stop_flag_with_custom_value(assert_logs, looptime):
    signal_flag: asyncio.Future[object] = asyncio.Future()
    stop_flag: asyncio.Future[object] = asyncio.Future()

    task = asyncio.create_task(stop_flag_checker(
        signal_flag=signal_flag,
        stop_flag=stop_flag,
    ))

    await asyncio.sleep(3)
    stop_flag.set_result('custom-reason')
    await task

    assert_logs(["Stop-flag is set to 'custom-reason'. Operator is stopping."])
    assert looptime == 3


async def test_stop_via_asyncio_event(assert_logs, looptime):
    signal_flag: asyncio.Future[object] = asyncio.Future()
    stop_flag = asyncio.Event()

    task = asyncio.create_task(stop_flag_checker(
        signal_flag=signal_flag,
        stop_flag=stop_flag,
    ))

    await asyncio.sleep(3)
    stop_flag.set()
    await task

    assert_logs(["Stop-flag is set to True. Operator is stopping."])
    assert looptime == 3


async def test_stop_via_signal_flag(assert_logs, looptime):
    signal_flag: asyncio.Future[object] = asyncio.Future()

    task = asyncio.create_task(stop_flag_checker(
        signal_flag=signal_flag,
        stop_flag=None,
    ))

    await asyncio.sleep(3)
    signal_flag.set_result(signal.SIGINT)
    await task

    assert_logs(["Signal SIGINT is received. Operator is stopping."])
    assert looptime == 3


async def test_cancellation_exits_silently(assert_logs, looptime):
    signal_flag: asyncio.Future[object] = asyncio.Future()

    task = asyncio.create_task(stop_flag_checker(
        signal_flag=signal_flag,
        stop_flag=None,
    ))

    await asyncio.sleep(3)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert_logs(prohibited=["Operator is stopping"])
    assert looptime == 3
