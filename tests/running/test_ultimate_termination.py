import asyncio
import signal
import threading

from kopf._core.reactor.running import ultimate_termination


async def test_no_kill_scheduled_when_stop_flag_is_set(settings, mocker, looptime):
    pthread_kill = mocker.patch.object(signal, 'pthread_kill')
    stop_flag = asyncio.Event()
    stop_flag.set()

    task = asyncio.create_task(ultimate_termination(
        settings=settings,
        stop_flag=stop_flag,
    ))

    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)

    assert task.done()
    assert not pthread_kill.called
    assert looptime == 0


async def test_no_kill_scheduled_when_timeout_is_none(settings, mocker, looptime):
    pthread_kill = mocker.patch.object(signal, 'pthread_kill')
    settings.process.ultimate_exiting_timeout = None

    task = asyncio.create_task(ultimate_termination(
        settings=settings,
        stop_flag=None,
    ))

    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)

    assert task.done()

    # Even after waiting, no kill should be scheduled.
    await asyncio.sleep(10)

    assert not pthread_kill.called
    assert looptime == 10


async def test_kill_scheduled_when_stop_flag_is_not_set(settings, mocker, looptime):
    pthread_kill = mocker.patch.object(signal, 'pthread_kill')
    mocker.patch.object(threading, 'get_ident', return_value=12345)
    settings.process.ultimate_exiting_timeout = 10

    task = asyncio.create_task(ultimate_termination(
        settings=settings,
        stop_flag=None,
    ))

    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)

    assert task.done()
    assert not pthread_kill.called

    # Fast-forward past the scheduled call_later timeout.
    await asyncio.sleep(10)

    assert pthread_kill.call_count == 1
    assert pthread_kill.call_args.args == (12345, signal.SIGKILL)
    assert looptime == 10
