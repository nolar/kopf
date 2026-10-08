import asyncio

from kopf._cogs.aiokits.aioenums import FlagSetter


# The test suite turns warnings into errors, so an unawaited coroutine fails the test.
async def test_async_waiting_cancelled_before_started_leaves_no_unawaited_coroutines():
    setter = FlagSetter()

    async def daemon() -> None:
        await setter.async_waiter.wait(2)

    task = asyncio.create_task(daemon())
    await asyncio.sleep(0)  # the daemon starts waiting, but the time-limited waiter does not
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
