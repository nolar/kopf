"""
Advanced modes of sleeping.
"""
import asyncio
from typing import Optional


async def sleep_or_wait(
        delay: float,
        event: asyncio.Event,
) -> Optional[float]:
    """
    Measure the sleep time: either until the timeout, or until the event is set.

    Returns the number of seconds left to sleep, or ``None`` if the sleep was
    not interrupted and reached its specified delay (an equivalent of ``0``).
    In theory, the result can be ``0`` if the sleep was interrupted precisely
    the last moment before timing out; this is unlikely to happen though.
    """
    loop = asyncio.get_running_loop()
    try:
        start_time = loop.time()
        await asyncio.wait_for(event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return None  # interruptable sleep is over: uninterrupted.
    else:
        end_time = loop.time()
        duration = end_time - start_time
        return max(0, delay - duration)
