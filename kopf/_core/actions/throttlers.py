import asyncio
import contextlib
import dataclasses
import time
from typing import AsyncGenerator, Iterable, Iterator, Optional, Tuple, Type, Union

from kopf._cogs.aiokits import aiotime
from kopf._cogs.helpers import typedefs


@dataclasses.dataclass(frozen=False)
class Throttler:
    """ A state of throttling for one specific purpose (there can be a few). """
    source_of_delays: Optional[Iterator[float]] = None
    last_used_delay: Optional[float] = None
    active_until: Optional[float] = None  # internal clock


@contextlib.asynccontextmanager
async def throttled(
        *,
        throttler: Throttler,
        delays: Iterable[float],
        wakeup: Optional[asyncio.Event] = None,
        logger: typedefs.Logger,
        errors: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
) -> AsyncGenerator[bool, None]:
    """
    A helper to throttle any arbitrary operation.
    """

    # The 1st sleep: if throttling is already active, but was interrupted by a queue replenishment.
    # It is needed to properly process the latest known event after the successful sleep.
    if throttler.active_until is not None:
        remaining_time = throttler.active_until - time.monotonic()
        unslept_time = await aiotime.sleep(remaining_time, wakeup=wakeup)
        if unslept_time is None:
            logger.info("Throttling is over. Switching back to normal operations.")
            throttler.active_until = None

    # Run only if throttling either is not active initially, or has just finished sleeping.
    should_run = throttler.active_until is None
    try:
        yield should_run

    except Exception as e:

        # If it is not an error-of-interest, escalate normally. BaseExceptions are escalated always.
        if not isinstance(e, errors):
            raise

        # If the code does not follow the recommendation to not run, escalate.
        if not should_run:
            raise

        # Activate throttling if not yet active, or reuse the active sequence of delays.
        if throttler.source_of_delays is None:
            throttler.source_of_delays = iter(delays)

        # Choose a delay. If there are none, avoid throttling at all.
        delay = next(throttler.source_of_delays, throttler.last_used_delay)
        if delay is not None:
            throttler.last_used_delay = delay
            throttler.active_until = time.monotonic() + delay
            logger.exception(f"Throttling for {delay} seconds due to an unexpected error: {e!r}")

    else:
        # Reset the throttling. Release the iterator to keep the memory free during normal run.
        if should_run:
            throttler.source_of_delays = throttler.last_used_delay = None

    # The 2nd sleep: if throttling has been just activated (i.e. there was a fresh error).
    # It is needed to have better logging/sleeping without workers exiting for "no events".
    if throttler.active_until is not None and should_run:
        remaining_time = throttler.active_until - time.monotonic()
        unslept_time = await aiotime.sleep(remaining_time, wakeup=wakeup)
        if unslept_time is None:
            throttler.active_until = None
            logger.info("Throttling is over. Switching back to normal operations.")
