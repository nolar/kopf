"""
Routines to apply effects accumulated during the reaction cycle.

Applying the effects is the last step in the reacting cycle:

* :mod:`queueing` streams the events to per-object processing tasks,
* :mod:`causation` detects what has happened based on the received events,
* :mod:`handling` decides how to react, and invokes the handlers,
* :mod:`effects` applies the effects accumulated during the handling.

The effects are accumulated both from the framework and from the operator's
handlers, and are generally of these three intermixed kinds:

* Patching the object with all the results & states persisted.
* Sleeping for the duration of known absence of activity (or until interrupted).
* Touching the object to trigger the next reaction cycle.

It is used from in :mod:`processing`, :mod:`actitivies`, and :mod:`daemons` --
all the modules, of which the reactor's core consists.
"""
import asyncio
import collections
import datetime
from typing import Collection, Optional, Union

from kopf.clients import patching
from kopf.engines import loggers
from kopf.structs import bodies, configuration, patches, primitives, resources

# How often to wake up from the long sleep, to show liveness in the logs.
WAITING_KEEPALIVE_INTERVAL = 10 * 60


async def apply(
        *,
        settings: configuration.OperatorSettings,
        resource: resources.Resource,
        body: bodies.Body,
        patch: patches.Patch,
        delays: Collection[float],
        logger: loggers.ObjectLogger,
        replenished: asyncio.Event,
) -> None:
    delay = min(delays) if delays else None

    # Delete dummies on occasion, but don't trigger special patching for them [discussable].
    if patch:  # TODO: LATER: and the dummies are there (without additional methods?)
        settings.persistence.progress_storage.touch(body=body, patch=patch, value=None)

    # Actually patch if it contained payload originally or after dummies removal.
    if patch:
        logger.debug("Patching with: %r", patch)
        await patching.patch_obj(resource=resource, patch=patch, body=body)

    # Sleep strictly after patching, never before -- to keep the status proper.
    # The patching above, if done, interrupts the sleep instantly, so we skip it at all.
    # Note: a zero-second or negative sleep is still a sleep, it will trigger a dummy patch.
    if delay and patch:
        logger.debug(f"Sleeping was skipped because of the patch, {delay} seconds left.")
    elif delay is None and not patch:
        logger.debug(f"Handling cycle is finished, waiting for new changes since now.")
    elif delay is not None:
        if delay > WAITING_KEEPALIVE_INTERVAL:
            limit = WAITING_KEEPALIVE_INTERVAL
            logger.debug(f"Sleeping for {delay} (capped {limit}) seconds for the delayed handlers.")
            unslept_delay = await sleep_or_wait(limit, replenished)
        elif delay > 0:
            logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
            unslept_delay = await sleep_or_wait(delay, replenished)
        else:
            unslept_delay = None  # no need to sleep? means: slept in full.

        # Exclude cases when touching immediately after patching (including: ``delay == 0``).
        if patch and not delay:
            pass
        elif unslept_delay is not None:
            logger.debug(f"Sleeping was interrupted by new changes, {unslept_delay} seconds left.")
        else:
            # Any unique always-changing value will work; not necessary a timestamp.
            value = datetime.datetime.utcnow().isoformat()
            touch_patch = patches.Patch()
            settings.persistence.progress_storage.touch(body=body, patch=touch_patch, value=value)
            if touch_patch:
                logger.debug("Provoking reaction with: %r", touch_patch)
                await patching.patch_obj(resource=resource, patch=touch_patch, body=body)


async def sleep_or_wait(
        delays: Union[None, float, Collection[Union[None, float]]],
        wakeup: Optional[Union[asyncio.Event, primitives.DaemonStopper]] = None,
) -> Optional[float]:
    """
    Measure the sleep time: either until the timeout, or until the event is set.

    Returns the number of seconds left to sleep, or ``None`` if the sleep was
    not interrupted and reached its specified delay (an equivalent of ``0``).
    In theory, the result can be ``0`` if the sleep was interrupted precisely
    the last moment before timing out; this is unlikely to happen though.
    """
    passed_delays = delays if isinstance(delays, collections.abc.Collection) else [delays]
    actual_delays = [delay for delay in passed_delays if delay is not None]
    minimal_delay = min(actual_delays) if actual_delays else 0

    awakening_event = (
        wakeup.async_event if isinstance(wakeup, primitives.DaemonStopper) else
        wakeup if wakeup is not None else
        asyncio.Event())

    loop = asyncio.get_running_loop()
    try:
        start_time = loop.time()
        await asyncio.wait_for(awakening_event.wait(), timeout=minimal_delay)
    except asyncio.TimeoutError:
        return None  # interruptable sleep is over: uninterrupted.
    else:
        end_time = loop.time()
        duration = end_time - start_time
        return max(0, minimal_delay - duration)
