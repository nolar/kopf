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
import contextlib
import datetime
import logging
import time
from typing import AsyncGenerator, Collection, Iterable, Optional, Tuple, Type, Union

from kopf.clients import patching
from kopf.engines import loggers
from kopf.structs import bodies, configuration, containers, dicts, \
                         diffs, patches, primitives, references

# How often to wake up from the long sleep, to show liveness in the logs.
WAITING_KEEPALIVE_INTERVAL = 10 * 60

# K8s-managed fields that are removed completely when patched to an empty list/dict.
KNOWN_INCONSISTENCIES = (
    dicts.parse_field('metadata.annotations'),
    dicts.parse_field('metadata.finalizers'),
    dicts.parse_field('metadata.labels'),
)


async def apply(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        body: bodies.Body,
        patch: patches.Patch,
        delays: Collection[float],
        logger: loggers.ObjectLogger,
        stream_pressure: Optional[asyncio.Event] = None,  # None for tests
) -> bool:
    delay = min(delays) if delays else None

    # Delete dummies on occasion, but don't trigger special patching for them [discussable].
    if patch:  # TODO: LATER: and the dummies are there (without additional methods?)
        settings.persistence.progress_storage.touch(body=body, patch=patch, value=None)

    # Actually patch if it was not empty originally or after the dummies removal.
    await patch_and_check(resource=resource, patch=patch, body=body, logger=logger)

    # Sleep strictly after patching, never before -- to keep the status proper.
    # The patching above, if done, interrupts the sleep instantly, so we skip it at all.
    # Note: a zero-second or negative sleep is still a sleep, it will trigger a dummy patch.
    applied = False
    if delay and patch:
        logger.debug(f"Sleeping was skipped because of the patch, {delay} seconds left.")
    elif delay is not None:
        if delay > WAITING_KEEPALIVE_INTERVAL:
            limit = WAITING_KEEPALIVE_INTERVAL
            logger.debug(f"Sleeping for {delay} (capped {limit}) seconds for the delayed handlers.")
            unslept_delay = await primitives.sleep_or_wait(limit, wakeup=stream_pressure)
        elif delay > 0:
            logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
            unslept_delay = await primitives.sleep_or_wait(delay, wakeup=stream_pressure)
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
            touch = patches.Patch()
            settings.persistence.progress_storage.touch(body=body, patch=touch, value=value)
            await patch_and_check(resource=resource, patch=touch, body=body, logger=logger)
    elif not patch:  # no patch/touch and no delay
        applied = True
    return applied


async def patch_and_check(
        *,
        resource: references.Resource,
        body: bodies.Body,
        patch: patches.Patch,
        logger: Union[logging.Logger, logging.LoggerAdapter],
) -> None:
    """
    Apply a patch and verify that it is applied correctly.

    The inconsistencies are checked only against what was in the patch.
    Other unexpected changes in the body are ignored, including the system
    fields, such as generations, resource versions, and other unrelated fields,
    such as other statuses, spec, labels, annotations, etc.

    Selected false-positive inconsistencies are explicitly ignored
    for K8s-managed fields, such as finalizers, labels or annotations:
    whenever an empty list/dict is stored, such fields are completely removed.
    For normal fields (e.g. in spec/status), an empty list/dict is still
    a value and is persisted in the object and matches with the patch.
    """
    if patch:
        logger.debug(f"Patching with: {patch!r}")
        resulting_body = await patching.patch_obj(
            resource=resource,
            namespace=body.metadata.namespace,
            name=body.metadata.name,
            patch=patch,
        )
        inconsistencies = diffs.diff(patch, resulting_body, scope=diffs.DiffScope.LEFT)
        inconsistencies = diffs.Diff(
            diffs.DiffItem(op, field, old, new)
            for op, field, old, new in inconsistencies
            if old or new or field not in KNOWN_INCONSISTENCIES
        )
        if resulting_body is None:
            logger.debug(f"Patching was skipped: the object does not exist anymore.")
        elif inconsistencies:
            logger.warning(f"Patching failed with inconsistencies: {inconsistencies}")


@contextlib.asynccontextmanager
async def throttled(
        *,
        throttler: containers.Throttler,
        delays: Iterable[float],
        wakeup: Optional[Union[asyncio.Event, primitives.DaemonStopper]] = None,
        logger: Union[logging.Logger, logging.LoggerAdapter],
        errors: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
) -> AsyncGenerator[bool, None]:
    """
    A helper to throttle any arbitrary operation.
    """

    # The 1st sleep: if throttling is already active, but was interrupted by a queue replenishment.
    # It is needed to properly process the latest known event after the successful sleep.
    if throttler.active_until is not None:
        remaining_time = throttler.active_until - time.monotonic()
        unslept_time = await primitives.sleep_or_wait(remaining_time, wakeup=wakeup)
        if unslept_time is None:
            logger.info("Throttling is over. Switching back to normal operations.")
            throttler.active_until = None

    # Run only if throttling either is not active initially, or has just finished sleeping.
    should_run = throttler.active_until is None
    try:
        yield should_run

    except asyncio.CancelledError:
        # CancelledError is a BaseException in 3.8 & 3.9, but a regular Exception in 3.7.
        # Behave as if it is a BaseException -- to enabled tests with async-timeout.
        raise

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
        unslept_time = await primitives.sleep_or_wait(remaining_time, wakeup=wakeup)
        if unslept_time is None:
            throttler.active_until = None
            logger.info("Throttling is over. Switching back to normal operations.")
