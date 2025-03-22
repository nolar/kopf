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
import datetime
from typing import Collection, Optional

from kopf._cogs.aiokits import aiotime
from kopf._cogs.clients import patching
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, dicts, diffs, patches, references
from kopf._core.actions import loggers

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
    await patch_and_check(
        settings=settings,
        resource=resource,
        logger=logger,
        patch=patch,
        body=body,
    )

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
            unslept_delay = await aiotime.sleep(limit, wakeup=stream_pressure)
        elif delay > 0:
            logger.debug(f"Sleeping for {delay} seconds for the delayed handlers.")
            unslept_delay = await aiotime.sleep(delay, wakeup=stream_pressure)
        else:
            unslept_delay = None  # no need to sleep? means: slept in full.

        # Exclude cases when touching immediately after patching (including: ``delay == 0``).
        if patch and not delay:
            pass
        elif unslept_delay is not None:
            logger.debug(f"Sleeping was interrupted by new changes, {unslept_delay} seconds left.")
        else:
            # Any unique always-changing value will work; not necessary a timestamp.
            value = datetime.datetime.now(datetime.timezone.utc).isoformat()
            touch = patches.Patch()
            settings.persistence.progress_storage.touch(body=body, patch=touch, value=value)
            await patch_and_check(
                settings=settings,
                resource=resource,
                logger=logger,
                patch=touch,
                body=body,
            )
    elif not patch:  # no patch/touch and no delay
        applied = True
    return applied


async def patch_and_check(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        body: bodies.Body,
        patch: patches.Patch,
        logger: typedefs.Logger,
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
            settings=settings,
            resource=resource,
            namespace=body.metadata.namespace,
            name=body.metadata.name,
            patch=patch,
            logger=logger,
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
