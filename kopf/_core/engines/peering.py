"""
Peer monitoring: knowing which other operators are running,
and exchanging the basic signals with them.

The main use-case is to suppress all deployed operators when a developer starts
a dev-/debug-mode operator for the same cluster on their workstation ---
to avoid any double-processing.

See also: ``kopf freeze`` & ``kopf resume`` CLI commands for the same purpose.

WARNING: There are **NO** per-object locks between the operators,
so only one operator should be functional for the cluster,
i.e. only one with the highest priority running.
If the operator sees the violations of this constraint, it will print a warning
pointing to another same-priority operator, but will continue to function.

The "signals" exchanged are only the keep-alives from the operator being alive,
and detection of other operators hard termination
(by timeout rather than by clear exit).

Peer monitoring covers both the in-cluster operators running,
and the dev-mode operators running in the dev workstations.

For this, special CRDs ``kind: ClusterKopfPeering`` & ``kind: KopfPeering``
should be registered in the cluster, and their ``status`` field is used
by all the operators to sync their keep-alive info.

The namespace-bound operators (e.g. ``--namespace=…``) report their individual
namespaces are part of the payload, can see all other cluster and namespaced
operators (even from the different namespaces), and behave accordingly.

The CRD is not applied automatically, so you have to deploy it explicitly.
To disable the peers monitoring, use the ``--standalone`` CLI option.
"""

import asyncio
import datetime
import getpass
import logging
import os
import random
from collections.abc import Iterable
from typing import Any, NewType, NoReturn, cast

import aiohttp
import iso8601

from kopf._cogs.aiokits import aiotasks, aiotime, aiotoggles
from kopf._cogs.clients import errors, patching
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import hostnames
from kopf._cogs.structs import bodies, patches, references

logger = logging.getLogger(__name__)

# The temporary failures of the API or of the network. Peering is best-effort: the keep-alives
# and the clean-ups are repeated later, so these errors must not stop the operator. Authentication
# & authorization errors are not here on purpose: they escalate, so that the operator is restarted
# as a whole with fresh credentials & permissions.
TEMPORARY_ERRORS = (
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    errors.APIServerError,
    errors.APITooManyRequestsError,
    TimeoutError,
)
KEEPALIVE_PAUSE_SAFETY_MARGIN = 10.0


class _PeeringObjectNotFoundError(Exception):
    """Raised internally when mandatory peering has no object to update."""


KEEPALIVE_ERRORS = TEMPORARY_ERRORS + (_PeeringObjectNotFoundError,)


def get_safety_margin(lifetime: float) -> float:
    """ How long before the record expires an operator pauses if it cannot renew the record. """
    return min(KEEPALIVE_PAUSE_SAFETY_MARGIN, max(1, int(lifetime)) / 5)

Identity = NewType('Identity', str)


# The class used to represent a peer in the parsed peers list (for convenience).
# The extra fields are for easier calculation when and if the peer is dead to the moment.
class Peer:

    def __init__(
            self,
            *,
            identity: Identity,
            priority: int = 0,
            lifetime: int = 60,
            lastseen: str | None = None,
            **_: Any,  # for the forward-compatibility with the new fields
    ):
        super().__init__()
        self.identity = identity
        self.priority = priority
        self.lifetime = datetime.timedelta(seconds=int(lifetime))
        self.lastseen = (iso8601.parse_date(lastseen) if lastseen is not None else
                         datetime.datetime.now(datetime.UTC))
        self.deadline = self.lastseen + self.lifetime
        self.is_dead = self.deadline <= datetime.datetime.now(datetime.UTC)

    def __repr__(self) -> str:
        clsname = self.__class__.__name__
        options = ", ".join(f"{key!s}={val!r}" for key, val in self.as_dict().items())
        return f"<{clsname} {self.identity}: {options}>"

    def as_dict(self) -> dict[str, Any]:
        # Only the non-calculated and non-identifying fields.
        return {
            'priority': int(self.priority),
            'lifetime': int(self.lifetime.total_seconds()),
            'lastseen': str(self.lastseen.isoformat()),
        }


async def process_peering_event(
        *,
        raw_event: bodies.RawEvent,
        namespace: references.Namespace,
        resource: references.Resource,
        identity: Identity,
        settings: configuration.OperatorSettings,
        autoclean: bool = True,
        stream_pressure: asyncio.Event | None = None,  # None for tests
        conflicts_found: aiotoggles.Toggle | None = None,  # None for tests & observation
        # Must be accepted whether used or not -- as passed by watcher()/worker().
        # NB: processing_mode is queueing.ProcessingMode, but it causes a module-level violation
        # (a lower level engine imports the higher level reactor), but we simply do not care here.
        # Otherwise, move the enum into cogs or intents — both are lower levels than engines.
        processing_mode: Any | None = None,  # None means doing everything
) -> None:
    """
    Handle a single update of the peers by us or by other operators.

    When an operator with a higher priority appears, pause this operator.
    When conflicting operators disappear or become presumably dead,
    resume the event handling in the current operator (un-pause it).
    """
    body: bodies.RawBody = raw_event['object']
    meta: bodies.RawMeta = raw_event['object']['metadata']

    # Silently ignore the peering objects which are not ours to worry.
    if meta.get('name') != settings.peering.name:
        return

    # A mandatory peering object disappearing is itself a reason to pause immediately, without
    # waiting for the next keep-alive. Optional auto-detected peering falls back to standalone.
    if raw_event['type'] == 'DELETED':
        if conflicts_found is not None:
            await conflicts_found.turn_to(settings.peering.mandatory)
        return

    # Find if we are still the highest priority operator.
    # Never treat our own stale record as a dead peer to clean: after a process pause or a
    # network outage, a delayed watch event can arrive right after our own fresh keep-alive
    # and would otherwise purge the record that we have just written.
    pairs = cast(dict[str, dict[str, Any]], body.get('status', {}))
    peers = [Peer(identity=Identity(opid), **opinfo) for opid, opinfo in pairs.items()]
    dead_peers = [peer for peer in peers if peer.is_dead and peer.identity != identity]
    live_peers = [peer for peer in peers if not peer.is_dead and peer.identity != identity]
    prio_peers = [peer for peer in live_peers if peer.priority > settings.peering.priority]
    same_peers = [peer for peer in live_peers if peer.priority == settings.peering.priority]

    where = f"in {namespace!r}" if namespace else "cluster-wide"
    if autoclean and dead_peers:
        try:
            await clean(peers=dead_peers, settings=settings, resource=resource, namespace=namespace)
        except TEMPORARY_ERRORS as e:
            logger.warning(f"Cleaning the dead peers {where} failed; will retry later: {e!r}")

    if conflicts_found is None:
        pass

    elif prio_peers:
        if conflicts_found.is_off():
            logger.info(f"Pausing operations in favour of {prio_peers}.")
            await conflicts_found.turn_to(True)

    elif same_peers and not settings.watching.shard_selectors:
        logger.warning(f"Possibly conflicting operators with the same priority: {same_peers}.")
        if conflicts_found.is_off():
            logger.warning(f"Pausing all operators, including self: {peers}")
            await conflicts_found.turn_to(True)

    else:
        # Optional peering can temporarily run standalone while its object is absent. If the object
        # reappears without our record, publish it now, so that other operators can see us without
        # waiting for the next keep-alive. The keep-alive retries it anyway, so do not wait long.
        if not settings.peering.mandatory and identity not in pairs:
            try:
                async with asyncio.timeout(get_safety_margin(settings.peering.lifetime)):
                    await touch(
                        identity=identity,
                        settings=settings,
                        resource=resource,
                        namespace=namespace,
                    )
            except TEMPORARY_ERRORS as e:
                logger.warning(f"Restoring the peering record {where} failed; "
                               f"will retry with the next keep-alive: {e!r}")
        if conflicts_found.is_on():
            logger.info(f"Resuming operations after the pause. Conflicting operators with the same priority are gone.")
            await conflicts_found.turn_to(False)

    # Either wait for external updates (and exit when they arrive), or until the blocking peers
    # are expected to expire, and force the immediate re-evaluation by a certain change of self.
    # This incurs an extra PATCH request besides usual keepalives, but in the complete silence
    # from other peers that existed a moment earlier, this should not be a problem.
    now = datetime.datetime.now(datetime.UTC)
    delays = [(peer.deadline - now).total_seconds() for peer in same_peers + prio_peers]
    unslept = await aiotime.sleep(delays, wakeup=stream_pressure)
    if unslept is None and delays:
        try:
            await touch(
                identity=identity,
                settings=settings,
                resource=resource,
                namespace=namespace,
            )
        except TEMPORARY_ERRORS as e:
            logger.warning(f"Touching the expired peers {where} failed; will retry later: {e!r}")


async def keepalive(
        *,
        namespace: references.Namespace,
        resource: references.Resource,
        identity: Identity,
        settings: configuration.OperatorSettings,
        keepalive_failed: aiotoggles.Toggle,
        conflicts_found: aiotoggles.Toggle | None = None,
) -> NoReturn:
    """
    An ever-running coroutine to regularly send our own keep-alive status for the peers.
    """
    error_backoff: float = 0
    record_deadline: float | None = None
    remove_record = True
    where = f"in {namespace!r}" if namespace else "cluster-wide"

    # Mandatory peering does not process events until our record is published. Optional peering
    # works standalone until then, as it does when the peering object is absent.
    if settings.peering.mandatory:
        await keepalive_failed.turn_to(True)
    try:
        while True:
            attempt_started = asyncio.get_running_loop().time()

            # Round the same way as the record does, so that the deadline matches the peers' view.
            # A non-positive lifetime makes the record dead on arrival: peering would do nothing.
            lifetime = int(settings.peering.lifetime)
            if lifetime <= 0:
                remove_record = False
                raise ValueError(f"settings.peering.lifetime must be at least 1 second, "
                                 f"got {settings.peering.lifetime!r}.")

            # The margin is at most 20% of the lifetime, so that the pause deadline (80% or later)
            # always comes after the renewal time (60% or earlier) even for very short lifetimes.
            # Once the record has passed that deadline during an outage, give each retry attempt
            # the same bounded window instead of an already-expired timestamp.
            safety_margin = get_safety_margin(lifetime)
            pause_at = attempt_started + lifetime - safety_margin
            if record_deadline is not None and record_deadline - safety_margin > attempt_started:
                pause_at = record_deadline - safety_margin
            try:
                # The request's own retries and timeouts can outlast the record. Bound each
                # attempt, so that a hanging request pauses event intake before the record expires
                # and is retried in place.
                async with asyncio.timeout_at(pause_at):
                    record_stored = await touch(
                        identity=identity,
                        settings=settings,
                        resource=resource,
                        namespace=namespace,
                    )
                    if record_stored is False and settings.peering.mandatory:
                        raise _PeeringObjectNotFoundError(
                            f"Peering object {settings.peering.name!r} {where} does not exist.")
            except KEEPALIVE_ERRORS as e:
                await keepalive_failed.turn_to(True)
                retry_after = e.retry_after if isinstance(e, errors.APIError) else None
                error_backoff, backoff = errors.get_reconnect_backoff(
                    base=settings.watching.reconnect_backoff,
                    previous=error_backoff,
                    retry_after=retry_after,
                    enforce_retry_after=settings.networking.enforce_retry_after,
                )
                logger.warning(
                    f"Keep-alive {where} failed; pausing and retrying in {backoff}s: {e!r}")
                await asyncio.sleep(backoff)
                continue

            error_backoff = 0
            record_deadline = attempt_started + lifetime if record_stored is not False else None
            if (record_stored is False and not settings.peering.mandatory
                    and conflicts_found is not None):
                await conflicts_found.turn_to(False)
            await keepalive_failed.turn_to(False)

            # Renew at 50-60% of the lifetime. This leaves enough of the last valid record
            # to pause event intake and retry in place before other operators can take over.
            # The percentage jitter distributes keep-alives over time. Measure from the record's
            # timestamp, not from request completion: a slow successful request must not postpone
            # the next renewal beyond the record's deadline.
            renew_at = attempt_started + lifetime * random.randint(50, 60) / 100
            await asyncio.sleep(max(0, renew_at - asyncio.get_running_loop().time()))
    finally:
        if remove_record:
            try:
                await asyncio.shield(touch(
                    identity=identity,
                    settings=settings,
                    resource=resource,
                    namespace=namespace,
                    lifetime=0,
                ))
            except asyncio.CancelledError:
                pass  # cancellations are treated as normal exiting
            except Exception:
                logger.exception(f"Couldn't remove self from the peering. Ignoring.")


async def touch(
        *,
        identity: Identity,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        lifetime: int | None = None,
) -> bool:
    name = settings.peering.name
    peer = Peer(
        identity=identity,
        priority=settings.peering.priority,
        lifetime=settings.peering.lifetime if lifetime is None else lifetime,
    )

    patch = patches.Patch()
    patch |= {'status': {identity: None if peer.is_dead else peer.as_dict()}}
    rsp, remaining_patch = await patching.patch_obj(
        settings=settings,
        resource=resource,
        namespace=namespace,
        name=name,
        patch=patch,
        logger=logger,
        silent=True,
    )

    if not settings.peering.stealth or rsp is None:
        where = f"in {namespace!r}" if namespace else "cluster-wide"
        result = "not found" if rsp is None else "ok"
        logger.debug(f"Keep-alive in {name!r} {where}: {result}.")
    return rsp is not None


async def clean(
        *,
        peers: Iterable[Peer],
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
) -> None:
    name = settings.peering.name
    patch = patches.Patch()
    patch |= {'status': {peer.identity: None for peer in peers}}
    await patching.patch_obj(
        settings=settings,
        resource=resource,
        namespace=namespace,
        name=name,
        patch=patch,
        logger=logger,
        silent=True,
    )


def detect_own_id(*, manual: bool) -> Identity:
    """
    Detect or generate the id for ourselves, i.e. the executing operator.

    It is constructed easy to detect in which pod it is running
    (if in the cluster), or who runs the operator (if not in the cluster,
    i.e. in the dev-mode), and how long ago was it started.

    The pod id can be specified by:

    .. code-block:: yaml

        env:
        - name: POD_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name

    Used in the :mod:`kopf._core.reactor.queueing` when the reactor starts,
    but is kept here, close to the rest of the peering logic.
    """

    pod = os.environ.get('POD_ID', None)
    if pod is not None:
        return Identity(pod)

    user = getpass.getuser()
    host = hostnames.get_descriptive_hostname()
    now = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S")
    rnd = ''.join(random.choices('abcdefhijklmnopqrstuvwxyz0123456789', k=3))
    return Identity(f'{user}@{host}' if manual else f'{user}@{host}/{now}/{rnd}')


def guess_selectors(settings: configuration.OperatorSettings) -> Iterable[references.Selector]:
    if settings.peering.standalone:
        return []
    elif settings.peering.clusterwide:
        return [references.CLUSTER_PEERINGS_K, references.CLUSTER_PEERINGS_Z]
    elif settings.peering.namespaced:
        return [references.NAMESPACED_PEERINGS_K, references.NAMESPACED_PEERINGS_Z]
    else:
        raise TypeError("Unidentified peering mode (none of standalone/cluster/namespaced).")


async def touch_command(
        *,
        lifetime: int | None,
        insights: references.Insights,
        identity: Identity,
        settings: configuration.OperatorSettings,
) -> None:

    await asyncio.wait({
        asyncio.create_task(insights.ready_namespaces.wait()),
        asyncio.create_task(insights.ready_resources.wait()),
    })

    selectors = guess_selectors(settings=settings)
    resources = [insights.backbone[s] for s in selectors if s in insights.backbone]
    if not resources:
        raise RuntimeError(f"Cannot find the peering resource for {selectors}.")

    await aiotasks.wait({
        aiotasks.create_guarded_task(
            name="peering command", finishable=True, logger=logger,
            coro=touch(
                namespace=namespace,
                resource=resource,
                identity=identity,
                settings=settings,
                lifetime=lifetime),
        )
        for namespace in insights.namespaces
        for resource in resources
    })
