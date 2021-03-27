"""
A in-memory storage of arbitrary information per resource/object.

The information is stored strictly in-memory and is not persistent.
On the operator restart, all the memories are lost.

It is used internally to track allocated system resources for each Kubernetes
object, even if that object does not show up in the event streams for long time.
"""
import copy
import dataclasses
import logging
import time
from typing import Dict, Iterator, MutableMapping, Optional, Set, Union

from kopf.storage import states
from kopf.structs import bodies, ephemera, handlers, ids, primitives
from kopf.utilities import aiotasks


@dataclasses.dataclass(frozen=True)
class Daemon:
    task: aiotasks.Task  # a guarding task of the daemon.
    logger: Union[logging.Logger, logging.LoggerAdapter]
    handler: handlers.ResourceSpawningHandler
    stopper: primitives.DaemonStopper  # a signaller for the termination and its reason.


@dataclasses.dataclass(frozen=False)
class Throttler:
    """ A state of throttling for one specific purpose (there can be a few). """
    source_of_delays: Optional[Iterator[float]] = None
    last_used_delay: Optional[float] = None
    active_until: Optional[float] = None  # internal clock


@dataclasses.dataclass(frozen=False)
class ResourceMemory:
    """ A system memo about a single resource/object. Usually stored in `Memories`. """

    # For arbitrary user data to be stored in memory, passed as `memo` to all the handlers.
    memo: ephemera.AnyMemo = dataclasses.field(default_factory=ephemera.Memo)

    # For resuming handlers tracking and deciding on should they be called or not.
    noticed_by_listing: bool = False
    fully_handled_once: bool = False

    # Throttling for API errors (mostly from PATCHing) and for processing in general.
    error_throttler: Throttler = dataclasses.field(default_factory=Throttler)

    # For background and timed threads/tasks (invoked with the kwargs of the last-seen body).
    live_fresh_body: Optional[bodies.Body] = None
    idle_reset_time: float = dataclasses.field(default_factory=time.monotonic)
    forever_stopped: Set[ids.HandlerId] = dataclasses.field(default_factory=set)
    running_daemons: Dict[ids.HandlerId, Daemon] = dataclasses.field(default_factory=dict)

    # For indexing errors backoffs/retries/timeouts. It is None when successfully indexed.
    indexing_state: Optional[states.State] = None


class ResourceMemories:
    """
    A container of all memos about every existing resource in a single operator.

    Distinct operator tasks have their own memory containers, which
    do not overlap. This solves the problem if storing the per-resource
    entries in the global or context variables.

    The memos can store anything the resource handlers need to persist within
    a single process/operator lifetime, but not persisted on the resource.
    For example, the runtime system resources: flags, threads, tasks, etc.
    Or the scalar values, which have meaning only for this operator process.

    The container is relatively async-safe: one individual resource is always
    handled sequentially, never in parallel with itself (different resources
    are handled in parallel through), so the same key will not be added/deleted
    in the background during the operation, so the locking is not needed.
    """
    _items: MutableMapping[str, ResourceMemory]

    def __init__(self) -> None:
        super().__init__()
        self._items = {}

    def iter_all_memories(self) -> Iterator[ResourceMemory]:
        for memory in self._items.values():
            yield memory

    async def recall(
            self,
            raw_body: bodies.RawBody,
            *,
            memo: Optional[ephemera.AnyMemo] = None,
            noticed_by_listing: bool = False,
            ephemeral: bool = False,
    ) -> ResourceMemory:
        """
        Either find a resource's memory, or create and remember a new one.

        Keep the last-seen body up to date for all the handlers.

        Ephemeral memos are not remembered now
        (later: will be remembered for short time, and then garbage-collected).
        They are used by admission webhooks before the resource is created --
        to not waste RAM for what might never exist. The persistent memo
        will be created *after* the resource creation really happens.
        """
        key = self._build_key(raw_body)
        if key in self._items:
            memory = self._items[key]
        else:
            if memo is None:
                memory = ResourceMemory(noticed_by_listing=noticed_by_listing)
            else:
                memory = ResourceMemory(noticed_by_listing=noticed_by_listing, memo=copy.copy(memo))
            if not ephemeral:
                self._items[key] = memory
        return memory

    async def forget(
            self,
            raw_body: bodies.RawBody,
    ) -> None:
        """
        Forget the resource's memory if it exists; or ignore if it does not.
        """
        key = self._build_key(raw_body)
        if key in self._items:
            del self._items[key]

    def _build_key(
            self,
            raw_body: bodies.RawBody,
    ) -> str:
        """
        Construct an immutable persistent key of a resource.

        Generally, a uid is sufficient, as it is unique within the cluster.
        But it can be e.g. plural/namespace/name triplet, or anything else,
        even of different types (as long as it satisfies the type checkers).

        But it must be consistent within a single process lifetime.
        """
        return raw_body.get('metadata', {}).get('uid') or ''
