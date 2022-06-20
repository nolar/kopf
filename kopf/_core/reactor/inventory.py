"""
An internal in-memory storage of structured records about resource objects.

Each object gets at most one record in the inventory of an operator's memories.
The information is stored strictly in-memory and is not persistent.
On the operator restart, all the memories are lost.
The information is not exposed to the operator developers (except for memos).

It is used internally to track allocated system resources for each Kubernetes
object, even if that object does not show up in the event streams for long time.

In the future, additional never-ending tasks can be running to maintain
the operator's memories and inventory and garbage-collect all outdated records.

The inventory memories are data structures, but they are a part of the reactor
because they store specialised data structures of specialised reactor modules
(e.g. daemons, admission, indexing, etc). For cohesion, these data structures
must be kept together with their owning modules rather than mirrored in structs.
"""
import copy
import dataclasses
from typing import Iterator, MutableMapping, Optional

from kopf._cogs.structs import bodies, ephemera
from kopf._core.actions import throttlers
from kopf._core.engines import admission, daemons, indexing


@dataclasses.dataclass(frozen=False)
class ResourceMemory:
    """ A system memo about a single resource/object. Usually stored in `Memories`. """
    memo: ephemera.AnyMemo = dataclasses.field(default_factory=lambda: ephemera.AnyMemo(ephemera.Memo()))
    error_throttler: throttlers.Throttler = dataclasses.field(default_factory=throttlers.Throttler)
    indexing_memory: indexing.IndexingMemory = dataclasses.field(default_factory=indexing.IndexingMemory)
    daemons_memory: daemons.DaemonsMemory = dataclasses.field(default_factory=daemons.DaemonsMemory)

    # For resuming handlers tracking and deciding on should they be called or not.
    noticed_by_listing: bool = False
    fully_handled_once: bool = False


class ResourceMemories(admission.MemoGetter, daemons.DaemonsMemoriesIterator):
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
        yield from self._items.values()

    def iter_all_daemon_memories(self) -> Iterator[daemons.DaemonsMemory]:
        for memory in self._items.values():
            yield memory.daemons_memory

    async def recall_memo(
            self,
            raw_body: bodies.RawBody,
            *,
            memobase: Optional[ephemera.AnyMemo] = None,
            ephemeral: bool = False,
    ) -> ephemera.AnyMemo:
        memory = await self.recall(raw_body=raw_body, memobase=memobase, ephemeral=ephemeral)
        return memory.memo

    async def recall(
            self,
            raw_body: bodies.RawBody,
            *,
            memobase: Optional[ephemera.AnyMemo] = None,
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
            if memobase is None:
                memory = ResourceMemory(noticed_by_listing=noticed_by_listing)
            else:
                memo = copy.copy(memobase)
                memory = ResourceMemory(noticed_by_listing=noticed_by_listing, memo=memo)
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
