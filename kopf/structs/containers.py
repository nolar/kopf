"""
A in-memory storage of arbitrary information per resource/object.

The information is stored strictly in-memory and is not persistent.
On the operator restart, all the memories are lost.

It is used internally to track allocated system resources for each Kubernetes
object, even if that object does not show up in the event streams for long time.
"""
import dataclasses
from typing import MutableMapping, Dict, Any

from kopf.structs import bodies


class ObjectDict(Dict[Any, Any]):
    """ A container to hold arbitrary keys-fields assigned by the users. """

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value

    def __delattr__(self, key: str) -> None:
        try:
            del self[key]
        except KeyError as e:
            raise AttributeError(str(e))

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(str(e))


@dataclasses.dataclass(frozen=False)
class ResourceMemory:
    """ A system memo about a single resource/object. Usually stored in `Memories`. """

    # For arbitrary user data to be stored in memory, passed as `memo` to all the handlers.
    user_data: ObjectDict = dataclasses.field(default_factory=ObjectDict)

    # For resuming handlers tracking and deciding on should they be called or not.
    noticed_by_listing: bool = False
    fully_handled_once: bool = False


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

    async def recall(
            self,
            body: bodies.Body,
            *,
            noticed_by_listing: bool = False,
    ) -> ResourceMemory:
        """
        Either find a resource's memory, or create and remember a new one.
        """
        key = self._build_key(body)
        if key not in self._items:
            memory = ResourceMemory(noticed_by_listing=noticed_by_listing)
            self._items[key] = memory
        return self._items[key]

    async def forget(self, body: bodies.Body) -> None:
        """
        Forget the resource's memory if it exists; or ignore if it does not.
        """
        key = self._build_key(body)
        if key in self._items:
            del self._items[key]

    def _build_key(
            self,
            body: bodies.Body,
    ) -> str:
        """
        Construct an immutable persistent key of a resource.

        Generally, a uid is sufficient, as it is unique within the cluster.
        But it can be e.g. plural/namespace/name triplet, or anything else,
        even of different types (as long as it satisfies the type checkers).

        But it must be consistent within a single process lifetime.
        """
        return body.get('metadata', {}).get('uid') or ''
