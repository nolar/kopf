"""
All the structures needed for Kubernetes patching.

Currently, it is implemented via a JSON merge-patch (RFC 7386),
i.e. a simple dictionary with field overrides, and ``None`` for field deletions.

In the future, it can be extended to a standalone object, which exposes
a dict-like behaviour, and remembers the changes in order of their execution,
and then generates the JSON patch (RFC 6902).
"""
import collections.abc
import copy
from collections.abc import Callable, Iterable
from typing import Any, Literal, TypedDict, cast

import jsonpatch

from kopf._cogs.structs import bodies, dicts

JSONPatchOp = Literal["add", "replace", "remove", "test", "move", "copy"]


class JSONPatchItem(TypedDict, total=False):
    op: JSONPatchOp
    # from: str  # but it is a python keyword
    path: str
    value: Any | None


JSONPatch = list[JSONPatchItem]

# An arbitrary transformation function, which modifies the body in place.
PatchFn = Callable[[bodies.RawBody], None]


class MetaPatch(dicts.MutableMappingView[str, Any]):
    _labels: dicts.MutableMappingView[str, str | None]
    _annotations: dicts.MutableMappingView[str, str | None]

    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'metadata')
        self._labels = dicts.MutableMappingView(self, 'labels')
        self._annotations = dicts.MutableMappingView(self, 'annotations')

    @property
    def labels(self) -> dicts.MutableMappingView[str, str | None]:
        return self._labels

    @property
    def annotations(self) -> dicts.MutableMappingView[str, str | None]:
        return self._annotations


class SpecPatch(dicts.MutableMappingView[str, Any]):
    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'spec')


class StatusPatch(dicts.MutableMappingView[str, Any]):
    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'status')


# Event-handling structures, used internally in the framework and handlers only.
class Patch(dict[str, Any]):

    def __init__(
        self,
        src: collections.abc.MutableMapping[str, Any] | None = None,
        /,
        body: bodies.RawBody | None = None,
        fns: Iterable[PatchFn] = (),
    ) -> None:
        super().__init__(src or {})
        self._meta = MetaPatch(self)
        self._spec = SpecPatch(self)
        self._status = StatusPatch(self)
        self._original = body
        self._fns = (src.fns if isinstance(src, Patch) else []) + list(fns)

    def __repr__(self) -> str:
        texts: list[str] = []
        if list(self):  # any keys at all?
            texts += [super().__repr__()]
        if self.fns:
            texts += [f"fns={list(self.fns)!r}"]
        text = ", ".join(texts)
        return f"{type(self).__name__}({text})"

    def __bool__(self) -> bool:
        return len(self) > 0 or bool(self.fns)

    @property
    def fns(self) -> list[PatchFn]:
        return self._fns

    @property
    def metadata(self) -> MetaPatch:
        return self._meta

    @property
    def meta(self) -> MetaPatch:
        return self._meta

    @property
    def spec(self) -> SpecPatch:
        return self._spec

    @property
    def status(self) -> StatusPatch:
        return self._status

    def as_json_patch(self, body: bodies.RawBody | None = None) -> JSONPatch:
        """
        Build a list of JSON-patch ops for the changes & transformations.

        As a reference resource body, either the argument is used (if provided),
        or the original resource body. But the reference body is mandatory —
        the patch calculates the differences relative to the reference body.

        Some changes might disappear from the list if they are useless (no-op):
        e.g., setting a key to ``None`` to delete it when it is already absent;
        or setting the key to a value which is already in the resource body.
        """
        # Clone the original body to be mutated in memory before diffing.
        body_as_is = body if body is not None else self._original
        body_to_be = copy.deepcopy(body_as_is)
        if not self:
            return []
        if body_as_is is None or body_to_be is None:
            raise ValueError("Cannot build a JSON-patch without the original body as a reference.")

        # Apply the changes: merge-patches first (since they are not smart to be the last ones).
        # Then all callable transformations on top of the mutated body.
        self._apply_patch(body_to_be, (), dict(self))
        for fn in self.fns:
            fn(body_to_be)

        # Calculate the actual JSON ops for this particular state of the resource.
        # No "test" operations in pure JSON-patches as used in the mutating admission calls.
        ops: JSONPatch = jsonpatch.JsonPatch.from_diff(dict(body_as_is), dict(body_to_be)).patch
        return ops

    def _apply_patch(self, body: bodies.RawBody, path: dicts.FieldPath, value: object) -> None:
        """Apply the merge-patch instructions to the mutable raw body."""
        # TODO: LATER: optimize: we now dive into the dict for every key in dicts.ensure(),
        #       but we can merge the dict + patch in one run — implement such a function.
        #       priority: low — patches are usually small, so not a big problem for now.
        match value:
            case None:
                dicts.remove(cast(dict[Any, Any], body), path)
            case collections.abc.Mapping():
                for key, val in value.items():
                    self._apply_patch(body, path + (key,), val)
            case _:
                # NB: lists overwrite the whole value, as the merge-patch does; no strategic merges.
                dicts.ensure(cast(dict[Any, Any], body), path, value)
