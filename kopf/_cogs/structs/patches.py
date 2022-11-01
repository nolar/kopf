"""
All the structures needed for Kubernetes patching.

Currently, it is implemented via a JSON merge-patch (RFC 7386),
i.e. a simple dictionary with field overrides, and ``None`` for field deletions.

In the future, it can be extended to a standalone object, which exposes
a dict-like behaviour, and remembers the changes in order of their execution,
and then generates the JSON patch (RFC 6902).
"""
import collections.abc
from typing import Any, Dict, List, MutableMapping, Optional

from typing_extensions import Literal, TypedDict

from kopf._cogs.structs import bodies, dicts

JSONPatchOp = Literal["add", "replace", "remove"]


def _escaped_path(keys: List[str]) -> str:
    """Provides an appropriately escaped path for JSON Patches.

    See https://datatracker.ietf.org/doc/html/rfc6901#section-3 for more details.
    """
    return '/'.join(map(lambda key: key.replace('~', '~0').replace('/', '~1'), keys))


class JSONPatchItem(TypedDict, total=False):
    op: JSONPatchOp
    path: str
    value: Optional[Any]


JSONPatch = List[JSONPatchItem]


class MetaPatch(dicts.MutableMappingView[str, Any]):
    _labels: dicts.MutableMappingView[str, Optional[str]]
    _annotations: dicts.MutableMappingView[str, Optional[str]]

    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'metadata')
        self._labels = dicts.MutableMappingView(self, 'labels')
        self._annotations = dicts.MutableMappingView(self, 'annotations')

    @property
    def labels(self) -> dicts.MutableMappingView[str, Optional[str]]:
        return self._labels

    @property
    def annotations(self) -> dicts.MutableMappingView[str, Optional[str]]:
        return self._annotations


class SpecPatch(dicts.MutableMappingView[str, Any]):
    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'spec')


class StatusPatch(dicts.MutableMappingView[str, Any]):
    def __init__(self, __src: "Patch") -> None:
        super().__init__(__src, 'status')


# Event-handling structures, used internally in the framework and handlers only.
class Patch(Dict[str, Any]):

    def __init__(
        self,
        __src: Optional[MutableMapping[str, Any]] = None,
        body: Optional[bodies.RawBody] = None
    ) -> None:
        super().__init__(__src or {})
        self._meta = MetaPatch(self)
        self._spec = SpecPatch(self)
        self._status = StatusPatch(self)
        self._original = body

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

    def as_json_patch(self) -> JSONPatch:
        return [] if not self else self._as_json_patch(self, keys=[''])

    def _as_json_patch(self, value: object, keys: List[str]) -> JSONPatch:
        result: JSONPatch = []
        if value is None:
            result.append(JSONPatchItem(op='remove', path=_escaped_path(keys)))
        elif len(keys) > 1 and self._original and not self._is_in_original_path(keys):
            result.append(JSONPatchItem(op='add', path=_escaped_path(keys), value=value))
        elif isinstance(value, collections.abc.Mapping) and value:
            for key, val in value.items():
                result.extend(self._as_json_patch(val, keys + [key]))
        else:
            result.append(JSONPatchItem(op='replace', path=_escaped_path(keys), value=value))
        return result

    def _is_in_original_path(self, keys: List[str]) -> bool:
        _search = self._original
        for key in keys:
            if key == '':
                continue
            try:
                _search = _search[key]  # type: ignore
            except (KeyError, TypeError):
                return False
        return True
