"""
State stores are used to track the handlers' states across handling cycles.

Specifically, they track which handlers are finished, which are not yet,
and how many retries were there, and some other information.

There could be more than one low-level k8s watch-events per one actual
high-level kopf-event (a cause). The handlers are called at different times,
and the overall handling routine should persist the handler status somewhere.

When the full event cycle is executed (possibly including multiple re-runs),
the state of all involved handlers is purged. The life-long persistence of state
is not intended: otherwise, multiple distinct causes will clutter the status
and collide with each other (especially critical for multiple updates).

Other unrelated handlers (e.g. from other operators) can co-exist with
the involved handlers (if stored in the resource itself), as the handler states
are independent of each other, and are purged individually, not all at once.

---

Originally, the handlers' state was persisted in ``.status.kopf.progress``.
But due to stricter Kubernetes schemas for built-in resources, they had to
move to annotations. As part of such move, any state persistence engines
are made possible by inheriting and overriding the base classes, though it is
considered an advanced use-case and is only briefly mentioned in the docs.

In all cases, the persisted state for each handler is a fixed-structure dict
with the following keys:

* ``started`` is a timestamp when the handler was first called.
* ``stopped`` is a timestamp when the handler either finished or failed.
* ``delayed`` is a timestamp when the handler should be invoked again (retried).
* ``retries`` is a number of retries so far or in total (if succeeded/failed).
* ``success`` is a boolean flag for a final success (no re-executions).
* ``failure`` is a boolean flag for a final failure (no retries).
* ``message`` is a descriptive message of the last error (an exception).

All timestamps are strings in ISO8601 format in UTC (no explicit ``Z`` suffix).
"""
import abc
import base64
import copy
import hashlib
import json
import warnings
from typing import Any, Collection, Dict, Iterable, Mapping, Optional, cast

from typing_extensions import TypedDict

from kopf.structs import bodies, dicts, handlers, patches


class ProgressRecord(TypedDict, total=True):
    """ A single record stored for persistence of a single handler. """
    started: Optional[str]
    stopped: Optional[str]
    delayed: Optional[str]
    retries: Optional[int]
    success: Optional[bool]
    failure: Optional[bool]
    message: Optional[str]
    subrefs: Optional[Collection[handlers.HandlerId]]


class ProgressStorage(metaclass=abc.ABCMeta):
    """
    Base class and an interface for all persistent states.

    The state is persisted strict per-handler, not for all handlers at once:
    to support overlapping operators (assuming different handler ids) storing
    their state on the same fields of the resource (e.g. ``state.kopf``).

    This also ensures that no extra logic for state merges will be needed:
    the handler states are atomic (i.e. state fields are not used separately)
    but independent: i.e. handlers should be persisted on their own, unrelated
    to other handlers; i.e. never combined to other atomic structures.

    If combining is still needed with performance optimization in mind (e.g.
    for relational/transactional databases), the keys can be cached in memory
    for short time, and ``flush()`` can be overridden to actually store them.
    """

    @abc.abstractmethod
    def fetch(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
    ) -> Optional[ProgressRecord]:
        raise NotImplementedError

    @abc.abstractmethod
    def store(
            self,
            *,
            key: handlers.HandlerId,
            record: ProgressRecord,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def purge(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def touch(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            value: Optional[str],
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self, *, essence: bodies.BodyEssence) -> bodies.BodyEssence:
        return copy.deepcopy(essence)

    def flush(self) -> None:
        pass


class AnnotationsProgressStorage(ProgressStorage):
    """
    State storage in ``.metadata.annotations`` with JSON-serialised content.

    An example without a prefix:

    .. code-block: yaml

        metadata:
          annotations:
            create_fn_1: '{"started": "2020-02-14T16:58:25.396364", "stopped":
                           "2020-02-14T16:58:25.401844", "retries": 1, "success": true}'
            create_fn_2: '{"started": "2020-02-14T16:58:25.396421", "retries": 0}'
        spec: ...
        status: ...

    An example with a prefix:

    .. code-block: yaml

        metadata:
          annotations:
            kopf.zalando.org/create_fn_1: '{"started": "2020-02-14T16:58:25.396364", "stopped":
                                    "2020-02-14T16:58:25.401844", "retries": 1, "success": true}'
            kopf.zalando.org/create_fn_2: '{"started": "2020-02-14T16:58:25.396421", "retries": 0}'
        spec: ...
        status: ...

    A note on the annotation names/keys:

    **V1** keys were implemented overly restrictive: the length of 63 chars
    was applied to the whole annotation key, including the prefix.

    This caused unnecessary and avoidable loss of useful information: e.g.
    ``lengthy-operator-name-to-hit-63-chars.example.com/update-OJOYLA``
    instead of
    ``lengthy-operator-name-to-hit-63-chars.example.com/update.sub1``.

    `K8s says`__ that only the name is max 63 chars long, while the whole key
    (i.e. including the prefix, if present) can be max 253 chars.

    __ https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set

    **V2** keys implement this new hashing approach, trying to keep as much
    information in the keys as possible. Only the really lengthy keys
    will be cut the same way as V1 keys.

    If the prefix is longer than 189 chars (253-63-1), the full key could
    be longer than the limit of 253 chars -- e.g. with lengthy handler ids,
    more often for field-handlers or sub-handlers. In that case,
    the annotation keys are not shortened, and the patching would fail.

    It is the developer's responsibility to choose the prefix short enough
    to fit into K8s's restrictions. However, a warning is issued on storage
    creation, so that the strict-mode operators could convert the warning
    into an exception, thus failing the operator startup.

    For smoother upgrades of operators from V1 to V2, and for safer rollbacks
    from V2 to V1, both versions of keys are stored in annotations.
    At some point in time, the V1 keys will be read, purged, but not stored,
    thus cutting the rollback possibilities to Kopf versions with V1 keys.

    Since the annotations are purged in case of a successful handling cycle,
    this multi-versioned behaviour will most likely be unnoticed by the users,
    except when investigating the issues with persistence.

    This mode can be controlled via the storage's constructor parameter
    ``v1=True/False`` (the default is ``True`` for the time of transition).
    """

    def __init__(
            self,
            *,
            prefix: Optional[str] = 'kopf.zalando.org',
            verbose: bool = False,
            touch_key: str = 'touch-dummy',  # NB: not dotted, but dashed
            v1: bool = True,  # Will be switch to False a few releases later.
    ) -> None:
        super().__init__()
        self.prefix = prefix
        self.verbose = verbose
        self.touch_key = touch_key
        self.v1 = v1

        # 253 is the max length, 63 is the most lengthy name part, 1 is for the "/" separator.
        if len(self.prefix or '') > 253 - 63 - 1:
            warnings.warn("The annotations prefix is too long. It can cause errors when PATCHing.")

    def fetch(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
    ) -> Optional[ProgressRecord]:
        for full_key in self.make_keys(key):
            key_field = ['metadata', 'annotations', full_key]
            encoded = dicts.resolve(body, key_field, None, assume_empty=True)
            decoded = json.loads(encoded) if encoded is not None else None
            if decoded is not None:
                return cast(ProgressRecord, decoded)
        return None

    def store(
            self,
            *,
            key: handlers.HandlerId,
            record: ProgressRecord,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        for full_key in self.make_keys(key):
            key_field = ['metadata', 'annotations', full_key]
            decoded = {key: val for key, val in record.items() if self.verbose or val is not None}
            encoded = json.dumps(decoded, separators=(',', ':'))  # NB: no spaces
            dicts.ensure(patch, key_field, encoded)

    def purge(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        absent = object()
        for full_key in self.make_keys(key):
            key_field = ['metadata', 'annotations', full_key]
            body_value = dicts.resolve(body, key_field, absent, assume_empty=True)
            patch_value = dicts.resolve(patch, key_field, absent, assume_empty=True)
            if body_value is not absent:
                dicts.ensure(patch, key_field, None)
            elif patch_value is not absent:
                dicts.remove(patch, key_field)

    def touch(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            value: Optional[str],
    ) -> None:
        for full_key in self.make_keys(self.touch_key):
            key_field = ['metadata', 'annotations', full_key]
            body_value = dicts.resolve(body, key_field, None, assume_empty=True)
            if body_value != value:  # also covers absent-vs-None cases.
                dicts.ensure(patch, key_field, value)

    def clear(self, *, essence: bodies.BodyEssence) -> bodies.BodyEssence:
        essence = super().clear(essence=essence)
        annotations = essence.get('metadata', {}).get('annotations', {})
        for name in list(annotations.keys()):
            if self.prefix and name.startswith(f'{self.prefix}/'):
                del annotations[name]
        return essence

    def make_key(self, key: str, max_length: int = 63) -> str:
        warnings.warn("make_key() is deprecated; use make_key_v1(), make_key_v2(), make_keys(), "
                      "or avoid making the keys directly at all.", DeprecationWarning)
        return self.make_key_v1(key, max_length=max_length)

    def make_keys(self, key: str) -> Iterable[str]:
        v2_keys = [self.make_key_v2(key)]
        v1_keys = [self.make_key_v1(key)] if self.v1 else []
        return v2_keys + list(set(v1_keys) - set(v2_keys))

    def make_key_v1(self, key: str, max_length: int = 63) -> str:

        # K8s has a limitation on the allowed charsets in annotation/label keys.
        # https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#syntax-and-character-set
        safe_key = key.replace('/', '.')

        # K8s has a limitation of 63 chars per annotation/label key.
        # Force it to 63 chars by replacing the tail with a consistent hash (with full alphabet).
        # Force it to end with alnums instead of altchars or trailing chars (K8s requirement).
        prefix = f'{self.prefix}/' if self.prefix else ''
        if len(safe_key) <= max_length - len(prefix):
            suffix = ''
        else:
            suffix = self.make_suffix(safe_key)

        full_key = f'{prefix}{safe_key[:max_length - len(prefix) - len(suffix)]}{suffix}'
        return full_key

    def make_key_v2(self, key: str, max_length: int = 63) -> str:
        prefix = f'{self.prefix}/' if self.prefix else ''
        suffix = self.make_suffix(key) if len(key) > max_length else ''
        key_limit = max(0, max_length - len(suffix))
        clean_key = key.replace('/', '.')
        final_key = f'{prefix}{clean_key[:key_limit]}{suffix}'
        return final_key

    def make_suffix(self, key: str) -> str:
        digest = hashlib.blake2b(key.encode('utf-8'), digest_size=4).digest()
        alnums = base64.b64encode(digest, altchars=b'-.').decode('ascii')
        return f'-{alnums}'.rstrip('=-.')


class StatusProgressStorage(ProgressStorage):
    """
    State storage in ``.status`` stanza with deep structure.

    The structure is this:

    .. code-block: yaml

        metadata: ...
        spec: ...
        status: ...
            kopf:
                progress:
                    handler1:
                        started: 2018-12-31T23:59:59,999999
                        stopped: 2018-01-01T12:34:56,789000
                        success: true
                    handler2:
                        started: 2018-12-31T23:59:59,999999
                        stopped: 2018-01-01T12:34:56,789000
                        failure: true
                        message: "Error message."
                    handler3:
                        started: 2018-12-31T23:59:59,999999
                        retries: 30
                    handler3/sub1:
                        started: 2018-12-31T23:59:59,999999
                        delayed: 2018-01-01T12:34:56,789000
                        retries: 10
                        message: "Not ready yet."
                    handler3/sub2:
                        started: 2018-12-31T23:59:59,999999
    """

    def __init__(
            self,
            *,
            name: str = 'kopf',
            field: dicts.FieldSpec = 'status.{name}.progress',
            touch_field: dicts.FieldSpec = 'status.{name}.dummy',
    ) -> None:
        super().__init__()
        self._name = name

        real_field = field.format(name=name) if isinstance(field, str) else field
        self._field = dicts.parse_field(real_field)

        real_field = touch_field.format(name=name) if isinstance(touch_field, str) else touch_field
        self._touch_field = dicts.parse_field(real_field)

    @property
    def field(self) -> dicts.FieldPath:
        return self._field

    @field.setter
    def field(self, field: dicts.FieldSpec) -> None:
        real_field = field.format(name=self._name) if isinstance(field, str) else field
        self._field = dicts.parse_field(real_field)

    @property
    def touch_field(self) -> dicts.FieldPath:
        return self._touch_field

    @touch_field.setter
    def touch_field(self, field: dicts.FieldSpec) -> None:
        real_field = field.format(name=self._name) if isinstance(field, str) else field
        self._touch_field = dicts.parse_field(real_field)

    def fetch(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
    ) -> Optional[ProgressRecord]:
        container: Mapping[handlers.HandlerId, ProgressRecord]
        container = dicts.resolve(body, self.field, {})
        return container.get(key, None)

    def store(
            self,
            *,
            key: handlers.HandlerId,
            record: ProgressRecord,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        # Nones are cleaned by K8s API itself.
        dicts.ensure(patch, self.field + (key,), record)

    def purge(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        absent = object()
        key_field = self.field + (key,)
        body_value = dicts.resolve(body, key_field, absent, assume_empty=True)
        patch_value = dicts.resolve(patch, key_field, absent, assume_empty=True)
        if body_value is not absent:
            dicts.ensure(patch, key_field, None)
        elif patch_value is not absent:
            dicts.remove(patch, key_field)

    def touch(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            value: Optional[str],
    ) -> None:
        key_field = self.touch_field
        body_value = dicts.resolve(body, key_field, None, assume_empty=True)
        if body_value != value:  # also covers absent-vs-None cases.
            dicts.ensure(patch, key_field, value)

    def clear(self, *, essence: bodies.BodyEssence) -> bodies.BodyEssence:
        essence = super().clear(essence=essence)

        # Work around an issue with mypy not treating TypedDicts as MutableMappings.
        essence_dict = cast(Dict[Any, Any], essence)
        dicts.remove(essence_dict, self.field)

        return essence


class MultiProgressStorage(ProgressStorage):

    def __init__(
            self,
            storages: Collection[ProgressStorage],
    ) -> None:
        super().__init__()
        self.storages = storages

    def fetch(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
    ) -> Optional[ProgressRecord]:
        for storage in self.storages:
            content = storage.fetch(key=key, body=body)
            if content is not None:
                return content
        return None

    def store(
            self,
            *,
            key: handlers.HandlerId,
            record: ProgressRecord,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        for storage in self.storages:
            storage.store(key=key, record=record, body=body, patch=patch)

    def purge(
            self,
            *,
            key: handlers.HandlerId,
            body: bodies.Body,
            patch: patches.Patch,
    ) -> None:
        for storage in self.storages:
            storage.purge(key=key, body=body, patch=patch)

    def touch(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            value: Optional[str],
    ) -> None:
        for storage in self.storages:
            storage.touch(body=body, patch=patch, value=value)

    def clear(self, *, essence: bodies.BodyEssence) -> bodies.BodyEssence:
        for storage in self.storages:
            essence = storage.clear(essence=essence)
        return essence


class SmartProgressStorage(MultiProgressStorage):

    def __init__(
            self,
            *,
            name: str = 'kopf',
            field: dicts.FieldSpec = 'status.{name}.progress',
            touch_key: str = 'touch-dummy',  # NB: not dotted, but dashed
            touch_field: dicts.FieldSpec = 'status.{name}.dummy',
            prefix: str = 'kopf.zalando.org',
            verbose: bool = False,
    ) -> None:
        super().__init__([
            AnnotationsProgressStorage(prefix=prefix, verbose=verbose, touch_key=touch_key),
            StatusProgressStorage(name=name, field=field, touch_field=touch_field),
        ])
