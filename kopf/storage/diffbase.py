import abc
import copy
import json
from typing import Any, Collection, Dict, Iterable, Optional, cast

from kopf.storage import conventions
from kopf.structs import bodies, dicts, patches


class DiffBaseStorage(conventions.StorageKeyMarkingConvention,
                      conventions.StorageStanzaCleaner,
                      metaclass=abc.ABCMeta):
    """
    Store the base essence for diff calculations, i.e. last handled state.

    The "essence" is a snapshot of meaningful fields, which must be tracked
    to identify the actual changes on the object (or absence of such).

    Used in the handling routines to check if there were significant changes
    (i.e. not the internal and system changes, like the uids, links, etc),
    and to get the exact per-field diffs for the specific handler functions.

    Conceptually similar to how ``kubectl apply`` stores the applied state
    on any object, and then uses that for the patch calculation:
    https://kubernetes.io/docs/concepts/overview/object-management-kubectl/declarative-config/
    """

    def build(
            self,
            *,
            body: bodies.Body,
            extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
    ) -> bodies.BodyEssence:
        """
        Extract only the relevant fields for the state comparisons.

        The framework ignores all the system fields (mostly from metadata)
        and the status senza completely. Except for some well-known and useful
        metadata, such as labels and annotations (except for sure garbage).

        A special set of fields can be provided even if they are supposed
        to be removed. This is used, for example, for handlers which react
        to changes in the specific fields in the status stanza,
        while the rest of the status stanza is removed.

        It is generally not a good idea to override this method in custom
        stores, unless a different definition of an object's essence is needed.
        """

        # Always use a copy, so that future changes do not affect the extracted essence.
        essence = cast(Dict[Any, Any], copy.deepcopy(dict(body)))

        # The top-level identifying fields never change, so there is not need to track them.
        if 'apiVersion' in essence:
            del essence['apiVersion']
        if 'kind' in essence:
            del essence['kind']

        # Purge the whole stenzas with system info (extra-fields are restored below).
        if 'metadata' in essence:
            del essence['metadata']
        if 'status' in essence:
            del essence['status']

        # We want some selected metadata to be tracked implicitly.
        dicts.cherrypick(src=body, dst=essence, fields=[
            'metadata.labels',
            'metadata.annotations',  # but not all of them! deleted below.
        ], picker=copy.deepcopy)

        # But we do not want all the annotations, only the potentially useful ones.
        # Also exclude the annotations of other Kopf-based operators' storages.
        annotations = essence.get('metadata', {}).get('annotations', {})
        ignored_prefixes = self._detect_marked_prefixes(annotations)
        for annotation in list(annotations):
            if any(annotation.startswith(f'{prefix}/') for prefix in ignored_prefixes):
                del annotations[annotation]
            elif annotation == 'kubectl.kubernetes.io/last-applied-configuration':
                del annotations[annotation]

        # Restore all explicitly whitelisted extra-fields from the original body.
        dicts.cherrypick(src=body, dst=essence, fields=extra_fields, picker=copy.deepcopy)

        self.remove_empty_stanzas(cast(bodies.BodyEssence, essence))
        return cast(bodies.BodyEssence, essence)

    @abc.abstractmethod
    def fetch(
            self,
            *,
            body: bodies.Body,
    ) -> Optional[bodies.BodyEssence]:
        raise NotImplementedError

    @abc.abstractmethod
    def store(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            essence: bodies.BodyEssence,
    ) -> None:
        raise NotImplementedError


class AnnotationsDiffBaseStorage(conventions.StorageKeyFormingConvention, DiffBaseStorage):

    def __init__(
            self,
            *,
            prefix: str = 'kopf.zalando.org',
            key: str = 'last-handled-configuration',
            v1: bool = True,  # will be switched to False a few releases later
    ) -> None:
        super().__init__(prefix=prefix, v1=v1)
        self.key = key

    def build(
            self,
            *,
            body: bodies.Body,
            extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
    ) -> bodies.BodyEssence:
        essence = super().build(body=body, extra_fields=extra_fields)
        self.remove_annotations(essence, set(self.make_keys(self.key, body=body)))
        self.remove_empty_stanzas(essence)
        return essence

    def fetch(
            self,
            *,
            body: bodies.Body,
    ) -> Optional[bodies.BodyEssence]:
        for full_key in self.make_keys(self.key, body=body):
            encoded = body.metadata.annotations.get(full_key, None)
            decoded = json.loads(encoded) if encoded is not None else None
            if decoded is not None:
                return cast(bodies.BodyEssence, decoded)
        return None

    def store(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            essence: bodies.BodyEssence,
    ) -> None:
        encoded: str = json.dumps(essence, separators=(',', ':'))  # NB: no spaces
        encoded += '\n'  # for better kubectl presentation without wrapping (same as kubectl's one)
        for full_key in self.make_keys(self.key, body=body):
            patch.metadata.annotations[full_key] = encoded
        self._store_marker(prefix=self.prefix, patch=patch, body=body)


class StatusDiffBaseStorage(DiffBaseStorage):

    def __init__(
            self,
            *,
            name: str = 'kopf',
            field: dicts.FieldSpec = 'status.{name}.last-handled-configuration',
    ) -> None:
        super().__init__()
        self._name = name
        real_field = field.format(name=self._name) if isinstance(field, str) else field
        self._field = dicts.parse_field(real_field)

    @property
    def field(self) -> dicts.FieldPath:
        return self._field

    @field.setter
    def field(self, field: dicts.FieldSpec) -> None:
        real_field = field.format(name=self._name) if isinstance(field, str) else field
        self._field = dicts.parse_field(real_field)

    def build(
            self,
            *,
            body: bodies.Body,
            extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
    ) -> bodies.BodyEssence:
        essence = super().build(body=body, extra_fields=extra_fields)

        # Work around an issue with mypy not treating TypedDicts as MutableMappings.
        essence_dict = cast(Dict[Any, Any], essence)
        dicts.remove(essence_dict, self.field)

        return essence

    def fetch(
            self,
            *,
            body: bodies.Body,
    ) -> Optional[bodies.BodyEssence]:
        encoded: Optional[str] = dicts.resolve(body, self.field, None)
        essence: Optional[bodies.BodyEssence] = json.loads(encoded) if encoded is not None else None
        return essence

    def store(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            essence: bodies.BodyEssence,
    ) -> None:
        # Store as a single string instead of full dict -- to avoid merges and unexpected data.
        encoded: str = json.dumps(essence, separators=(',', ':'))  # NB: no spaces
        dicts.ensure(patch, self.field, encoded)


class MultiDiffBaseStorage(DiffBaseStorage):

    def __init__(
            self,
            storages: Collection[DiffBaseStorage],
    ) -> None:
        super().__init__()
        self.storages = storages

    def build(
            self,
            *,
            body: bodies.Body,
            extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
    ) -> bodies.BodyEssence:
        essence = super().build(body=body, extra_fields=extra_fields)
        for storage in self.storages:
            # Let the individual stores to also clean the essence from their own fields.
            # For this, assume the the previous essence _is_ the body (what's left of it).
            essence = storage.build(body=bodies.Body(essence), extra_fields=extra_fields)
        return essence

    def fetch(
            self,
            *,
            body: bodies.Body,
    ) -> Optional[bodies.BodyEssence]:
        for storage in self.storages:
            content = storage.fetch(body=body)
            if content is not None:
                return content
        return None

    def store(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            essence: bodies.BodyEssence,
    ) -> None:
        for storage in self.storages:
            storage.store(body=body, patch=patch, essence=essence)
