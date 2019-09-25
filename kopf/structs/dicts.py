"""
Some basic dicts and field-in-a-dict manipulation helpers.
"""
import collections.abc
from typing import Any, Union, MutableMapping, Mapping, Tuple, List, Text, Iterable, Optional

FieldPath = Tuple[str, ...]
FieldSpec = Union[None, Text, FieldPath, List[str]]

_UNSET = object()


def parse_field(
        field: FieldSpec,
) -> FieldPath:
    """
    Convert any field into a tuple of nested sub-fields.

    Supported notations:

    * ``None`` (for root of a dict).
    * ``"field.subfield"``
    * ``("field", "subfield")``
    * ``["field", "subfield"]``
    """
    if field is None:
        return tuple()
    elif isinstance(field, str):
        return tuple(field.split('.'))
    elif isinstance(field, (list, tuple)):
        return tuple(field)
    else:
        raise ValueError(f"Field must be either a str, or a list/tuple. Got {field!r}")


def resolve(
        d: Mapping,
        field: FieldSpec,
        default: Any = _UNSET,
        *,
        assume_empty: bool = False,
):
    """
    Retrieve a nested sub-field from a dict.

    If ``assume_empty`` is set, then the non-existent path keys are assumed
    to be empty dictionaries, and then the ``default`` is returned.

    Otherwise (by default), any attempt to get a key from ``None``
    leads to a ``TypeError`` -- same as in Python: ``None['key']``.
    """
    path = parse_field(field)
    try:
        result = d
        for key in path:
            if result is None and assume_empty and default is not _UNSET:
                return default
            elif isinstance(result, collections.abc.Mapping):
                result = result[key]
            else:
                raise TypeError(f"The structure is not a dict with field {key!r}: {result!r}")
        return result
    except KeyError:
        if default is _UNSET:
            raise
        else:
            return default


def ensure(
        d: MutableMapping,
        field: FieldSpec,
        value: Any,
):
    """
    Force-set a nested sub-field in a dict.
    """
    result = d
    path = parse_field(field)
    if not path:
        raise ValueError("Setting a root of a dict is impossible. Provide the specific fields.")
    for key in path[:-1]:
        try:
            result = result[key]
        except KeyError:
            result = result.setdefault(key, {})
    result[path[-1]] = value


def cherrypick(
        src: Mapping,
        dst: MutableMapping,
        fields: Optional[Iterable[FieldSpec]],
):
    """
    Copy all specified fields between dicts (from src to dst).
    """
    fields = fields if fields is not None else []
    for field in fields:
        try:
            ensure(dst, field, resolve(src, field))
        except KeyError:
            pass  # absent in the source, nothing to merge


def walk(
        objs,
        nested: Optional[Iterable[FieldSpec]] = None,
):
    """
    Iterate over one or many dicts (and sub-dicts recursively).
    """
    if objs is None:
        return
    elif isinstance(objs, collections.abc.Mapping):
        yield objs
        for subfield in (nested if nested is not None else []):
            try:
                yield resolve(objs, parse_field(subfield))
            except KeyError:
                pass
    elif isinstance(objs, collections.abc.Iterable):
        for obj in objs:
            yield from walk(obj, nested=nested)
    else:
        yield objs  # NB: not a mapping, no nested sub-fields.
