"""
Some basic dicts and field-in-a-dict manipulation helpers.
"""
import collections.abc
import enum
from typing import (TypeVar, Any, Union, MutableMapping, Mapping, Tuple, List,
                    Iterable, Iterator, Callable, Optional, Generic)

FieldPath = Tuple[str, ...]
FieldSpec = Union[None, str, FieldPath, List[str]]

_T = TypeVar('_T')
_K = TypeVar('_K')
_V = TypeVar('_V')


class _UNSET(enum.Enum):
    token = enum.auto()


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
        d: Optional[Mapping[Any, Any]],
        field: FieldSpec,
        default: Union[_T, _UNSET] = _UNSET.token,
        *,
        assume_empty: bool = False,
) -> Union[Any, _T]:
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
            if result is None and assume_empty and not isinstance(default, _UNSET):
                return default
            elif isinstance(result, collections.abc.Mapping):
                result = result[key]
            else:
                raise TypeError(f"The structure is not a dict with field {key!r}: {result!r}")
        return result
    except KeyError:
        if not isinstance(default, _UNSET):
            return default
        raise


def ensure(
        d: MutableMapping[Any, Any],
        field: FieldSpec,
        value: Any,
) -> None:
    """
    Force-set a nested sub-field in a dict.

    If some levels of parents are missing, they are created as empty dicts
    (this what makes it "ensuring", not just "setting").
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


def remove(
        d: MutableMapping[Any, Any],
        field: FieldSpec,
) -> None:
    """
    Remove a nested sub-field from a dict, and all empty parents (optionally).

    All intermediate parents that become empty after the removal are also
    removed, making the whole original dict cleaner. For single-field removal,
    use a built-in ``del d[key]`` operation.

    If the target key is absent already, or any of the intermediate parents
    is absent (which implies that the target key is also absent), no error
    is raised, since the goal of deletion is achieved. The empty parents are
    anyway removed.
    """
    path = parse_field(field)
    if not path:
        raise ValueError("Removing a root of a dict is impossible. Provide a specific field.")

    elif len(path) == 1:
        try:
            del d[path[0]]
        except KeyError:
            pass

    else:
        try:
            # Recursion is the easiest way to implement it, assuming the bodies/patches are shallow.
            remove(d[path[0]], path[1:])
        except KeyError:
            pass
        else:
            # Clean the parent dict if it has become empty due to deletion of the only sub-key.
            # Upper parents will be handled by upper recursion functions.
            if d[path[0]] == {}:  # but not None, and not False, etc.
                del d[path[0]]


def cherrypick(
        src: Mapping[Any, Any],
        dst: MutableMapping[Any, Any],
        fields: Optional[Iterable[FieldSpec]],
        picker: Optional[Callable[[_T], _T]] = None,
) -> None:
    """
    Copy all specified fields between dicts (from src to dst).
    """
    picker = picker if picker is not None else lambda x: x
    fields = fields if fields is not None else []
    for field in fields:
        try:
            ensure(dst, field, picker(resolve(src, field)))
        except KeyError:
            pass  # absent in the source, nothing to merge


def walk(
        objs: Union[_T,
                    Iterable[_T],
                    Iterable[Union[_T,
                                   Iterable[_T]]]],
        *,
        nested: Optional[Iterable[FieldSpec]] = None,
) -> Iterator[_T]:
    """
    Iterate over objects, flattening the lists/tuples/iterables recursively.

    In plain English, the source is either an object, or a list/tuple/iterable
    of objects with any level of nesting. The dicts/mappings are excluded,
    despite they are iterables too, as they are treated as objects themselves.

    For the output, it yields all the objects in a flat iterable suitable for::

        for obj in walk(objs):
            pass

    The type declares only 2-level nesting, but this is done only
    for type-checker's limitations. The actual nesting can be infinite.
    It is highly unlikely that there will be anything deeper than one level.
    """
    if objs is None:
        pass
    elif isinstance(objs, collections.abc.Mapping):
        yield objs  # type: ignore
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


class MappingView(Generic[_K, _V], Mapping[_K, _V]):
    """
    A lazy resolver for the "on-demand" dict keys.

    This is needed to have :kwarg:`spec`, :kwarg:`status`, and other fields
    to be *assumed* as dicts, even if they are actually not present.
    And to prevent their implicit creation with ``.setdefault('spec', {})``,
    which produces unwanted side-effects (actually adds this field).

    >>> body = {}
    >>> spec = MappingView(body, 'spec')
    >>> spec.get('field', 'default')
    ... 'default'
    >>> body['spec'] = {'field': 'value'}
    >>> spec.get('field', 'default')
    ... 'value'
    """
    _src: Mapping[_K, _V]

    def __init__(self, __src: Mapping[Any, Any], __path: FieldSpec = None) -> None:
        super().__init__()
        self._src = __src
        self._path = parse_field(__path)

    def __repr__(self) -> str:
        return repr(dict(self))

    def __len__(self) -> int:
        return len(resolve(self._src, self._path, {}, assume_empty=True))

    def __iter__(self) -> Iterator[Any]:
        return iter(resolve(self._src, self._path, {}, assume_empty=True))

    def __getitem__(self, item: _K) -> _V:
        return resolve(self._src, self._path + (item,))


class MutableMappingView(Generic[_K, _V], MappingView[_K, _V], MutableMapping[_K, _V]):
    """
    A mapping view with values stored and sub-dicts auto-created.

    >>> patch = {}
    >>> status = MutableMappingView(patch, 'status')
    >>> status.get('field', 'default')
    ... 'default'
    >>> patch
    ... {}
    >>> status['field'] = 'value'
    >>> patch
    ... {'status': {'field': 'value'}}
    >>> status.get('field', 'default')
    ... 'value'
    """
    _src: MutableMapping[_K, _V]  # type clarification

    def __delitem__(self, item: _K) -> None:
        d = resolve(self._src, self._path)
        del d[item]

    def __setitem__(self, item: _K, value: _V) -> None:
        ensure(self._src, self._path + (item,), value)


class ReplaceableMappingView(Generic[_K, _V], MappingView[_K, _V]):
    """
    A mapping view where the whole source can be replaced atomically.

    All derived mapping views that use this mapping view as their source will
    immediately notice the change.

    The method names are intentionally long and multi-word -- to not have
    potential collisions with regular expected attributes/properties.

    >>> body = ReplaceableMappingView()
    >>> spec = MappingView(body, 'spec')
    >>> spec.get('field', 'default')
    ... 'default'
    >>> body._replace_with({'spec': {'field': 'value'}})
    >>> spec.get('field', 'default')
    ... 'value'
    """

    def _replace_from(self, __src: MappingView[_K, _V]) -> None:
        self._src = __src._src

    def _replace_with(self, __src: Mapping[_K, _V]) -> None:
        self._src = __src
