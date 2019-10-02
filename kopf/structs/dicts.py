"""
Some basic dicts and field-in-a-dict manipulation helpers.
"""
import collections.abc
import enum
from typing import (Any, Union, MutableMapping, Mapping, Tuple, List, Text,
                    Iterable, Iterator, Optional, TypeVar)

FieldPath = Tuple[str, ...]
FieldSpec = Union[None, Text, FieldPath, List[str]]

_T = TypeVar('_T')


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
        d: Mapping,
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


class DictView(Mapping[Any, Any]):
    """
    A lazy resolver for the "on-demand" dict keys.

    This is needed to have ``spec``, ``status``, and other special fields
    to be *assumed* as dicts, even if they are actually not present.
    And to prevent their implicit creation with ``.setdefault('spec', {})``,
    which produces unwanted side-effects (actually adds this field).

    >>> body = {}
    >>> spec = DictView(body, 'spec')

    >>> spec.get('field', 'default')
    ... 'default'

    >>> body['spec'] = {'field': 'value'}

    >>> spec.get('field', 'default')
    ... 'value'

    """

    def __init__(self, __src: Mapping[Any, Any], __path: FieldSpec = None):
        super().__init__()
        self._src = __src
        self._path = parse_field(__path)

    def __repr__(self):
        return repr(dict(self))

    def __len__(self) -> int:
        return len(resolve(self._src, self._path, {}, assume_empty=True))

    def __iter__(self) -> Iterator[Any]:
        return iter(resolve(self._src, self._path, {}, assume_empty=True))

    def __getitem__(self, item: Any) -> Any:
        return resolve(self._src, self._path + (item,))
