"""
Some basic dicts and field-in-a-dict manipulation helpers.
"""
from typing import Any, Union, Mapping, Tuple, List, Text

FieldPath = Tuple[str, ...]
FieldSpec = Union[None, Text, FieldPath, List[str]]

_UNSET = object()


def parse_field(
        field: FieldSpec,
) -> FieldPath:
    """
    Convert any field into a tuple of nested sub-fields.
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
):
    """
    Retrieve a nested sub-field from a dict.
    """
    path = parse_field(field)
    try:
        result = d
        for key in path:
            result = result[key]
        return result
    except KeyError:
        if default is _UNSET:
            raise
        else:
            return default
