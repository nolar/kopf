"""
Some basic dicts and field-in-a-dict manipulation helpers.
"""
from typing import Union, Tuple, List, Text

FieldPath = Tuple[str, ...]
FieldSpec = Union[None, Text, FieldPath, List[str]]


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
