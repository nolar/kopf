import enum
from typing import Any, Mapping, Union

from kopf.structs import callbacks


class MetaFilterToken(enum.Enum):
    """ Tokens for filtering by annotations/labels. """
    PRESENT = enum.auto()
    ABSENT = enum.auto()


# For exporting to the top-level package.
ABSENT = MetaFilterToken.ABSENT
PRESENT = MetaFilterToken.PRESENT

# Filters for handler specifications (not the same as the object's values).
MetaFilter = Mapping[str, Union[str, MetaFilterToken, callbacks.MetaFilterFn]]

# Filters for old/new values of a field.
# NB: `Any` covers all other values, but we want to highlight that they are specially treated.
ValueFilter = Union[None, Any, MetaFilterToken, callbacks.MetaFilterFn]
