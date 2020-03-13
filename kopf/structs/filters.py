import enum
from typing import Mapping, Union

from kopf.structs import callbacks


class MetaFilterToken(enum.Enum):
    """ Tokens for filtering by annotations/labels. """
    PRESENT = enum.auto()
    ABSENT = enum.auto()


# For exporting to the top-level package.
ABSENT = MetaFilterToken.ABSENT
PRESENT = MetaFilterToken.PRESENT

# Filters for handler specifications (not the same as the object's values).
MetaFilter = Mapping[str, Union[None, str, MetaFilterToken, callbacks.MetaFilterFn]]
