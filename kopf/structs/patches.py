"""
All the structures needed for Kubernetes patching.

Currently, it is implemented via a JSON merge-patch (RFC 7386),
i.e. a simple dictionary with field overrides, and ``None`` for field deletions.

In the future, it can be extended to a standalone object, which exposes
a dict-like behaviour, and remembers the changes in order of their execution,
and then generates the JSON patch (RFC 6902).
"""
from typing import Any, Dict


# Event-handling structures, used internally in the framework and handlers only.
class Patch(Dict[Any, Any]):
    pass
