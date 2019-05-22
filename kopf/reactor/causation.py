import logging
from typing import NamedTuple, Text, MutableMapping, Optional, Any, Union

from kopf.reactor import registries
from kopf.structs import diffs

# The constants for the event types, to prevent the direct string usage and typos.
# They are not exposed by the framework, but are used internally. See also: `kopf.on`.
CREATE = 'create'
UPDATE = 'update'
DELETE = 'delete'


class Cause(NamedTuple):
    """
    The cause is what has caused the whole reaction as a chain of handlers.

    Unlike the low-level Kubernetes watch-events, the cause is aware
    of the actual field changes, including the multi-handlers changes.
    """
    logger: Union[logging.Logger, logging.LoggerAdapter]
    resource: registries.Resource
    event: Text
    body: MutableMapping
    patch: MutableMapping
    diff: Optional[diffs.Diff] = None
    old: Optional[Any] = None
    new: Optional[Any] = None
