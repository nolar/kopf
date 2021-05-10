import dataclasses
import enum
from typing import Any, Optional

from kopf.reactor import causation, invocation
from kopf.structs import callbacks, dicts, filters, ids, references


class ErrorsMode(enum.Enum):
    """ How arbitrary (non-temporary/non-permanent) exceptions are treated. """
    IGNORED = enum.auto()
    TEMPORARY = enum.auto()
    PERMANENT = enum.auto()


# A registered handler (function + meta info).
# FIXME: Must be frozen, but mypy fails in _call_handler() with a cryptic error:
# FIXME:    Argument 1 to "invoke" has incompatible type "Optional[HandlerResult]";
# FIXME:    expected "Union[LifeCycleFn, ActivityHandlerFn, ResourceHandlerFn]"
@dataclasses.dataclass
class BaseHandler:
    id: ids.HandlerId
    fn: invocation.Invokable
    param: Optional[Any]
    errors: Optional[ErrorsMode]
    timeout: Optional[float]
    retries: Optional[int]
    backoff: Optional[float]

    # Used in the logs. Overridden in some (but not all) handler types for better log messages.
    def __str__(self) -> str:
        return f"Handler {self.id!r}"


@dataclasses.dataclass
class ActivityHandler(BaseHandler):
    fn: callbacks.ActivityFn  # type clarification
    activity: Optional[causation.Activity]
    _fallback: bool = False  # non-public!

    def __str__(self) -> str:
        return f"Activity {self.id!r}"


@dataclasses.dataclass
class ResourceHandler(BaseHandler):
    selector: Optional[references.Selector]  # None is used only in sub-handlers
    labels: Optional[filters.MetaFilter]
    annotations: Optional[filters.MetaFilter]
    when: Optional[callbacks.WhenFilterFn]
    field: Optional[dicts.FieldPath]
    value: Optional[filters.ValueFilter]

    @property
    def requires_patching(self) -> bool:
        return True  # all typical handlers except several ones with overrides


@dataclasses.dataclass
class WebhookHandler(ResourceHandler):
    fn: callbacks.WebhookFn  # type clarification
    reason: causation.WebhookType
    operation: Optional[str]
    persistent: Optional[bool]
    side_effects: Optional[bool]
    ignore_failures: Optional[bool]

    def __str__(self) -> str:
        return f"Webhook {self.id!r}"

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class IndexingHandler(ResourceHandler):
    fn: callbacks.IndexingFn  # type clarification

    def __str__(self) -> str:
        return f"Indexer {self.id!r}"

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class WatchingHandler(ResourceHandler):
    fn: callbacks.WatchingFn  # type clarification

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class ChangingHandler(ResourceHandler):
    fn: callbacks.ChangingFn  # type clarification
    reason: Optional[causation.Reason]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]
    field_needs_change: Optional[bool]  # to identify on-field/on-update with support for old=/new=.
    old: Optional[filters.ValueFilter]
    new: Optional[filters.ValueFilter]


@dataclasses.dataclass
class SpawningHandler(ResourceHandler):
    requires_finalizer: Optional[bool]
    initial_delay: Optional[float]


@dataclasses.dataclass
class DaemonHandler(SpawningHandler):
    fn: callbacks.DaemonFn  # type clarification
    cancellation_backoff: Optional[float]  # how long to wait before actual cancellation.
    cancellation_timeout: Optional[float]  # how long to wait before giving up on cancellation.
    cancellation_polling: Optional[float]  # how often to check for cancellation status.

    def __str__(self) -> str:
        return f"Daemon {self.id!r}"


@dataclasses.dataclass
class TimerHandler(SpawningHandler):
    fn: callbacks.TimerFn  # type clarification
    sharp: Optional[bool]
    idle: Optional[float]
    interval: Optional[float]

    def __str__(self) -> str:
        return f"Timer {self.id!r}"
