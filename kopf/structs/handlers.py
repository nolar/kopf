import dataclasses
import enum
from typing import Any, Optional

from kopf.structs import callbacks, dicts, filters, ids, references


class ErrorsMode(enum.Enum):
    """ How arbitrary (non-temporary/non-permanent) exceptions are treated. """
    IGNORED = enum.auto()
    TEMPORARY = enum.auto()
    PERMANENT = enum.auto()


class Activity(str, enum.Enum):
    STARTUP = 'startup'
    CLEANUP = 'cleanup'
    AUTHENTICATION = 'authentication'
    PROBE = 'probe'


class WebhookType(str, enum.Enum):
    VALIDATING = 'validating'
    MUTATING = 'mutating'

    def __str__(self) -> str:
        return str(self.value)


# Constants for cause types, to prevent a direct usage of strings, and typos.
# They are not exposed by the framework, but are used internally. See also: `kopf.on`.
class Reason(str, enum.Enum):
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    RESUME = 'resume'
    NOOP = 'noop'
    FREE = 'free'
    GONE = 'gone'

    def __str__(self) -> str:
        return str(self.value)


# These sets are checked in few places, so we keep them centralised:
# the user-facing causes (for handlers) and internally facing (for the reactor).
HANDLER_REASONS = (
    Reason.CREATE,
    Reason.UPDATE,
    Reason.DELETE,
    Reason.RESUME,
)
REACTOR_REASONS = (
    Reason.NOOP,
    Reason.FREE,
    Reason.GONE,
)
ALL_REASONS = HANDLER_REASONS + REACTOR_REASONS

# The human-readable names of these causes. Will be capitalised when needed.
TITLES = {
    Reason.CREATE: 'creation',
    Reason.UPDATE: 'updating',
    Reason.DELETE: 'deletion',
    Reason.RESUME: 'resuming',
}


# A registered handler (function + meta info).
# FIXME: Must be frozen, but mypy fails in _call_handler() with a cryptic error:
# FIXME:    Argument 1 to "invoke" has incompatible type "Optional[HandlerResult]";
# FIXME:    expected "Union[LifeCycleFn, ActivityHandlerFn, ResourceHandlerFn]"
@dataclasses.dataclass
class BaseHandler:
    id: ids.HandlerId
    fn: callbacks.BaseFn
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
    activity: Optional[Activity]
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
class ResourceWebhookHandler(ResourceHandler):
    fn: callbacks.ResourceWebhookFn  # type clarification
    reason: WebhookType
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
class ResourceIndexingHandler(ResourceHandler):
    fn: callbacks.ResourceIndexingFn  # type clarification

    def __str__(self) -> str:
        return f"Indexer {self.id!r}"

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class ResourceWatchingHandler(ResourceHandler):
    fn: callbacks.ResourceWatchingFn  # type clarification

    @property
    def requires_patching(self) -> bool:
        return False


@dataclasses.dataclass
class ResourceChangingHandler(ResourceHandler):
    fn: callbacks.ResourceChangingFn  # type clarification
    reason: Optional[Reason]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]
    field_needs_change: Optional[bool]  # to identify on-field/on-update with support for old=/new=.
    old: Optional[filters.ValueFilter]
    new: Optional[filters.ValueFilter]


@dataclasses.dataclass
class ResourceSpawningHandler(ResourceHandler):
    requires_finalizer: Optional[bool]
    initial_delay: Optional[float]


@dataclasses.dataclass
class ResourceDaemonHandler(ResourceSpawningHandler):
    fn: callbacks.ResourceDaemonFn  # type clarification
    cancellation_backoff: Optional[float]  # how long to wait before actual cancellation.
    cancellation_timeout: Optional[float]  # how long to wait before giving up on cancellation.
    cancellation_polling: Optional[float]  # how often to check for cancellation status.

    def __str__(self) -> str:
        return f"Daemon {self.id!r}"


@dataclasses.dataclass
class ResourceTimerHandler(ResourceSpawningHandler):
    fn: callbacks.ResourceTimerFn  # type clarification
    sharp: Optional[bool]
    idle: Optional[float]
    interval: Optional[float]

    def __str__(self) -> str:
        return f"Timer {self.id!r}"
