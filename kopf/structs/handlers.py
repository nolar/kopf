import dataclasses
import enum
import warnings
from typing import NewType, Optional, Any

from kopf.structs import callbacks
from kopf.structs import dicts
from kopf.structs import filters

# Strings are taken from the users, but then tainted as this type for stricter type-checking:
# to prevent usage of some other strings (e.g. operator id) as the handlers ids.
HandlerId = NewType('HandlerId', str)


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
    Reason.UPDATE: 'update',
    Reason.DELETE: 'deletion',
    Reason.RESUME: 'resuming',
}


# A registered handler (function + meta info).
# FIXME: Must be frozen, but mypy fails in _call_handler() with a cryptic error:
# FIXME:    Argument 1 to "invoke" has incompatible type "Optional[HandlerResult]";
# FIXME:    expected "Union[LifeCycleFn, ActivityHandlerFn, ResourceHandlerFn]"
@dataclasses.dataclass
class BaseHandler:
    id: HandlerId
    fn: callbacks.BaseFn
    errors: Optional[ErrorsMode]
    timeout: Optional[float]
    retries: Optional[int]
    backoff: Optional[float]
    cooldown: dataclasses.InitVar[Optional[float]]  # deprecated, use `backoff`

    def __post_init__(self, cooldown: Optional[float]) -> None:
        if self.backoff is not None and cooldown is not None:
            raise TypeError("Either backoff or cooldown can be set, not both.")
        elif cooldown is not None:
            warnings.warn("cooldown=... is deprecated, use backoff=...", DeprecationWarning)
            self.backoff = cooldown

    # @property cannot be used due to a data field definition with the same name.
    def __getattribute__(self, name: str) -> Any:
        if name == 'cooldown':
            warnings.warn("handler.cooldown is deprecated, use handler.backoff", DeprecationWarning)
            return self.backoff
        else:
            return super().__getattribute__(name)

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
    labels: Optional[filters.MetaFilter]
    annotations: Optional[filters.MetaFilter]
    when: Optional[callbacks.WhenFilterFn]


@dataclasses.dataclass
class ResourceWatchingHandler(ResourceHandler):
    fn: callbacks.ResourceWatchingFn  # type clarification


@dataclasses.dataclass
class ResourceChangingHandler(ResourceHandler):
    fn: callbacks.ResourceChangingFn  # type clarification
    reason: Optional[Reason]
    field: Optional[dicts.FieldPath]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]

    @property
    def event(self) -> Optional[Reason]:
        warnings.warn("handler.event is deprecated; use handler.reason.", DeprecationWarning)
        return self.reason


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
