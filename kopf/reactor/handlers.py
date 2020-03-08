import dataclasses
import warnings
from typing import NewType, Callable, Optional, Union, Any

from kopf.reactor import causation
from kopf.reactor import errors as errors_
from kopf.structs import callbacks
from kopf.structs import dicts
from kopf.structs import filters

# Strings are taken from the users, but then tainted as this type for stricter type-checking:
# to prevent usage of some other strings (e.g. operator id) as the handlers ids.
HandlerId = NewType('HandlerId', str)


# A registered handler (function + meta info).
# FIXME: Must be frozen, but mypy fails in _call_handler() with a cryptic error:
# FIXME:    Argument 1 to "invoke" has incompatible type "Optional[HandlerResult]";
# FIXME:    expected "Union[LifeCycleFn, ActivityHandlerFn, ResourceHandlerFn]"
@dataclasses.dataclass
class BaseHandler:
    id: HandlerId
    fn: Callable[..., Optional[callbacks.Result]]
    errors: Optional[errors_.ErrorsMode]
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


@dataclasses.dataclass
class ActivityHandler(BaseHandler):
    fn: callbacks.ActivityFn  # type clarification
    activity: Optional[causation.Activity]
    _fallback: bool = False  # non-public!


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
    reason: Optional[causation.Reason]
    field: Optional[dicts.FieldPath]
    initial: Optional[bool]
    deleted: Optional[bool]  # used for mixed-in (initial==True) @on.resume handlers only.
    requires_finalizer: Optional[bool]

    @property
    def event(self) -> Optional[causation.Reason]:
        warnings.warn("handler.event is deprecated; use handler.reason.", DeprecationWarning)
        return self.reason
