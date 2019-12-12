"""
A registry of the handlers, attached to the resources or events.

The global registry is populated by the `kopf.on` decorators, and is used
to register the resources being watched and handled, and to attach
the handlers to the specific causes (create/update/delete/field-change).

The simple registry is part of the global registry (for each individual
resource), and also used for the sub-handlers within a top-level handler.

Both are used in the `kopf.reactor.handling` to retrieve the list
of the handlers to be executed on each reaction cycle.
"""
import abc
import collections
import dataclasses
import enum
import functools
import logging
import warnings
from types import FunctionType, MethodType
from typing import (Any, MutableMapping, Optional, Sequence, Collection, Iterable, Iterator,
                    Union, List, Set, FrozenSet, Mapping, NewType, Callable, cast,
                    Generic, TypeVar)

from typing_extensions import Protocol

from kopf.reactor import causation
from kopf.structs import bodies
from kopf.structs import dicts
from kopf.structs import diffs
from kopf.structs import patches
from kopf.structs import resources as resources_
from kopf.utilities import piggybacking

# Strings are taken from the users, but then tainted as this type for stricter type-checking:
# to prevent usage of some other strings (e.g. operator id) as the handlers ids.
HandlerId = NewType('HandlerId', str)

# A specialised type to highlight the purpose or origin of the data of type Any,
# to not be mixed with other arbitrary Any values, where it is indeed "any".
HandlerResult = NewType('HandlerResult', object)


class ErrorsMode(enum.Enum):
    """ How arbitrary (non-temporary/non-permanent) exceptions are treated. """
    IGNORED = enum.auto()
    TEMPORARY = enum.auto()
    PERMANENT = enum.auto()


class ActivityHandlerFn(Protocol):
    def __call__(
            self,
            *args: Any,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            **kwargs: Any,
    ) -> Optional[HandlerResult]: ...


class ResourceHandlerFn(Protocol):
    def __call__(
            self,
            *args: Any,
            type: str,
            event: Union[str, bodies.Event],
            body: bodies.Body,
            meta: bodies.Meta,
            spec: bodies.Spec,
            status: bodies.Status,
            uid: str,
            name: str,
            namespace: Optional[str],
            patch: patches.Patch,
            logger: Union[logging.Logger, logging.LoggerAdapter],
            diff: diffs.Diff,
            old: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            new: Optional[Union[bodies.BodyEssence, Any]],  # "Any" is for field-handlers.
            **kwargs: Any,
    ) -> Optional[HandlerResult]: ...


# A registered handler (function + meta info).
# FIXME: Must be frozen, but mypy fails in _call_handler() with a cryptic error:
# FIXME:    Argument 1 to "invoke" has incompatible type "Optional[HandlerResult]";
# FIXME:    expected "Union[LifeCycleFn, ActivityHandlerFn, ResourceHandlerFn]"
@dataclasses.dataclass
class BaseHandler:
    id: HandlerId
    fn: Callable[..., Optional[HandlerResult]]
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


@dataclasses.dataclass
class ActivityHandler(BaseHandler):
    fn: ActivityHandlerFn  # type clarification
    activity: Optional[causation.Activity] = None
    _fallback: bool = False  # non-public!


@dataclasses.dataclass
class ResourceHandler(BaseHandler):
    fn: ResourceHandlerFn  # type clarification
    reason: Optional[causation.Reason]
    field: Optional[dicts.FieldPath]
    initial: Optional[bool] = None
    deleted: Optional[bool] = None  # used for mixed-in (initial==True) @on.resume handlers only.
    labels: Optional[bodies.Labels] = None
    annotations: Optional[bodies.Annotations] = None
    requires_finalizer: Optional[bool] = None

    @property
    def event(self) -> Optional[causation.Reason]:
        warnings.warn("`handler.event` is deprecated; use `handler.reason`.", DeprecationWarning)
        return self.reason


# We only type-check for known classes of handlers/callbacks, and ignore any custom subclasses.
HandlerFnT = TypeVar('HandlerFnT', ActivityHandlerFn, ResourceHandlerFn)
HandlerT = TypeVar('HandlerT', ActivityHandler, ResourceHandler)
CauseT = TypeVar('CauseT', bound=causation.BaseCause)


class GenericRegistry(Generic[HandlerT, HandlerFnT]):
    """ A generic base class of a simple registry (with no handler getters). """
    _handlers: List[HandlerT]

    def __init__(self, prefix: Optional[str] = None) -> None:
        super().__init__()
        self.prefix = prefix
        self._handlers = []

    def __bool__(self) -> bool:
        return bool(self._handlers)

    def append(self, handler: HandlerT) -> None:
        self._handlers.append(handler)


class ActivityRegistry(GenericRegistry[ActivityHandler, ActivityHandlerFn]):
    """ An actual registry of activity handlers. """

    def register(
            self,
            fn: ActivityHandlerFn,
            *,
            id: Optional[str] = None,
            errors: Optional[ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            activity: Optional[causation.Activity] = None,
            _fallback: bool = False,
    ) -> ActivityHandlerFn:
        real_id = generate_id(fn=fn, id=id, prefix=self.prefix)
        handler = ActivityHandler(
            id=real_id, fn=fn, activity=activity,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            _fallback=_fallback,
        )
        self.append(handler)
        return fn

    def get_handlers(
            self,
            activity: causation.Activity,
    ) -> Sequence[ActivityHandler]:
        return list(_deduplicated(self.iter_handlers(activity=activity)))

    def iter_handlers(
            self,
            activity: causation.Activity,
    ) -> Iterator[ActivityHandler]:
        found: bool = False

        # Regular handlers go first.
        for handler in self._handlers:
            if handler.activity is None or handler.activity == activity and not handler._fallback:
                yield handler
                found = True

        # Fallback handlers -- only if there were no matching regular handlers.
        if not found:
            for handler in self._handlers:
                if handler.activity is None or handler.activity == activity and handler._fallback:
                    yield handler


class ResourceRegistry(GenericRegistry[ResourceHandler, ResourceHandlerFn], Generic[CauseT]):
    """ An actual registry of resource handlers. """

    def register(
            self,
            fn: ResourceHandlerFn,
            *,
            id: Optional[str] = None,
            reason: Optional[causation.Reason] = None,
            event: Optional[str] = None,  # deprecated, use `reason`
            field: Optional[dicts.FieldSpec] = None,
            errors: Optional[ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            initial: Optional[bool] = None,
            deleted: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[bodies.Labels] = None,
            annotations: Optional[bodies.Annotations] = None,
    ) -> ResourceHandlerFn:
        if reason is None and event is not None:
            reason = causation.Reason(event)

        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = generate_id(fn=fn, id=id, prefix=self.prefix, suffix=".".join(real_field or []))
        handler = ResourceHandler(
            id=real_id, fn=fn, reason=reason, field=real_field,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            initial=initial, deleted=deleted, requires_finalizer=requires_finalizer,
            labels=labels, annotations=annotations,
        )

        self.append(handler)
        return fn

    def get_handlers(
            self,
            cause: CauseT,
    ) -> Sequence[ResourceHandler]:
        return list(_deduplicated(self.iter_handlers(cause=cause)))

    @abc.abstractmethod
    def iter_handlers(
            self,
            cause: CauseT,
    ) -> Iterator[ResourceHandler]:
        raise NotImplementedError

    def get_extra_fields(
            self,
    ) -> Set[dicts.FieldPath]:
        return set(self.iter_extra_fields())

    def iter_extra_fields(
            self,
    ) -> Iterator[dicts.FieldPath]:
        for handler in self._handlers:
            if handler.field:
                yield handler.field

    def requires_finalizer(
            self,
            body: bodies.Body,
    ) -> bool:
        # check whether the body matches a deletion handler
        for handler in self._handlers:
            if handler.requires_finalizer and match(handler=handler, body=body):
                return True

        return False


class ResourceWatchingRegistry(ResourceRegistry[causation.ResourceWatchingCause]):

    def iter_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[ResourceHandler]:
        for handler in self._handlers:
            if match(handler=handler, body=cause.body, ignore_fields=True):
                yield handler


class ResourceChangingRegistry(ResourceRegistry[causation.ResourceChangingCause]):

    def iter_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        changed_fields = frozenset(field for _, field, _, _ in cause.diff or [])
        for handler in self._handlers:
            if handler.reason is None or handler.reason == cause.reason:
                if handler.initial and not cause.initial:
                    pass  # ignore initial handlers in non-initial causes.
                elif handler.initial and cause.deleted and not handler.deleted:
                    pass  # ignore initial handlers on deletion, unless explicitly marked as usable.
                elif match(handler=handler, body=cause.body, changed_fields=changed_fields):
                    yield handler


class OperatorRegistry:
    """
    A global registry is used for handling of multiple resources & activities.

    It is usually populated by the ``@kopf.on...`` decorators, but can also
    be explicitly created and used in the embedded operators.
    """
    _activity_handlers: ActivityRegistry
    _resource_watching_handlers: MutableMapping[resources_.Resource, ResourceWatchingRegistry]
    _resource_changing_handlers: MutableMapping[resources_.Resource, ResourceChangingRegistry]

    def __init__(self) -> None:
        super().__init__()
        self._activity_handlers = ActivityRegistry()
        self._resource_watching_handlers = collections.defaultdict(ResourceWatchingRegistry)
        self._resource_changing_handlers = collections.defaultdict(ResourceChangingRegistry)

    @property
    def resources(self) -> FrozenSet[resources_.Resource]:
        """ All known resources in the registry. """
        return frozenset(self._resource_watching_handlers) | frozenset(self._resource_changing_handlers)

    def register_activity_handler(
            self,
            fn: ActivityHandlerFn,
            *,
            id: Optional[str] = None,
            errors: Optional[ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            activity: Optional[causation.Activity] = None,
            _fallback: bool = False,
    ) -> ActivityHandlerFn:
        return self._activity_handlers.register(
            fn=fn, id=id, activity=activity,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            _fallback=_fallback,
        )

    def register_resource_watching_handler(
            self,
            group: str,
            version: str,
            plural: str,
            fn: ResourceHandlerFn,
            id: Optional[str] = None,
            labels: Optional[bodies.Labels] = None,
            annotations: Optional[bodies.Annotations] = None,
    ) -> ResourceHandlerFn:
        """
        Register an additional handler function for low-level events.
        """
        resource = resources_.Resource(group, version, plural)
        return self._resource_watching_handlers[resource].register(
            fn=fn, id=id,
            labels=labels, annotations=annotations,
        )

    def register_resource_changing_handler(
            self,
            group: str,
            version: str,
            plural: str,
            fn: ResourceHandlerFn,
            id: Optional[str] = None,
            reason: Optional[causation.Reason] = None,
            event: Optional[str] = None,  # deprecated, use `reason`
            field: Optional[dicts.FieldSpec] = None,
            errors: Optional[ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            initial: Optional[bool] = None,
            deleted: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[bodies.Labels] = None,
            annotations: Optional[bodies.Annotations] = None,
    ) -> ResourceHandlerFn:
        """
        Register an additional handler function for the specific resource and specific reason.
        """
        resource = resources_.Resource(group, version, plural)
        return self._resource_changing_handlers[resource].register(
            reason=reason, event=event, field=field, fn=fn, id=id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            initial=initial, deleted=deleted, requires_finalizer=requires_finalizer,
            labels=labels, annotations=annotations,
        )

    def has_activity_handlers(
            self,
    ) -> bool:
        return bool(self._activity_handlers)

    def has_resource_watching_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        return (resource in self._resource_watching_handlers and
                bool(self._resource_watching_handlers[resource]))

    def has_resource_changing_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        return (resource in self._resource_changing_handlers and
                bool(self._resource_changing_handlers[resource]))

    def get_activity_handlers(
            self,
            *,
            activity: causation.Activity,
    ) -> Sequence[ActivityHandler]:
        return list(_deduplicated(self.iter_activity_handlers(activity=activity)))

    def get_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Sequence[ResourceHandler]:
        return list(_deduplicated(self.iter_resource_watching_handlers(cause=cause)))

    def get_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[ResourceHandler]:
        return list(_deduplicated(self.iter_resource_changing_handlers(cause=cause)))

    def iter_activity_handlers(
            self,
            *,
            activity: causation.Activity,
    ) -> Iterator[ActivityHandler]:
        yield from self._activity_handlers.iter_handlers(activity=activity)

    def iter_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[ResourceHandler]:
        """
        Iterate all handlers for the low-level events.
        """
        if cause.resource in self._resource_watching_handlers:
            yield from self._resource_watching_handlers[cause.resource].iter_handlers(cause=cause)

    def iter_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        """
        Iterate all handlers that match this cause/event, in the order they were registered (even if mixed).
        """
        if cause.resource in self._resource_changing_handlers:
            yield from self._resource_changing_handlers[cause.resource].iter_handlers(cause=cause)

    def get_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Set[dicts.FieldPath]:
        return set(self.iter_extra_fields(resource=resource))

    def iter_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Iterator[dicts.FieldPath]:
        if resource in self._resource_changing_handlers:
            yield from self._resource_changing_handlers[resource].iter_extra_fields()

    def requires_finalizer(
            self,
            resource: resources_.Resource,
            body: bodies.Body,
    ) -> bool:
        """
        Check whether a finalizer should be added to the given resource or not.
        """
        return (resource in self._resource_changing_handlers and
                self._resource_changing_handlers[resource].requires_finalizer(body=body))


class SmartOperatorRegistry(OperatorRegistry):

    def __init__(self) -> None:
        super().__init__()

        try:
            import pykube
        except ImportError:
            pass
        else:
            self.register_activity_handler(
                id='login_via_pykube',
                fn=cast(ActivityHandlerFn, piggybacking.login_via_pykube),
                activity=causation.Activity.AUTHENTICATION,
                errors=ErrorsMode.IGNORED,
                _fallback=True,
            )

        try:
            import kubernetes
        except ImportError:
            pass
        else:
            self.register_activity_handler(
                id='login_via_client',
                fn=cast(ActivityHandlerFn, piggybacking.login_via_client),
                activity=causation.Activity.AUTHENTICATION,
                errors=ErrorsMode.IGNORED,
                _fallback=True,
            )


def generate_id(
        fn: ResourceHandlerFn,
        id: Optional[str],
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
) -> HandlerId:
    real_id: str
    real_id = id if id is not None else get_callable_id(fn)
    real_id = real_id if not suffix else f'{real_id}/{suffix}'
    real_id = real_id if not prefix else f'{prefix}/{real_id}'
    return cast(HandlerId, real_id)


def get_callable_id(c: Optional[Callable[..., Any]]) -> str:
    """ Get an reasonably good id of any commonly used callable. """
    if c is None:
        raise ValueError("Cannot build a persistent id of None.")
    elif isinstance(c, functools.partial):
        return get_callable_id(c.func)
    elif hasattr(c, '__wrapped__'):  # @functools.wraps()
        return get_callable_id(getattr(c, '__wrapped__'))
    elif isinstance(c, FunctionType) and c.__name__ == '<lambda>':
        # The best we can do to keep the id stable across the process restarts,
        # assuming at least no code changes. The code changes are not detectable.
        line = c.__code__.co_firstlineno
        path = c.__code__.co_filename
        return f'lambda:{path}:{line}'
    elif isinstance(c, (FunctionType, MethodType)):
        return str(getattr(c, '__qualname__', getattr(c, '__name__', repr(c))))
    else:
        raise ValueError(f"Cannot get id of {c!r}.")


def _deduplicated(
        handlers: Iterable[HandlerT],
) -> Iterator[HandlerT]:
    """
    Yield the handlers deduplicated.

    The same handler function should not be invoked more than once for one
    single event/cause, even if it is registered with multiple decorators
    (e.g. different filtering criteria or different but same-effect causes).

    One of the ways how this could happen::

        @kopf.on.create(...)
        @kopf.on.resume(...)
        def fn(**kwargs): pass

    In normal cases, the function will be called either on resource creation
    or on operator restart for the pre-existing (already handled) resources.
    When a resource is created during the operator downtime, it is
    both creation and resuming at the same time: the object is new (not yet
    handled) **AND** it is detected as per-existing before operator start.
    But `fn()` should be called only once for this cause.
    """
    seen_ids: Set[int] = set()
    for handler in handlers:
        if id(handler.fn) in seen_ids:
            pass
        else:
            seen_ids.add(id(handler.fn))
            yield handler


def match(
        handler: ResourceHandler,
        body: bodies.Body,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
        ignore_fields: bool = False,
) -> bool:
    return all([
        _matches_field(handler, changed_fields or {}, ignore_fields),
        _matches_labels(handler, body),
        _matches_annotations(handler, body),
    ])


def _matches_field(
        handler: ResourceHandler,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
        ignore_fields: bool = False,
) -> bool:
    return (ignore_fields or
            not handler.field or
            any(field[:len(handler.field)] == handler.field for field in changed_fields))


def _matches_labels(
        handler: ResourceHandler,
        body: bodies.Body,
) -> bool:
    return (not handler.labels or
            _matches_metadata(pattern=handler.labels,
                              content=body.get('metadata', {}).get('labels', {})))


def _matches_annotations(
        handler: ResourceHandler,
        body: bodies.Body,
) -> bool:
    return (not handler.annotations or
            _matches_metadata(pattern=handler.annotations,
                              content=body.get('metadata', {}).get('annotations', {})))


def _matches_metadata(
        *,
        pattern: Mapping[str, str],  # from the handler
        content: Mapping[str, str],  # from the body
) -> bool:
    for key, value in pattern.items():
        if key not in content:
            return False
        elif value is not None and value != content[key]:
            return False
        else:
            continue
    return True


_default_registry: Optional[OperatorRegistry] = None


def get_default_registry() -> OperatorRegistry:
    """
    Get the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    if _default_registry is None:
        # TODO: Deprecated registry to ensure backward-compatibility until removal:
        from kopf.toolkits.legacy_registries import SmartGlobalRegistry
        _default_registry = SmartGlobalRegistry()
    return _default_registry


def set_default_registry(registry: OperatorRegistry) -> None:
    """
    Set the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    _default_registry = registry
