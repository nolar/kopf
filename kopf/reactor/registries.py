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
import functools
import warnings
from types import FunctionType, MethodType
from typing import (Any, MutableMapping, Optional, Sequence, Collection, Iterable, Iterator,
                    List, Set, FrozenSet, Mapping, Callable, cast, Generic, TypeVar, Union,
                    Container)

from kopf.reactor import causation
from kopf.reactor import invocation
from kopf.structs import callbacks
from kopf.structs import dicts
from kopf.structs import filters
from kopf.structs import handlers
from kopf.structs import resources as resources_
from kopf.utilities import piggybacking

# We only type-check for known classes of handlers/callbacks, and ignore any custom subclasses.
CauseT = TypeVar('CauseT', bound=causation.BaseCause)
HandlerT = TypeVar('HandlerT', bound=handlers.BaseHandler)
ResourceHandlerT = TypeVar('ResourceHandlerT', bound=handlers.ResourceHandler)
HandlerFnT = TypeVar('HandlerFnT',
                     callbacks.ActivityFn,
                     callbacks.ResourceWatchingFn,
                     callbacks.ResourceSpawningFn,
                     callbacks.ResourceChangingFn,
                     Union[callbacks.ResourceWatchingFn, callbacks.ResourceChangingFn])  # DEPRECATED: for legacy_registries


class GenericRegistry(Generic[HandlerFnT, HandlerT]):
    """ A generic base class of a simple registry (with no handler getters). """
    _handlers: List[HandlerT]

    def __init__(self) -> None:
        super().__init__()
        self._handlers = []

    def __bool__(self) -> bool:
        return bool(self._handlers)

    def append(self, handler: HandlerT) -> None:
        self._handlers.append(handler)


class ActivityRegistry(GenericRegistry[
        callbacks.ActivityFn,
        handlers.ActivityHandler]):

    def register(
            self,
            fn: callbacks.ActivityFn,
            *,
            id: Optional[str] = None,
            errors: Optional[handlers.ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            activity: Optional[handlers.Activity] = None,
            _fallback: bool = False,
    ) -> callbacks.ActivityFn:
        warnings.warn("registry.register() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)
        real_id = generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            id=real_id, fn=fn, activity=activity,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            _fallback=_fallback,
        )
        self.append(handler)
        return fn

    def get_handlers(
            self,
            activity: handlers.Activity,
    ) -> Sequence[handlers.ActivityHandler]:
        return list(_deduplicated(self.iter_handlers(activity=activity)))

    def iter_handlers(
            self,
            activity: handlers.Activity,
    ) -> Iterator[handlers.ActivityHandler]:
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


class ResourceRegistry(
        Generic[CauseT, HandlerFnT, ResourceHandlerT],
        GenericRegistry[HandlerFnT, ResourceHandlerT]):

    def get_handlers(
            self,
            cause: CauseT,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> Sequence[ResourceHandlerT]:
        return list(_deduplicated(self.iter_handlers(cause=cause, excluded=excluded)))

    @abc.abstractmethod
    def iter_handlers(
            self,
            cause: CauseT,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> Iterator[ResourceHandlerT]:
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
            cause: causation.ResourceCause,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> bool:
        """
        Check whether a finalizer should be added to the given resource or not.
        """
        # check whether the body matches a deletion handler
        for handler in self._handlers:
            if handler.id not in excluded:
                if handler.requires_finalizer and match(handler=handler, cause=cause):
                    return True
        return False


class ResourceWatchingRegistry(ResourceRegistry[
        causation.ResourceWatchingCause,
        callbacks.ResourceWatchingFn,
        handlers.ResourceWatchingHandler]):

    def register(
            self,
            fn: callbacks.ResourceWatchingFn,
            *,
            id: Optional[str] = None,
            errors: Optional[handlers.ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            labels: Optional[filters.MetaFilter] = None,
            annotations: Optional[filters.MetaFilter] = None,
            when: Optional[callbacks.WhenFilterFn] = None,
    ) -> callbacks.ResourceWatchingFn:
        warnings.warn("registry.register() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)

        real_id = generate_id(fn=fn, id=id)
        handler = handlers.ResourceWatchingHandler(
            id=real_id, fn=fn,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
        )
        self.append(handler)
        return fn

    def iter_handlers(
            self,
            cause: causation.ResourceWatchingCause,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if match(handler=handler, cause=cause, ignore_fields=True):
                    yield handler


class ResourceSpawningRegistry(ResourceRegistry[
        causation.ResourceSpawningCause,
        callbacks.ResourceSpawningFn,
        handlers.ResourceSpawningHandler]):

    @abc.abstractmethod
    def iter_handlers(
            self,
            cause: causation.ResourceSpawningCause,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceSpawningHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if match(handler=handler, cause=cause):
                    yield handler


class ResourceChangingRegistry(ResourceRegistry[
        causation.ResourceChangingCause,
        callbacks.ResourceChangingFn,
        handlers.ResourceChangingHandler]):

    def register(
            self,
            fn: callbacks.ResourceChangingFn,
            *,
            id: Optional[str] = None,
            reason: Optional[handlers.Reason] = None,
            event: Optional[str] = None,  # deprecated, use `reason`
            field: Optional[dicts.FieldSpec] = None,
            errors: Optional[handlers.ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            initial: Optional[bool] = None,
            deleted: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[filters.MetaFilter] = None,
            annotations: Optional[filters.MetaFilter] = None,
            when: Optional[callbacks.WhenFilterFn] = None,
    ) -> callbacks.ResourceChangingFn:
        warnings.warn("registry.register() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)

        if reason is None and event is not None:
            reason = handlers.Reason(event)

        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        handler = handlers.ResourceChangingHandler(
            id=real_id, fn=fn, reason=reason, field=real_field,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            initial=initial, deleted=deleted, requires_finalizer=requires_finalizer,
            labels=labels, annotations=annotations, when=when,
        )

        self.append(handler)
        return fn

    def iter_handlers(
            self,
            cause: causation.ResourceChangingCause,
            excluded: Container[handlers.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceChangingHandler]:
        changed_fields = frozenset(field for _, field, _, _ in cause.diff or [])
        for handler in self._handlers:
            if handler.id not in excluded:
                if handler.reason is None or handler.reason == cause.reason:
                    if handler.initial and not cause.initial:
                        pass  # skip initial handlers in non-initial causes.
                    elif handler.initial and cause.deleted and not handler.deleted:
                        pass  # skip initial handlers on deletion, unless explicitly marked as used.
                    elif match(handler=handler, cause=cause, changed_fields=changed_fields):
                        yield handler


class OperatorRegistry:
    """
    A global registry is used for handling of multiple resources & activities.

    It is usually populated by the ``@kopf.on...`` decorators, but can also
    be explicitly created and used in the embedded operators.
    """
    activity_handlers: ActivityRegistry
    resource_watching_handlers: MutableMapping[resources_.Resource, ResourceWatchingRegistry]
    resource_spawning_handlers: MutableMapping[resources_.Resource, ResourceSpawningRegistry]
    resource_changing_handlers: MutableMapping[resources_.Resource, ResourceChangingRegistry]

    def __init__(self) -> None:
        super().__init__()
        self.activity_handlers = ActivityRegistry()
        self.resource_watching_handlers = collections.defaultdict(ResourceWatchingRegistry)
        self.resource_spawning_handlers = collections.defaultdict(ResourceSpawningRegistry)
        self.resource_changing_handlers = collections.defaultdict(ResourceChangingRegistry)

    @property
    def resources(self) -> FrozenSet[resources_.Resource]:
        """ All known resources in the registry. """
        return (frozenset(self.resource_watching_handlers) |
                frozenset(self.resource_spawning_handlers) |
                frozenset(self.resource_changing_handlers))

    #
    # Everything below is deprecated and will be removed in the next major release.
    #

    def register_activity_handler(
            self,
            fn: callbacks.ActivityFn,
            *,
            id: Optional[str] = None,
            errors: Optional[handlers.ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            activity: Optional[handlers.Activity] = None,
            _fallback: bool = False,
    ) -> callbacks.ActivityFn:
        warnings.warn("registry.register_activity_handler() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)
        return self.activity_handlers.register(
            fn=fn, id=id, activity=activity,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            _fallback=_fallback,
        )

    def register_resource_watching_handler(
            self,
            group: str,
            version: str,
            plural: str,
            fn: callbacks.ResourceWatchingFn,
            id: Optional[str] = None,
            labels: Optional[filters.MetaFilter] = None,
            annotations: Optional[filters.MetaFilter] = None,
            when: Optional[callbacks.WhenFilterFn] = None,
    ) -> callbacks.ResourceWatchingFn:
        """
        Register an additional handler function for low-level events.
        """
        warnings.warn("registry.register_resource_watching_handler() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)
        resource = resources_.Resource(group, version, plural)
        return self.resource_watching_handlers[resource].register(
            fn=fn, id=id,
            labels=labels, annotations=annotations, when=when,
        )

    def register_resource_changing_handler(
            self,
            group: str,
            version: str,
            plural: str,
            fn: callbacks.ResourceChangingFn,
            id: Optional[str] = None,
            reason: Optional[handlers.Reason] = None,
            event: Optional[str] = None,  # deprecated, use `reason`
            field: Optional[dicts.FieldSpec] = None,
            errors: Optional[handlers.ErrorsMode] = None,
            timeout: Optional[float] = None,
            retries: Optional[int] = None,
            backoff: Optional[float] = None,
            cooldown: Optional[float] = None,  # deprecated, use `backoff`
            initial: Optional[bool] = None,
            deleted: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[filters.MetaFilter] = None,
            annotations: Optional[filters.MetaFilter] = None,
            when: Optional[callbacks.WhenFilterFn] = None,
    ) -> callbacks.ResourceChangingFn:
        """
        Register an additional handler function for the specific resource and specific reason.
        """
        warnings.warn("registry.register_resource_changing_handler() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)
        resource = resources_.Resource(group, version, plural)
        return self.resource_changing_handlers[resource].register(
            reason=reason, event=event, field=field, fn=fn, id=id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            initial=initial, deleted=deleted, requires_finalizer=requires_finalizer,
            labels=labels, annotations=annotations, when=when,
        )

    def has_activity_handlers(
            self,
    ) -> bool:
        warnings.warn("registry.has_activity_handlers() is deprecated; "
                      "use registry.activity_handlers directly.",
                      DeprecationWarning)
        return bool(self.activity_handlers)

    def has_resource_watching_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        warnings.warn("registry.has_resource_watching_handlers() is deprecated; "
                      "use registry.resource_watching_handlers[resource] directly.",
                      DeprecationWarning)
        return bool(self.resource_watching_handlers[resource])

    def has_resource_changing_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        warnings.warn("registry.has_resource_changing_handlers() is deprecated; "
                      "use registry.resource_changing_handlers[resource] directly.",
                      DeprecationWarning)
        return bool(self.resource_changing_handlers[resource])

    def get_activity_handlers(
            self,
            *,
            activity: handlers.Activity,
    ) -> Sequence[handlers.ActivityHandler]:
        warnings.warn("registry.get_activity_handlers() is deprecated; "
                      "use registry.activity_handlers.get_handlers().",
                      DeprecationWarning)
        return self.activity_handlers.get_handlers(activity=activity)

    def get_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Sequence[handlers.ResourceWatchingHandler]:
        warnings.warn("registry.get_resource_watching_handlers() is deprecated; "
                      "use registry.resource_watching_handlers[resource].get_handlers().",
                      DeprecationWarning)
        return self.resource_watching_handlers[cause.resource].get_handlers(cause=cause)

    def get_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[handlers.ResourceChangingHandler]:
        warnings.warn("registry.get_resource_changing_handlers() is deprecated; "
                      "use registry.resource_changing_handlers[resource].get_handlers().",
                      DeprecationWarning)
        return self.resource_changing_handlers[cause.resource].get_handlers(cause=cause)

    def iter_activity_handlers(
            self,
            *,
            activity: handlers.Activity,
    ) -> Iterator[handlers.ActivityHandler]:
        warnings.warn("registry.iter_activity_handlers() is deprecated; "
                      "use registry.activity_handlers.iter_handlers().",
                      DeprecationWarning)
        yield from self.activity_handlers.iter_handlers(activity=activity)

    def iter_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        """
        Iterate all handlers for the low-level events.
        """
        warnings.warn("registry.iter_resource_watching_handlers() is deprecated; "
                      "use registry.resource_watching_handlers[resource].iter_handlers().",
                      DeprecationWarning)
        yield from self.resource_watching_handlers[cause.resource].iter_handlers(cause=cause)

    def iter_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[handlers.ResourceChangingHandler]:
        """
        Iterate all handlers that match this cause/event, in the order they were registered (even if mixed).
        """
        warnings.warn("registry.iter_resource_changing_handlers() is deprecated; "
                      "use registry.resource_changing_handlers[resource].iter_handlers().",
                      DeprecationWarning)
        yield from self.resource_changing_handlers[cause.resource].iter_handlers(cause=cause)

    def get_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Set[dicts.FieldPath]:
        warnings.warn("registry.get_extra_fields() is deprecated; "
                      "use registry.resource_changing_handlers[resource].get_extra_fields().",
                      DeprecationWarning)
        return self.resource_changing_handlers[resource].get_extra_fields()

    def iter_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Iterator[dicts.FieldPath]:
        warnings.warn("registry.iter_extra_fields() is deprecated; "
                      "use registry.resource_changing_handlers[resource].iter_extra_fields().",
                      DeprecationWarning)
        yield from self.resource_changing_handlers[resource].iter_extra_fields()

    def requires_finalizer(
            self,
            resource: resources_.Resource,
            cause: causation.ResourceCause,
    ) -> bool:
        """
        Check whether a finalizer should be added to the given resource or not.
        """
        warnings.warn("registry.requires_finalizer() is deprecated; "
                      "use registry.resource_changing_handlers[resource].requires_finalizer().",
                      DeprecationWarning)
        return self.resource_changing_handlers[resource].requires_finalizer(cause=cause)


class SmartOperatorRegistry(OperatorRegistry):

    def __init__(self) -> None:
        super().__init__()

        try:
            import pykube
        except ImportError:
            pass
        else:
            self.activity_handlers.append(handlers.ActivityHandler(
                id=handlers.HandlerId('login_via_pykube'),
                fn=cast(callbacks.ActivityFn, piggybacking.login_via_pykube),
                activity=handlers.Activity.AUTHENTICATION,
                errors=handlers.ErrorsMode.IGNORED,
                timeout=None, retries=None, backoff=None, cooldown=None,
                _fallback=True,
            ))
        try:
            import kubernetes
        except ImportError:
            pass
        else:
            self.activity_handlers.append(handlers.ActivityHandler(
                id=handlers.HandlerId('login_via_client'),
                fn=cast(callbacks.ActivityFn, piggybacking.login_via_client),
                activity=handlers.Activity.AUTHENTICATION,
                errors=handlers.ErrorsMode.IGNORED,
                timeout=None, retries=None, backoff=None, cooldown=None,
                _fallback=True,
            ))


def generate_id(
        fn: Callable[..., Any],
        id: Optional[str],
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
) -> handlers.HandlerId:
    real_id: str
    real_id = id if id is not None else get_callable_id(fn)
    real_id = real_id if not suffix else f'{real_id}/{suffix}'
    real_id = real_id if not prefix else f'{prefix}/{real_id}'
    return cast(handlers.HandlerId, real_id)


def get_callable_id(c: Callable[..., Any]) -> str:
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
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
        ignore_fields: bool = False,
) -> bool:
    # Kwargs are lazily evaluated on the first _actual_ use, and shared for all filters since then.
    kwargs: MutableMapping[str, Any] = {}
    return all([
        _matches_field(handler, changed_fields or {}, ignore_fields),
        _matches_labels(handler, cause, kwargs),
        _matches_annotations(handler, cause, kwargs),
        _matches_filter_callback(handler, cause, kwargs),
    ])


def _matches_field(
        handler: handlers.ResourceHandler,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
        ignore_fields: bool = False,
) -> bool:
    return (ignore_fields or
            not isinstance(handler, handlers.ResourceChangingHandler) or
            not handler.field or
            any(changed_field[:len(handler.field)] == handler.field or  # a.b.c -vs- a.b => ignore c
                changed_field == handler.field[:len(changed_field)]     # a.b -vs- a.b.c => ignore c
                for changed_field in changed_fields))


def _matches_labels(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        kwargs: MutableMapping[str, Any],
) -> bool:
    return (not handler.labels or
            _matches_metadata(pattern=handler.labels,
                              content=cause.body.get('metadata', {}).get('labels', {}),
                              kwargs=kwargs, cause=cause))


def _matches_annotations(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        kwargs: MutableMapping[str, Any],
) -> bool:
    return (not handler.annotations or
            _matches_metadata(pattern=handler.annotations,
                              content=cause.body.get('metadata', {}).get('annotations', {}),
                              kwargs=kwargs, cause=cause))


def _matches_metadata(
        *,
        pattern: filters.MetaFilter,  # from the handler
        content: Mapping[str, str],  # from the body
        kwargs: MutableMapping[str, Any],
        cause: causation.ResourceCause,
) -> bool:
    for key, value in pattern.items():
        if value is filters.MetaFilterToken.ABSENT and key not in content:
            continue
        elif value is filters.MetaFilterToken.PRESENT and key in content:
            continue
        elif value is None and key in content:  # deprecated; warned in @kopf.on
            continue
        elif callable(value):
            if not kwargs:
                kwargs.update(invocation.build_kwargs(cause=cause))
            if value(content.get(key, None), **kwargs):
                continue
            else:
                return False
        elif key not in content:
            return False
        elif value != content[key]:
            return False
        else:
            continue
    return True


def _matches_filter_callback(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        kwargs: MutableMapping[str, Any],
) -> bool:
    if handler.when is None:
        return True
    if not kwargs:
        kwargs.update(invocation.build_kwargs(cause=cause))
    return handler.when(**kwargs)


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
