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
import enum
import functools
from types import FunctionType, MethodType
from typing import Any, Callable, Collection, Container, Generic, Iterable, Iterator, List, \
                   Mapping, MutableMapping, Optional, Sequence, Set, Tuple, TypeVar, cast

from kopf.reactor import causation, invocation
from kopf.structs import callbacks, dicts, filters, handlers, ids, references
from kopf.utilities import piggybacking

# We only type-check for known classes of handlers/callbacks, and ignore any custom subclasses.
CauseT = TypeVar('CauseT', bound=causation.BaseCause)
HandlerT = TypeVar('HandlerT', bound=handlers.BaseHandler)
ResourceHandlerT = TypeVar('ResourceHandlerT', bound=handlers.ResourceHandler)
HandlerFnT = TypeVar('HandlerFnT',
                     callbacks.ActivityFn,
                     callbacks.ResourceIndexingFn,
                     callbacks.ResourceWatchingFn,
                     callbacks.ResourceSpawningFn,
                     callbacks.ResourceChangingFn,
                     callbacks.ResourceWebhookFn)


class GenericRegistry(Generic[HandlerFnT, HandlerT]):
    """ A generic base class of a simple registry (with no handler getters). """
    _handlers: List[HandlerT]

    def __init__(self) -> None:
        super().__init__()
        self._handlers = []

    def append(self, handler: HandlerT) -> None:
        self._handlers.append(handler)

    def get_all_handlers(self) -> Collection[HandlerT]:
        return list(self._handlers)


class ActivityRegistry(GenericRegistry[
        callbacks.ActivityFn,
        handlers.ActivityHandler]):

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
        GenericRegistry[HandlerFnT, ResourceHandlerT],
        Generic[CauseT, HandlerFnT, ResourceHandlerT]):

    def has_handlers(
            self,
            resource: references.Resource,
    ) -> bool:
        for handler in self._handlers:
            if _matches_resource(handler, resource):
                return True
        return False

    def get_handlers(
            self,
            cause: CauseT,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Sequence[ResourceHandlerT]:
        return list(_deduplicated(self.iter_handlers(cause=cause, excluded=excluded)))

    @abc.abstractmethod
    def iter_handlers(
            self,
            cause: CauseT,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[ResourceHandlerT]:
        raise NotImplementedError

    def get_extra_fields(
            self,
            resource: references.Resource,
    ) -> Set[dicts.FieldPath]:
        return set(self.iter_extra_fields(resource=resource))

    def iter_extra_fields(
            self,
            resource: references.Resource,
    ) -> Iterator[dicts.FieldPath]:
        for handler in self._handlers:
            if _matches_resource(handler, resource):
                if handler.field:
                    yield handler.field


class ResourceIndexingRegistry(ResourceRegistry[
        causation.ResourceIndexingCause,
        callbacks.ResourceIndexingFn,
        handlers.ResourceIndexingHandler]):

    def iter_handlers(
            self,
            cause: causation.ResourceIndexingCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceIndexingHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if match(handler=handler, cause=cause):
                    yield handler


class ResourceWatchingRegistry(ResourceRegistry[
        causation.ResourceWatchingCause,
        callbacks.ResourceWatchingFn,
        handlers.ResourceWatchingHandler]):

    def iter_handlers(
            self,
            cause: causation.ResourceWatchingCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if match(handler=handler, cause=cause):
                    yield handler


class ResourceSpawningRegistry(ResourceRegistry[
        causation.ResourceSpawningCause,
        callbacks.ResourceSpawningFn,
        handlers.ResourceSpawningHandler]):

    def iter_handlers(
            self,
            cause: causation.ResourceSpawningCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceSpawningHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if match(handler=handler, cause=cause):
                    yield handler

    def requires_finalizer(
            self,
            cause: causation.ResourceSpawningCause,
            excluded: Container[ids.HandlerId] = frozenset(),
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


class ResourceChangingRegistry(ResourceRegistry[
        causation.ResourceChangingCause,
        callbacks.ResourceChangingFn,
        handlers.ResourceChangingHandler]):

    def iter_handlers(
            self,
            cause: causation.ResourceChangingCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceChangingHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                if handler.reason is None or handler.reason == cause.reason:
                    if handler.initial and not cause.initial:
                        pass  # skip initial handlers in non-initial causes.
                    elif handler.initial and cause.deleted and not handler.deleted:
                        pass  # skip initial handlers on deletion, unless explicitly marked as used.
                    elif match(handler=handler, cause=cause):
                        yield handler

    def requires_finalizer(
            self,
            cause: causation.ResourceCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> bool:
        """
        Check whether a finalizer should be added to the given resource or not.
        """
        # check whether the body matches a deletion handler
        for handler in self._handlers:
            if handler.id not in excluded:
                if handler.requires_finalizer and prematch(handler=handler, cause=cause):
                    return True
        return False

    def prematch(
            self,
            cause: causation.ResourceChangingCause,
    ) -> bool:
        for handler in self._handlers:
            if prematch(handler=handler, cause=cause):
                return True
        return False

    def get_resource_handlers(
            self,
            resource: references.Resource,
    ) -> Sequence[handlers.ResourceChangingHandler]:
        found_handlers: List[handlers.ResourceChangingHandler] = []
        for handler in self._handlers:
            if _matches_resource(handler, resource):
                found_handlers.append(handler)
        return list(_deduplicated(found_handlers))


class ResourceWebhooksRegistry(ResourceRegistry[
        causation.ResourceWebhookCause,
        callbacks.ResourceWebhookFn,
        handlers.ResourceWebhookHandler]):

    def iter_handlers(
            self,
            cause: causation.ResourceWebhookCause,
            excluded: Container[ids.HandlerId] = frozenset(),
    ) -> Iterator[handlers.ResourceWebhookHandler]:
        for handler in self._handlers:
            if handler.id not in excluded:
                # Only the handlers for the hinted webhook, if possible; if not hinted, then all.
                matching_reason = cause.reason is None or cause.reason == handler.reason
                matching_webhook = cause.webhook is None or cause.webhook == handler.id
                if matching_reason and matching_webhook:
                    # For deletion, exclude all mutation handlers unless explicitly enabled.
                    non_mutating = handler.reason != handlers.WebhookType.MUTATING
                    non_deletion = cause.operation != 'DELETE'
                    explicitly_for_deletion = handler.operation == 'DELETE'
                    if non_mutating or non_deletion or explicitly_for_deletion:
                        # Filter by usual criteria: labels, annotations, fields, callbacks.
                        if match(handler=handler, cause=cause):
                            yield handler


class OperatorRegistry:
    """
    A global registry is used for handling of multiple resources & activities.

    It is usually populated by the ``@kopf.on...`` decorators, but can also
    be explicitly created and used in the embedded operators.
    """
    def __init__(self) -> None:
        super().__init__()
        self._activities = ActivityRegistry()
        self._resource_indexing = ResourceIndexingRegistry()
        self._resource_watching = ResourceWatchingRegistry()
        self._resource_spawning = ResourceSpawningRegistry()
        self._resource_changing = ResourceChangingRegistry()
        self._resource_webhooks = ResourceWebhooksRegistry()


class SmartOperatorRegistry(OperatorRegistry):
    def __init__(self) -> None:
        super().__init__()
        try:
            import pykube
        except ImportError:
            pass
        else:
            self._activities.append(handlers.ActivityHandler(
                id=ids.HandlerId('login_via_pykube'),
                fn=cast(callbacks.ActivityFn, piggybacking.login_via_pykube),
                activity=handlers.Activity.AUTHENTICATION,
                errors=handlers.ErrorsMode.IGNORED,
                param=None, timeout=None, retries=None, backoff=None,
                _fallback=True,
            ))
        try:
            import kubernetes
        except ImportError:
            pass
        else:
            self._activities.append(handlers.ActivityHandler(
                id=ids.HandlerId('login_via_client'),
                fn=cast(callbacks.ActivityFn, piggybacking.login_via_client),
                activity=handlers.Activity.AUTHENTICATION,
                errors=handlers.ErrorsMode.IGNORED,
                param=None, timeout=None, retries=None, backoff=None,
                _fallback=True,
            ))


def generate_id(
        fn: Callable[..., Any],
        id: Optional[str],
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
) -> ids.HandlerId:
    real_id: str
    real_id = id if id is not None else get_callable_id(fn)
    real_id = real_id if not suffix else f'{real_id}/{suffix}'
    real_id = real_id if not prefix else f'{prefix}/{real_id}'
    return cast(ids.HandlerId, real_id)


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
        src: Iterable[HandlerT],
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
    seen_ids: Set[Tuple[int, ids.HandlerId]] = set()
    for handler in src:
        key = (id(handler.fn), handler.id)
        if key in seen_ids:
            pass
        else:
            seen_ids.add(key)
            yield handler


def prematch(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
) -> bool:
    # Kwargs are lazily evaluated on the first _actual_ use, and shared for all filters since then.
    kwargs: MutableMapping[str, Any] = {}
    return (
        _matches_resource(handler, cause.resource) and
        _matches_labels(handler, cause, kwargs) and
        _matches_annotations(handler, cause, kwargs) and
        _matches_field_values(handler, cause, kwargs) and
        _matches_filter_callback(handler, cause, kwargs)  # the callback comes in the end!
    )


def match(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
) -> bool:
    # Kwargs are lazily evaluated on the first _actual_ use, and shared for all filters since then.
    kwargs: MutableMapping[str, Any] = {}
    return (
        _matches_resource(handler, cause.resource) and
        _matches_labels(handler, cause, kwargs) and
        _matches_annotations(handler, cause, kwargs) and
        _matches_field_values(handler, cause, kwargs) and
        _matches_field_changes(handler, cause, kwargs) and
        _matches_filter_callback(handler, cause, kwargs)  # the callback comes in the end!
    )


def _matches_resource(
        handler: handlers.ResourceHandler,
        resource: references.Resource,
) -> bool:
    return (handler.selector is None or
            handler.selector.check(resource))


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


def _matches_field_values(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        kwargs: MutableMapping[str, Any],
) -> bool:
    if not handler.field:
        return True

    if not kwargs:
        kwargs.update(invocation.build_kwargs(cause=cause))

    absent = _UNSET.token  # or any other identifyable object
    if isinstance(cause, causation.ResourceChangingCause):
        # For on.update/on.field, so as for on.create/resume/delete for uniformity and simplicity:
        old = dicts.resolve(cause.old, handler.field, absent)
        new = dicts.resolve(cause.new, handler.field, absent)
        values = [new, old]  # keep "new" first, to avoid "old" callbacks if "new" works.
    else:
        # For event-watching, timers/daemons (could also work for on.create/resume/delete):
        val = dicts.resolve(cause.body, handler.field, absent)
        values = [val]
    return (
        (handler.value is None and any(value is not absent for value in values)) or
        (handler.value is filters.PRESENT and any(value is not absent for value in values)) or
        (handler.value is filters.ABSENT and any(value is absent for value in values)) or
        (callable(handler.value) and any(handler.value(value, **kwargs) for value in values)) or
        (any(handler.value == value for value in values))
    )


def _matches_field_changes(
        handler: handlers.ResourceHandler,
        cause: causation.ResourceCause,
        kwargs: MutableMapping[str, Any],
) -> bool:
    if not isinstance(handler, handlers.ResourceChangingHandler):
        return True
    if not isinstance(cause, causation.ResourceChangingCause):
        return True
    if not handler.field:
        return True

    if not kwargs:
        kwargs.update(invocation.build_kwargs(cause=cause))

    absent = _UNSET.token  # or any other identifyable object
    old = dicts.resolve(cause.old, handler.field, absent)
    new = dicts.resolve(cause.new, handler.field, absent)
    return ((
        not handler.field_needs_change or
        old != new  # ... or there IS a change.
    ) and (
        (handler.old is None) or
        (handler.old is filters.ABSENT and old is absent) or
        (handler.old is filters.PRESENT and old is not absent) or
        (callable(handler.old) and handler.old(old, **kwargs)) or
        (handler.old == old)
    ) and (
        (handler.new is None) or
        (handler.new is filters.ABSENT and new is absent) or
        (handler.new is filters.PRESENT and new is not absent) or
        (callable(handler.new) and handler.new(new, **kwargs)) or
        (handler.new == new)
    ))


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


class _UNSET(enum.Enum):
    token = enum.auto()


_default_registry: Optional[OperatorRegistry] = None


def get_default_registry() -> OperatorRegistry:
    """
    Get the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = SmartOperatorRegistry()
    return _default_registry


def set_default_registry(registry: OperatorRegistry) -> None:
    """
    Set the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    _default_registry = registry
