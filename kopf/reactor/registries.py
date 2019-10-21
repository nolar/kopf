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
import functools
import logging
import warnings
from types import FunctionType, MethodType
from typing import (Any, MutableMapping, Optional, Sequence, Collection, Iterable, Iterator,
                    NamedTuple, Union, List, Set, FrozenSet, Mapping, NewType, cast)

from typing_extensions import Protocol

from kopf.reactor import causation
from kopf.structs import bodies
from kopf.structs import dicts
from kopf.structs import diffs
from kopf.structs import patches
from kopf.structs import resources as resources_

# Strings are taken from the users, but then tainted as this type for stricter type-checking:
# to prevent usage of some other strings (e.g. operator id) as the handlers ids.
HandlerId = NewType('HandlerId', str)


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
    ) -> Any: ...


# A registered handler (function + event meta info).
class ResourceHandler(NamedTuple):
    fn: ResourceHandlerFn
    id: HandlerId
    reason: Optional[causation.Reason]
    field: Optional[dicts.FieldPath]
    timeout: Optional[float] = None
    initial: Optional[bool] = None
    labels: Optional[bodies.Labels] = None
    annotations: Optional[bodies.Annotations] = None

    @property
    def event(self) -> Optional[causation.Reason]:
        warnings.warn("`handler.event` is deprecated; use `handler.reason`.", DeprecationWarning)
        return self.reason



class BaseRegistry(metaclass=abc.ABCMeta):
    """
    A registry stores the handlers and provides them to the reactor.
    """

    def get_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Sequence[ResourceHandler]:
        return list(self._deduplicated(self.iter_resource_watching_handlers(cause=cause)))

    def get_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[ResourceHandler]:
        return list(self._deduplicated(self.iter_resource_changing_handlers(cause=cause)))

    @abc.abstractmethod
    def iter_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[ResourceHandler]:
        pass

    @abc.abstractmethod
    def iter_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        pass

    def get_extra_fields(self, resource: resources_.Resource) -> Set[dicts.FieldPath]:
        return set(self.iter_extra_fields(resource=resource))

    @abc.abstractmethod
    def iter_extra_fields(self, resource: resources_.Resource) -> Iterator[dicts.FieldPath]:
        pass

    @staticmethod
    def _deduplicated(handlers: Iterable[ResourceHandler]) -> Iterator[ResourceHandler]:
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

    #
    # Backward-compatibility of a semi-public interface: registries are exposed,
    # but these methods were not documented or demonstrated, but could be used.
    #

    def get_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.Event,
    ) -> Sequence[ResourceHandler]:
        warnings.warn("registry.get_event_handlers() is deprecated; "
                      "use registry.get_resource_watching_handlers().", DeprecationWarning)
        return list(self._deduplicated(self.iter_event_handlers(resource=resource, event=event)))

    def get_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[ResourceHandler]:
        warnings.warn("registry.get_cause_handlers() is deprecated; "
                      "use registry.get_resource_changing_handlers().", DeprecationWarning)
        return self.get_resource_changing_handlers(cause=cause)

    def iter_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.Event,
    ) -> Iterator[ResourceHandler]:
        warnings.warn("registry.iter_event_handlers() is deprecated; "
                      "use registry.iter_resource_watching_handlers().", DeprecationWarning)
        return self.iter_resource_watching_handlers(cause=causation.detect_resource_watching_cause(
            resource=resource,
            event=event,
            patch=patches.Patch(),          # unused
            type=event['type'],             # unused
            body=event['object'],           # unused
            raw=event,                      # unused
            logger=logging.Logger('kopf'),  # unused
        ))

    def iter_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        warnings.warn("registry.iter_cause_handlers() is deprecated; "
                      "use registry.iter_resource_changing_handlers().", DeprecationWarning)
        return self.iter_resource_changing_handlers(cause=cause)


class ResourceRegistry(BaseRegistry):
    """
    A simple registry is just a list of handlers, no grouping.
    """

    def __init__(self, prefix: Optional[str] = None) -> None:
        super().__init__()
        self.prefix = prefix
        self._handlers: List[ResourceHandler] = []
        self._handlers_requiring_finalizer: List[ResourceHandler] = []

    def __bool__(self) -> bool:
        return bool(self._handlers)

    def append(self, handler: ResourceHandler) -> None:
        self._handlers.append(handler)

    def register(
            self,
            fn: ResourceHandlerFn,
            id: Optional[str] = None,
            reason: Optional[causation.Reason] = None,
            event: Optional[str] = None,  # deprecated, use `reason`
            field: Optional[dicts.FieldSpec] = None,
            timeout: Optional[float] = None,
            initial: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[bodies.Labels] = None,
            annotations: Optional[bodies.Annotations] = None,
    ) -> ResourceHandlerFn:
        if reason is None and event is not None:
            reason = causation.Reason(event)

        if field is None:
            field = None  # for the non-field events
        elif isinstance(field, str):
            field = tuple(field.split('.'))
        elif isinstance(field, (list, tuple)):
            field = tuple(field)
        else:
            raise ValueError(f"Field must be either a str, or a list/tuple. Got {field!r}")

        real_id: HandlerId
        real_id = cast(HandlerId, id) if id is not None else cast(HandlerId, get_callable_id(fn))
        real_id = real_id if field is None else cast(HandlerId, f'{real_id}/{".".join(field)}')
        real_id = real_id if self.prefix is None else cast(HandlerId, f'{self.prefix}/{real_id}')
        handler = ResourceHandler(
            id=real_id, fn=fn, reason=reason, field=field, timeout=timeout,
            initial=initial,
            labels=labels, annotations=annotations,
        )

        self.append(handler)

        if requires_finalizer:
            self._handlers_requiring_finalizer.append(handler)

        return fn  # to be usable as a decorator too.

    def iter_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[ResourceHandler]:
        for handler in self._handlers:
            if match(handler=handler, body=cause.body):
                yield handler

    def iter_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        changed_fields = frozenset(field for _, field, _, _ in cause.diff or [])
        for handler in self._handlers:
            if handler.reason is None or handler.reason == cause.reason:
                if handler.initial and not cause.initial:
                    pass  # ignore initial handlers in non-initial causes.
                elif match(handler=handler, body=cause.body, changed_fields=changed_fields):
                    yield handler

    def iter_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Iterator[dicts.FieldPath]:
        for handler in self._handlers:
            if handler.field:
                yield handler.field

    def requires_finalizer(
            self,
            resource: resources_.Resource,
            body: bodies.Body,
    ) -> bool:
        # check whether the body matches a deletion handler
        for handler in self._handlers_requiring_finalizer:
            if match(handler=handler, body=body):
                return True

        return False


def get_callable_id(c: ResourceHandlerFn) -> str:
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


class OperatorRegistry(BaseRegistry):
    """
    A global registry is used for handling of the multiple resources.
    It is usually populated by the `@kopf.on...` decorators.
    """
    _resource_watching_handlers: MutableMapping[resources_.Resource, ResourceRegistry]
    _resource_changing_handlers: MutableMapping[resources_.Resource, ResourceRegistry]

    def __init__(self) -> None:
        super().__init__()
        self._resource_watching_handlers = {}
        self._resource_changing_handlers = {}

    @property
    def resources(self) -> FrozenSet[resources_.Resource]:
        """ All known resources in the registry. """
        return frozenset(self._resource_watching_handlers) | frozenset(self._resource_changing_handlers)

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
        registry = self._resource_watching_handlers.setdefault(resource, ResourceRegistry())
        registry.register(fn=fn, id=id, labels=labels, annotations=annotations)
        return fn  # to be usable as a decorator too.

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
            timeout: Optional[float] = None,
            initial: Optional[bool] = None,
            requires_finalizer: bool = False,
            labels: Optional[bodies.Labels] = None,
            annotations: Optional[bodies.Annotations] = None,
    ) -> ResourceHandlerFn:
        """
        Register an additional handler function for the specific resource and specific reason.
        """
        resource = resources_.Resource(group, version, plural)
        registry = self._resource_changing_handlers.setdefault(resource, ResourceRegistry())
        registry.register(reason=reason, event=event, field=field, fn=fn, id=id, timeout=timeout,
                          initial=initial, requires_finalizer=requires_finalizer,
                          labels=labels, annotations=annotations)
        return fn  # to be usable as a decorator too.

    def has_resource_watching_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        resource_registry = self._resource_watching_handlers.get(resource, None)
        return bool(resource_registry)

    def has_resource_changing_handlers(
            self,
            resource: resources_.Resource,
    ) -> bool:
        resource_registry = self._resource_changing_handlers.get(resource, None)
        return bool(resource_registry)

    def iter_resource_watching_handlers(
            self,
            cause: causation.ResourceWatchingCause,
    ) -> Iterator[ResourceHandler]:
        """
        Iterate all handlers for the low-level events.
        """
        resource_registry = self._resource_watching_handlers.get(cause.resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_resource_watching_handlers(cause=cause)

    def iter_resource_changing_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[ResourceHandler]:
        """
        Iterate all handlers that match this cause/event, in the order they were registered (even if mixed).
        """
        resource_registry = self._resource_changing_handlers.get(cause.resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_resource_changing_handlers(cause=cause)

    def iter_extra_fields(
            self,
            resource: resources_.Resource,
    ) -> Iterator[dicts.FieldPath]:
        resource_registry = self._resource_changing_handlers.get(resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_extra_fields(resource=resource)

    def requires_finalizer(
            self,
            resource: resources_.Resource,
            body: bodies.Body,
    ) -> bool:
        """
        Return whether a finalizer should be added to
        the given resource or not.
        """
        resource_registry = self._resource_changing_handlers.get(resource, None)
        if resource_registry is None:
            return False
        return resource_registry.requires_finalizer(resource, body)

    #
    # Backward-compatibility of a semi-public interface: registries are exposed,
    # but these methods were not documented or demonstrated, but could be used.
    #

    def register_event_handler(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("registry.register_event_handler() is deprecated; "
                      "use registry.register_resource_watching_handler().", DeprecationWarning)
        return self.register_resource_watching_handler(*args, **kwargs)

    def register_cause_handler(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("registry.register_cause_handler() is deprecated; "
                      "use registry.register_resource_changing_handler().", DeprecationWarning)
        return self.register_resource_changing_handler(*args, **kwargs)

    def has_event_handlers(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("registry.has_event_handlers() is deprecated; "
                      "use registry.has_resource_watching_handlers().", DeprecationWarning)
        return self.has_resource_watching_handlers(*args, **kwargs)

    def has_cause_handlers(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("registry.has_cause_handlers() is deprecated; "
                      "use registry.has_resource_changing_handlers().", DeprecationWarning)
        return self.has_resource_changing_handlers(*args, **kwargs)


_default_registry: OperatorRegistry = OperatorRegistry()


def get_default_registry() -> OperatorRegistry:
    """
    Get the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    return _default_registry


def set_default_registry(registry: OperatorRegistry) -> None:
    """
    Set the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    _default_registry = registry


def match(
        handler: ResourceHandler,
        body: bodies.Body,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
) -> bool:
    return all([
        _matches_field(handler, changed_fields or {}),
        _matches_labels(handler, body),
        _matches_annotations(handler, body),
    ])


def _matches_field(
        handler: ResourceHandler,
        changed_fields: Collection[dicts.FieldPath] = frozenset(),
) -> bool:
    return (not handler.field or
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
