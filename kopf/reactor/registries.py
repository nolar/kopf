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
from types import FunctionType, MethodType
from typing import MutableMapping, NamedTuple, Text, Optional, Tuple, Callable


# An immutable reference to a custom resource definition.
class Resource(NamedTuple):
    group: Text
    version: Text
    plural: Text

# A registered handler (function + event meta info).
class Handler(NamedTuple):
    fn: Callable
    id: Text
    event: Text
    field: Optional[Tuple[str, ...]]
    timeout: Optional[float] = None
    initial: Optional[bool] = None


class BaseRegistry(metaclass=abc.ABCMeta):
    """
    A registry stores the handlers and provides them to the reactor.
    """

    def get_cause_handlers(self, cause):
        return list(self._deduplicated(self.iter_cause_handlers(cause=cause)))

    @abc.abstractmethod
    def iter_cause_handlers(self, cause):
        pass

    def get_event_handlers(self, resource, event):
        return list(self._deduplicated(self.iter_event_handlers(resource=resource, event=event)))

    @abc.abstractmethod
    def iter_event_handlers(self, resource, event):
        pass

    @staticmethod
    def _deduplicated(handlers):
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
        seen_ids = set()
        for handler in handlers:
            if id(handler.fn) in seen_ids:
                pass
            else:
                seen_ids.add(id(handler.fn))
                yield handler


class SimpleRegistry(BaseRegistry):
    """
    A simple registry is just a list of handlers, no grouping.
    """

    def __init__(self, prefix=None):
        super().__init__()
        self.prefix = prefix
        self._handlers = []  # [Handler, ...]

    def __bool__(self):
        return bool(self._handlers)

    def append(self, handler):
        self._handlers.append(handler)

    def register(self, fn, id=None, event=None, field=None, timeout=None, initial=None):

        if field is None:
            field = None  # for the non-field events
        elif isinstance(field, str):
            field = tuple(field.split('.'))
        elif isinstance(field, (list, tuple)):
            field = tuple(field)
        else:
            raise ValueError(f"Field must be either a str, or a list/tuple. Got {field!r}")

        id = id if id is not None else get_callable_id(fn)
        id = id if field is None else f'{id}/{".".join(field)}'
        id = id if self.prefix is None else f'{self.prefix}/{id}'
        handler = Handler(id=id, fn=fn, event=event, field=field, timeout=timeout, initial=initial)

        self.append(handler)
        return fn  # to be usable as a decorator too.

    def iter_cause_handlers(self, cause):
        fields = {field for _, field, _, _ in cause.diff or []}
        for handler in self._handlers:
            if handler.event is None or handler.event == cause.event:
                if handler.initial and not cause.initial:
                    pass  # ignore initial handlers in non-initial causes.
                elif handler.field:
                    if any(field[:len(handler.field)] == handler.field for field in fields):
                        yield handler
                else:
                    yield handler

    def iter_event_handlers(self, resource, event):
        for handler in self._handlers:
            yield handler


def get_callable_id(c):
    """ Get an reasonably good id of any commonly used callable. """
    if c is None:
        return None
    elif isinstance(c, functools.partial):
        return get_callable_id(c.func)
    elif hasattr(c, '__wrapped__'):  # @functools.wraps()
        return get_callable_id(c.__wrapped__)
    elif isinstance(c, FunctionType) and c.__name__ == '<lambda>':
        # The best we can do to keep the id stable across the process restarts,
        # assuming at least no code changes. The code changes are not detectable.
        line = c.__code__.co_firstlineno
        path = c.__code__.co_filename
        return f'lambda:{path}:{line}'
    elif isinstance(c, (FunctionType, MethodType)):
        return getattr(c, '__qualname__', getattr(c, '__name__', repr(c)))
    else:
        raise ValueError(f"Cannot get id of {c!r}.")


class GlobalRegistry(BaseRegistry):
    """
    A global registry is used for handling of the multiple resources.
    It is usually populated by the `@kopf.on...` decorators.
    """

    def __init__(self):
        super().__init__()
        self._cause_handlers: MutableMapping[Resource, SimpleRegistry] = {}
        self._event_handlers: MutableMapping[Resource, SimpleRegistry] = {}

    def register_cause_handler(self, group, version, plural, fn,
                               id=None, event=None, field=None, timeout=None, initial=None):
        """
        Register an additional handler function for the specific resource and specific event.
        """
        resource = Resource(group, version, plural)
        registry = self._cause_handlers.setdefault(resource, SimpleRegistry())
        registry.register(event=event, field=field, fn=fn, id=id, timeout=timeout, initial=initial)
        return fn  # to be usable as a decorator too.

    def register_event_handler(self, group, version, plural, fn, id=None):
        """
        Register an additional handler function for low-level events.
        """
        resource = Resource(group, version, plural)
        registry = self._event_handlers.setdefault(resource, SimpleRegistry())
        registry.register(fn=fn, id=id)
        return fn  # to be usable as a decorator too.

    @property
    def resources(self):
        """ All known resources in the registry. """
        return frozenset(self._cause_handlers) | frozenset(self._event_handlers)

    def has_cause_handlers(self, resource):
        resource_registry = self._cause_handlers.get(resource, None)
        return bool(resource_registry)

    def has_event_handlers(self, resource):
        resource_registry = self._event_handlers.get(resource, None)
        return bool(resource_registry)

    def iter_cause_handlers(self, cause):
        """
        Iterate all handlers that match this cause/event, in the order they were registered (even if mixed).
        """
        resource_registry = self._cause_handlers.get(cause.resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_cause_handlers(cause=cause)

    def iter_event_handlers(self, resource, event):
        """
        Iterate all handlers for the low-level events.
        """
        resource_registry = self._event_handlers.get(resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_event_handlers(resource=resource, event=event)


_default_registry = GlobalRegistry()


def get_default_registry() -> GlobalRegistry:
    """
    Get the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    return _default_registry


def set_default_registry(registry: GlobalRegistry):
    """
    Set the default registry to be used by the decorators and the reactor
    unless the explicit registry is provided to them.
    """
    global _default_registry
    _default_registry = registry
