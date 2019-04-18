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
from types import FunctionType, MethodType

# The constants for the event types, to prevent the direct string usage and typos.
# They are not exposed by the framework, but are used internally. See also: `kopf.on`.
CREATE = 'create'
UPDATE = 'update'
DELETE = 'delete'
FIELD = 'field'


# An immutable reference to a custom resource definition.
Resource = collections.namedtuple('Resource', 'group version plural')

# A registered handler (function + event meta info).
Handler = collections.namedtuple('Handler', 'fn id event field timeout')


class BaseRegistry(metaclass=abc.ABCMeta):
    """
    A registry stores the handlers and provides them to the reactor.
    """

    def get_handlers(self, cause):
        return list(self.iter_handlers(cause=cause))

    @abc.abstractmethod
    def iter_handlers(self, cause):
        pass


class SimpleRegistry(BaseRegistry):
    """
    A simple registry is just a list of handlers, no grouping.
    """

    def __init__(self, prefix=None):
        super().__init__()
        self.prefix = prefix
        self._handlers = []  # [Handler, ...]

    def append(self, handler):
        self._handlers.append(handler)

    def register(self, fn, id=None, event=None, field=None, timeout=None):

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
        handler = Handler(id=id, fn=fn, event=event, field=field, timeout=timeout)

        self.append(handler)
        return fn  # to be usable as a decorator too.

    def iter_handlers(self, cause):
        fields = {field for _, field, _, _ in cause.diff or []}
        for handler in self._handlers:
            if handler.event == FIELD:
                if any(field[:len(handler.field)] == handler.field for field in fields):
                    yield handler
            elif handler.event is None or handler.event == cause.event:
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
        self._handlers = {}  # {Resource: SimpleRegistry[Handler, ...]}

    def register(self, group, version, plural, fn, id=None, event=None, field=None, timeout=None):
        """
        Register an additional handler function for the specific resource and specific event.
        """
        resource = Resource(group, version, plural)
        registry = self._handlers.setdefault(resource, SimpleRegistry())
        registry.register(event=event, field=field, fn=fn, id=id, timeout=timeout)
        return fn  # to be usable as a decorator too.

    @property
    def resources(self):
        """ All known resources in the registry. """
        return frozenset(self._handlers)

    def iter_handlers(self, cause):
        """
        Iterate all handlers for this and special FIELD event, in the order they were registered (even if mixed).
        For the FIELD event, also filter only the handlers where the field matches one of the actually changed fields.
        """
        resource_registry = self._handlers.get(cause.resource, None)
        if resource_registry is not None:
            yield from resource_registry.iter_handlers(cause=cause)


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
