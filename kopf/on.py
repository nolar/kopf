"""
The decorators for the event handlers. Usually used as::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def creation_handler(**kwargs):
        pass

"""

# TODO: add cluster=True support (different API methods)

from typing import Optional, Union, Tuple, List

from kopf.reactor.handling import subregistry_var
from kopf.reactor.registry import CREATE, UPDATE, DELETE, FIELD
from kopf.reactor.registry import GlobalRegistry, SimpleRegistry, get_default_registry


def create(
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[GlobalRegistry] = None):
    """ ``@kopf.on.create()`` handler for the object creation. """
    registry = registry if registry is not None else get_default_registry()
    def decorator(fn):
        registry.register(
            group=group, version=version, plural=plural,
            event=CREATE, id=id, timeout=timeout,
            fn=fn)
        return fn
    return decorator


def update(
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[GlobalRegistry] = None):
    """ ``@kopf.on.update()`` handler for the object update or change. """
    registry = registry if registry is not None else get_default_registry()
    def decorator(fn):
        registry.register(
            group=group, version=version, plural=plural,
            event=UPDATE, id=id, timeout=timeout,
            fn=fn)
        return fn
    return decorator


def delete(
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[GlobalRegistry] = None):
    """ ``@kopf.on.delete()`` handler for the object deletion. """
    registry = registry if registry is not None else get_default_registry()
    def decorator(fn):
        registry.register(
            group=group, version=version, plural=plural,
            event=DELETE, id=id, timeout=timeout,
            fn=fn)
        return fn
    return decorator


def field(
        group: str, version: str, plural: str,
        field: Union[str, List[str], Tuple[str, ...]],
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[GlobalRegistry] = None):
    """ ``@kopf.on.field()`` handler for the individual field changes. """
    registry = registry if registry is not None else get_default_registry()
    def decorator(fn):
        registry.register(
            group=group, version=version, plural=plural,
            event=FIELD, field=field, id=id, timeout=timeout,
            fn=fn)
        return fn
    return decorator


# TODO: find a better name: `@kopf.on.this` is confusing and does not fully
# TODO: match with the `@kopf.on.{cause}` pattern, where cause is create/update/delete.
def this(
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[SimpleRegistry] = None):
    """
    ``@kopf.on.this()`` decorator for the dynamically generated sub-handlers.

    Can be used only inside of the handler function.
    It is efficiently a syntax sugar to look like all other handlers::

        @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
        def create(*, spec, **kwargs):

            for task in spec.get('tasks', []):

                @kopf.on.this(id=f'task_{task}')
                def create_task(*, spec, task=task, **kwargs):

                    pass

    In this example, having spec.tasks set to ``[abc, def]``, this will create
    the following handlers: ``create``, ``create/task_abc``, ``create/task_def``.

    The parent handler is not considered as finished if there are unfinished
    sub-handlers left. Since the sub-handlers will be executed in the regular
    reactor and lifecycle, with multiple low-level events (one per iteration),
    the parent handler will also be executed multiple times, and is expected
    to produce the same (or at least predictable) set of sub-handlers.
    In addition, keep its logic idempotent (not failing on the repeated calls).

    Note: ``task=task`` is needed to freeze the closure variable, so that every
    create function will have its own value, not the latest in the for-cycle.
    """
    registry = registry if registry is not None else subregistry_var.get()
    def decorator(fn):
        registry.register(id=id, fn=fn, timeout=timeout)
        return fn
    return decorator


def register(
        fn,
        *,
        id: Optional[str] = None,
        timeout: Optional[float] = None,
        registry: Optional[SimpleRegistry] = None,
):
    """
    Register a function as a sub-handler of the currently executed handler.

    Example::

        @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
        def create_it(spec, **kwargs):
            for task in spec.get('tasks', []):

                def create_single_task(task=task, **_):
                    pass

                kopf.register(id=task, fn=create_single_task)

    This is efficiently an equivalent for::

        @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
        def create_it(spec, **kwargs):
            for task in spec.get('tasks', []):

                @kopf.on.this(id=task)
                def create_single_task(task=task, **_):
                    pass
    """
    return this(id=id, timeout=timeout, registry=registry)(fn)
