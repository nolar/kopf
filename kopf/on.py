"""
The decorators for the event handlers. Usually used as::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def creation_handler(**kwargs):
        pass

This module is a part of the framework's public interface.
"""

# TODO: add cluster=True support (different API methods)

from typing import Optional, Callable

from kopf.reactor import callbacks
from kopf.reactor import causation
from kopf.reactor import errors as errors_
from kopf.reactor import handlers
from kopf.reactor import handling
from kopf.reactor import registries
from kopf.structs import bodies
from kopf.structs import dicts
from kopf.structs import resources

ResourceHandlerDecorator = Callable[[callbacks.ResourceHandlerFn], callbacks.ResourceHandlerFn]
ActivityHandlerDecorator = Callable[[callbacks.ActivityHandlerFn], callbacks.ActivityHandlerFn]


def startup(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityHandlerDecorator:
    def decorator(fn: callbacks.ActivityHandlerFn) -> callbacks.ActivityHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=causation.Activity.STARTUP,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def cleanup(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityHandlerDecorator:
    def decorator(fn: callbacks.ActivityHandlerFn) -> callbacks.ActivityHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=causation.Activity.CLEANUP,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def login(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityHandlerDecorator:
    """ ``@kopf.on.login()`` handler for custom (re-)authentication. """
    def decorator(fn: callbacks.ActivityHandlerFn) -> callbacks.ActivityHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=causation.Activity.AUTHENTICATION,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def probe(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityHandlerDecorator:
    """ ``@kopf.on.probe()`` handler for arbitrary liveness metrics. """
    def decorator(fn: callbacks.ActivityHandlerFn) -> callbacks.ActivityHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=causation.Activity.PROBE,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def resume(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        deleted: Optional[bool] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.resume()`` handler for the object resuming on operator (re)start. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=True, deleted=deleted, requires_finalizer=None,
            reason=None,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def create(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated; use backoff.
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.create()`` handler for the object creation. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=None,
            reason=causation.Reason.CREATE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def update(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.update()`` handler for the object update or change. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=None,
            reason=causation.Reason.UPDATE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def delete(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        optional: Optional[bool] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.delete()`` handler for the object deletion. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=bool(not optional),
            reason=causation.Reason.DELETE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def field(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        field: dicts.FieldSpec,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.field()`` handler for the individual field changes. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=real_field,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def event(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
    """ ``@kopf.on.event()`` handler for the silent spies on the events. """
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry.resource_watching_handlers[real_resource].append(handler)
        return fn
    return decorator


# TODO: find a better name: `@kopf.on.this` is confusing and does not fully
# TODO: match with the `@kopf.on.{cause}` pattern, where cause is create/update/delete.
def this(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.ResourceChangingRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> ResourceHandlerDecorator:
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
    def decorator(fn: callbacks.ResourceHandlerFn) -> callbacks.ResourceHandlerFn:
        parent_handler = handling.handler_var.get()
        real_registry = registry if registry is not None else handling.subregistry_var.get()
        real_id = registries.generate_id(fn=fn, id=id,
                                         prefix=parent_handler.id if parent_handler else None)
        handler = handlers.ResourceHandler(
            fn=fn, id=real_id, field=None,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry.append(handler)
        return fn
    return decorator


def register(  # lgtm[py/similar-function]
        fn: callbacks.ResourceHandlerFn,
        *,
        id: Optional[str] = None,
        errors: Optional[errors_.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.ResourceChangingRegistry] = None,
        labels: Optional[bodies.Labels] = None,
        annotations: Optional[bodies.Annotations] = None,
        when: Optional[callbacks.WhenHandlerFn] = None,
) -> callbacks.ResourceHandlerFn:
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
    decorator = this(
        id=id, registry=registry,
        errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
        labels=labels, annotations=annotations, when=when,
    )
    return decorator(fn)
