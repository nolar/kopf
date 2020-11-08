"""
The decorators for the event handlers. Usually used as::

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples')
    def creation_handler(**kwargs):
        pass

This module is a part of the framework's public interface.
"""

# TODO: add cluster=True support (different API methods)
import inspect
import warnings
from typing import Any, Callable, Optional

from kopf.reactor import handling, registries
from kopf.structs import callbacks, dicts, filters, handlers, resources

ActivityDecorator = Callable[[callbacks.ActivityFn], callbacks.ActivityFn]
ResourceWatchingDecorator = Callable[[callbacks.ResourceWatchingFn], callbacks.ResourceWatchingFn]
ResourceChangingDecorator = Callable[[callbacks.ResourceChangingFn], callbacks.ResourceChangingFn]
ResourceDaemonDecorator = Callable[[callbacks.ResourceDaemonFn], callbacks.ResourceDaemonFn]
ResourceTimerDecorator = Callable[[callbacks.ResourceTimerFn], callbacks.ResourceTimerFn]


def startup(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        _warn_deprecated_signatures(fn)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=handlers.Activity.STARTUP,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def cleanup(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        _warn_deprecated_signatures(fn)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=handlers.Activity.CLEANUP,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def login(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    """ ``@kopf.on.login()`` handler for custom (re-)authentication. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        _warn_deprecated_signatures(fn)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=handlers.Activity.AUTHENTICATION,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def probe(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    """ ``@kopf.on.probe()`` handler for arbitrary liveness metrics. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        _warn_deprecated_signatures(fn)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            activity=handlers.Activity.PROBE,
        )
        real_registry.activity_handlers.append(handler)
        return fn
    return decorator


def resume(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        deleted: Optional[bool] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.resume()`` handler for the object resuming on operator (re)start. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
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
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated; use backoff.
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.create()`` handler for the object creation. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=None,
            reason=handlers.Reason.CREATE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def update(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,
        new: Optional[filters.ValueFilter] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.update()`` handler for the object update or change. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value, old, new)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
            initial=None, deleted=None, requires_finalizer=None,
            reason=handlers.Reason.UPDATE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


def delete(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        optional: Optional[bool] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.delete()`` handler for the object deletion. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=bool(not optional),
            reason=handlers.Reason.DELETE,
        )
        real_registry.resource_changing_handlers[real_resource].append(handler)
        return fn
    return decorator


# Used only until the positional field is deleted -- for easier type checking without extra enums.
__UNSET = 'a.deprecation.marker.that.is.never.going.to.go.beyond.decorators.and.deprecation.checks'


def field(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        __field: dicts.FieldSpec = __UNSET,  # deprecated (as positional)
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: dicts.FieldSpec = __UNSET,  # TODO: when positional __field is removed, remove the value.
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,
        new: Optional[filters.ValueFilter] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.field()`` handler for the individual field changes. """
    field = _warn_deprecated_positional_field(__field, field)
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value, old, new)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
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
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
) -> ResourceWatchingDecorator:
    """ ``@kopf.on.event()`` handler for the silent spies on the events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceWatchingFn,
    ) -> callbacks.ResourceWatchingFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceWatchingHandler(
            fn=fn, id=real_id,
            errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
            labels=labels, annotations=annotations, when=when, field=real_field, value=value,
        )
        real_registry.resource_watching_handlers[real_resource].append(handler)
        return fn
    return decorator


def daemon(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        initial_delay: Optional[float] = None,
        cancellation_backoff: Optional[float] = None,
        cancellation_timeout: Optional[float] = None,
        cancellation_polling: Optional[float] = None,
) -> ResourceDaemonDecorator:
    """ ``@kopf.daemon()`` decorator for the background threads/tasks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceDaemonFn,
    ) -> callbacks.ResourceDaemonFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceDaemonHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when, field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            cancellation_backoff=cancellation_backoff,
            cancellation_timeout=cancellation_timeout,
            cancellation_polling=cancellation_polling,
        )
        real_registry.resource_spawning_handlers[real_resource].append(handler)
        return fn
    return decorator


def timer(  # lgtm[py/similar-function]
        group: str, version: str, plural: str,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.OperatorRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        initial_delay: Optional[float] = None,
        sharp: Optional[bool] = None,
        idle: Optional[float] = None,
        interval: Optional[float] = None,
) -> ResourceTimerDecorator:
    """ ``@kopf.timer()`` handler for the regular events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceTimerFn,
    ) -> callbacks.ResourceTimerFn:
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_resource = resources.Resource(group, version, plural)
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ResourceTimerHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when, field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            sharp=sharp, idle=idle, interval=interval,
        )
        real_registry.resource_spawning_handlers[real_resource].append(handler)
        return fn
    return decorator


# TODO: find a better name: `@kopf.on.this` is confusing and does not fully
# TODO: match with the `@kopf.on.{cause}` pattern, where cause is create/update/delete.
def this(  # lgtm[py/similar-function]
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.ResourceChangingRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,  # only for on.update's subhandlers
        new: Optional[filters.ValueFilter] = None,  # only for on.update's subhandlers
) -> ResourceChangingDecorator:
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
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        parent_handler = handling.handler_var.get()
        if not isinstance(parent_handler, handlers.ResourceChangingHandler):
            raise TypeError("Sub-handlers are only supported for resource-changing handlers.")
        _warn_incompatible_parent_with_oldnew(parent_handler, old, new)
        _warn_deprecated_signatures(fn)
        _warn_deprecated_filters(labels, annotations)
        _warn_conflicting_values(field, value, old, new)
        real_registry = registry if registry is not None else handling.subregistry_var.get()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id,
                                         prefix=parent_handler.id if parent_handler else None)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new,
            field_needs_change=parent_handler.field_needs_change, # inherit dymaically
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry.append(handler)
        return fn
    return decorator


def register(  # lgtm[py/similar-function]
        fn: callbacks.ResourceChangingFn,
        *,
        id: Optional[str] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        cooldown: Optional[float] = None,  # deprecated, use `backoff`
        registry: Optional[registries.ResourceChangingRegistry] = None,
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
) -> callbacks.ResourceChangingFn:
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


def _warn_deprecated_signatures(
        fn: Callable[..., Any],
) -> None:
    argspec = inspect.getfullargspec(fn)
    if 'cause' in argspec.args or 'cause' in argspec.kwonlyargs:
        warnings.warn("`cause` kwarg is deprecated; use kwargs directly.", DeprecationWarning)


def _warn_deprecated_filters(
        labels: Optional[filters.MetaFilter],
        annotations: Optional[filters.MetaFilter],
) -> None:
    if labels is not None:
        for key, val in labels.items():
            if val is None:
                warnings.warn(
                    f"`None` for label filters is deprecated; use kopf.PRESENT.",
                    DeprecationWarning, stacklevel=2)
    if annotations is not None:
        for key, val in annotations.items():
            if val is None:
                warnings.warn(
                    f"`None` for annotation filters is deprecated; use kopf.PRESENT.",
                    DeprecationWarning, stacklevel=2)


def _warn_deprecated_positional_field(
        __field: dicts.FieldSpec,
        field: dicts.FieldSpec,
) -> dicts.FieldSpec:
    if field == __UNSET and __field == __UNSET:
        raise TypeError("Field is not specified; use field= kwarg explicitly.")
    elif field != __UNSET and __field != __UNSET:
        raise TypeError("Field is ambiguous; use field= kwarg only, not a positional.")
    elif field == __UNSET and __field != __UNSET:
        warnings.warn("Positional field name is deprecated, use field= kwarg.",
                      DeprecationWarning)
        field = __field
    return field


def _warn_conflicting_values(
        field: Optional[dicts.FieldSpec],
        value: Optional[filters.ValueFilter],
        old: Optional[filters.ValueFilter] = None,
        new: Optional[filters.ValueFilter] = None,
) -> None:
    if field is None and (value is not None or old is not None or new is not None):
        raise TypeError("Value/old/new filters are specified without a mandatory field.")
    if value is not None and (old is not None or new is not None):
        raise TypeError("Either value= or old=/new= can be defined, not both.")


def _warn_incompatible_parent_with_oldnew(
        handler: handlers.BaseHandler,
        old: Any,
        new: Any,
) -> None:
    if old is not None or new is not None:
        if isinstance(handler, handlers.ResourceChangingHandler):
            is_on_update = handler.reason == handlers.Reason.UPDATE
            is_on_field = handler.reason is None and not handler.initial
            if not is_on_update and not is_on_field:
                raise TypeError("Filters old=/new= can only be used in update handlers.")
