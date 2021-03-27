"""
The decorators for the event handlers. Usually used as::

    import kopf

    @kopf.on.create('kopfexamples')
    def creation_handler(**kwargs):
        pass

This module is a part of the framework's public interface.
"""

# TODO: add cluster=True support (different API methods)
from typing import Any, Callable, Optional, Union

from kopf.reactor import handling, registries
from kopf.structs import callbacks, dicts, filters, handlers, references, reviews

ActivityDecorator = Callable[[callbacks.ActivityFn], callbacks.ActivityFn]
ResourceIndexingDecorator = Callable[[callbacks.ResourceIndexingFn], callbacks.ResourceIndexingFn]
ResourceWatchingDecorator = Callable[[callbacks.ResourceWatchingFn], callbacks.ResourceWatchingFn]
ResourceChangingDecorator = Callable[[callbacks.ResourceChangingFn], callbacks.ResourceChangingFn]
ResourceWebhookDecorator = Callable[[callbacks.ResourceWebhookFn], callbacks.ResourceWebhookFn]
ResourceDaemonDecorator = Callable[[callbacks.ResourceDaemonFn], callbacks.ResourceDaemonFn]
ResourceTimerDecorator = Callable[[callbacks.ResourceTimerFn], callbacks.ResourceTimerFn]


def startup(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=handlers.Activity.STARTUP,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def cleanup(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=handlers.Activity.CLEANUP,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def login(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    """ ``@kopf.on.login()`` handler for custom (re-)authentication. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=handlers.Activity.AUTHENTICATION,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def probe(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ActivityDecorator:
    """ ``@kopf.on.probe()`` handler for arbitrary liveness metrics. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=handlers.Activity.PROBE,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def validate(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        operation: Optional[reviews.Operation] = None,  # -> .webhooks.*.rules.*.operations[0]
        persistent: Optional[bool] = None,
        side_effects: Optional[bool] = None,  # -> .webhooks.*.sideEffects
        ignore_failures: Optional[bool] = None,  # -> .webhooks.*.failurePolicy=Ignore
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceWebhookDecorator:
    """ ``@kopf.on.validate()`` handler for validating admission webhooks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceWebhookFn,
    ) -> callbacks.ResourceWebhookFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceWebhookHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,  # TODO: add some meaning later
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            reason=handlers.WebhookType.VALIDATING, operation=operation,
            persistent=persistent, side_effects=side_effects, ignore_failures=ignore_failures,
        )
        real_registry._resource_webhooks.append(handler)
        return fn
    return decorator


def mutate(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        operation: Optional[reviews.Operation] = None,  # -> .webhooks.*.rules.*.operations[0]
        persistent: Optional[bool] = None,
        side_effects: Optional[bool] = None,  # -> .webhooks.*.sideEffects
        ignore_failures: Optional[bool] = None,  # -> .webhooks.*.failurePolicy=Ignore
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceWebhookDecorator:
    """ ``@kopf.on.mutate()`` handler for mutating admission webhooks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceWebhookFn,
    ) -> callbacks.ResourceWebhookFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceWebhookHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,  # TODO: add some meaning later
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            reason=handlers.WebhookType.MUTATING, operation=operation,
            persistent=persistent, side_effects=side_effects, ignore_failures=ignore_failures,
        )
        real_registry._resource_webhooks.append(handler)
        return fn
    return decorator


def resume(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        deleted: Optional[bool] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.resume()`` handler for the object resuming on operator (re)start. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=True, deleted=deleted, requires_finalizer=None,
            reason=None,
        )
        real_registry._resource_changing.append(handler)
        return fn
    return decorator


def create(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.create()`` handler for the object creation. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=None,
            reason=handlers.Reason.CREATE,
        )
        real_registry._resource_changing.append(handler)
        return fn
    return decorator


def update(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,
        new: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.update()`` handler for the object update or change. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_conflicting_values(field, value, old, new)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
            initial=None, deleted=None, requires_finalizer=None,
            reason=handlers.Reason.UPDATE,
        )
        real_registry._resource_changing.append(handler)
        return fn
    return decorator


def delete(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        optional: Optional[bool] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.delete()`` handler for the object deletion. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=bool(not optional),
            reason=handlers.Reason.DELETE,
        )
        real_registry._resource_changing.append(handler)
        return fn
    return decorator


def field(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: dicts.FieldSpec,
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,
        new: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceChangingDecorator:
    """ ``@kopf.on.field()`` handler for the individual field changes. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceChangingFn,
    ) -> callbacks.ResourceChangingFn:
        _warn_conflicting_values(field, value, old, new)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry._resource_changing.append(handler)
        return fn
    return decorator


def index(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceIndexingDecorator:
    """ ``@kopf.index()`` handler for the indexing callbacks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceIndexingFn,
    ) -> callbacks.ResourceIndexingFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id)
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceIndexingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
        )
        real_registry._resource_indexing.append(handler)
        return fn
    return decorator


def event(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceWatchingDecorator:
    """ ``@kopf.on.event()`` handler for the silent spies on the events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceWatchingFn,
    ) -> callbacks.ResourceWatchingFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceWatchingHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
        )
        real_registry._resource_watching.append(handler)
        return fn
    return decorator


def daemon(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        initial_delay: Optional[float] = None,
        cancellation_backoff: Optional[float] = None,
        cancellation_timeout: Optional[float] = None,
        cancellation_polling: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceDaemonDecorator:
    """ ``@kopf.daemon()`` decorator for the background threads/tasks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceDaemonFn,
    ) -> callbacks.ResourceDaemonFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceDaemonHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            cancellation_backoff=cancellation_backoff,
            cancellation_timeout=cancellation_timeout,
            cancellation_polling=cancellation_polling,
        )
        real_registry._resource_spawning.append(handler)
        return fn
    return decorator


def timer(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: Optional[Union[str, references.Marker]] = None,
        __version_or_name: Optional[Union[str, references.Marker]] = None,
        __name: Optional[Union[str, references.Marker]] = None,
        *,
        group: Optional[str] = None,
        version: Optional[str] = None,
        kind: Optional[str] = None,
        plural: Optional[str] = None,
        singular: Optional[str] = None,
        shortcut: Optional[str] = None,
        category: Optional[str] = None,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        interval: Optional[float] = None,
        initial_delay: Optional[float] = None,
        sharp: Optional[bool] = None,
        idle: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        # Operator specification:
        registry: Optional[registries.OperatorRegistry] = None,
) -> ResourceTimerDecorator:
    """ ``@kopf.timer()`` handler for the regular events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ResourceTimerFn,
    ) -> callbacks.ResourceTimerFn:
        _warn_conflicting_values(field, value)
        _verify_filters(labels, annotations)
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        selector = references.Selector(
            __group_or_groupversion_or_name, __version_or_name, __name,
            group=group, version=version,
            kind=kind, plural=plural, singular=singular, shortcut=shortcut, category=category,
        )
        handler = handlers.ResourceTimerHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            sharp=sharp, idle=idle, interval=interval,
        )
        real_registry._resource_spawning.append(handler)
        return fn
    return decorator


def subhandler(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
        field: Optional[dicts.FieldSpec] = None,
        value: Optional[filters.ValueFilter] = None,
        old: Optional[filters.ValueFilter] = None,  # only for on.update's subhandlers
        new: Optional[filters.ValueFilter] = None,  # only for on.update's subhandlers
) -> ResourceChangingDecorator:
    """
    ``@kopf.subhandler()`` decorator for the dynamically generated sub-handlers.

    Can be used only inside of the handler function.
    It is efficiently a syntax sugar to look like all other handlers::

        @kopf.on.create('kopfexamples')
        def create(*, spec, **kwargs):

            for task in spec.get('tasks', []):

                @kopf.subhandler(id=f'task_{task}')
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
        _warn_conflicting_values(field, value, old, new)
        _verify_filters(labels, annotations)
        real_registry = handling.subregistry_var.get()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id,
                                         prefix=parent_handler.id if parent_handler else None)
        handler = handlers.ResourceChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=None, labels=labels, annotations=annotations, when=when,
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
        # Handler's behaviour specification:
        id: Optional[str] = None,
        param: Optional[Any] = None,
        errors: Optional[handlers.ErrorsMode] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        # Resource object specification:
        labels: Optional[filters.MetaFilter] = None,
        annotations: Optional[filters.MetaFilter] = None,
        when: Optional[callbacks.WhenFilterFn] = None,
) -> callbacks.ResourceChangingFn:
    """
    Register a function as a sub-handler of the currently executed handler.

    Example::

        @kopf.on.create('kopfexamples')
        def create_it(spec, **kwargs):
            for task in spec.get('tasks', []):

                def create_single_task(task=task, **_):
                    pass

                kopf.register(id=task, fn=create_single_task)

    This is efficiently an equivalent for::

        @kopf.on.create('kopfexamples')
        def create_it(spec, **kwargs):
            for task in spec.get('tasks', []):

                @kopf.subhandler(id=task)
                def create_single_task(task=task, **_):
                    pass
    """
    decorator = subhandler(
        id=id, param=param,
        errors=errors, timeout=timeout, retries=retries, backoff=backoff,
        labels=labels, annotations=annotations, when=when,
    )
    return decorator(fn)


def _verify_filters(
        labels: Optional[filters.MetaFilter],
        annotations: Optional[filters.MetaFilter],
) -> None:
    if labels is not None:
        for key, val in labels.items():
            if val is None:
                raise ValueError("`None` for label filters is not supported; "
                                 "use kopf.PRESENT or kopf.ABSENT.")
    if annotations is not None:
        for key, val in annotations.items():
            if val is None:
                raise ValueError("`None` for annotation filters is not supported; "
                                 "use kopf.PRESENT or kopf.ABSENT.")


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
