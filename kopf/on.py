"""
The decorators for the event handlers. Usually used as:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def creation_handler(**kwargs):
        pass

This module is a part of the framework's public interface.
"""
import warnings
from collections.abc import Callable, Collection
# TODO: add cluster=True support (different API methods)
from typing import Any

from kopf._cogs.structs import dicts, references, reviews
from kopf._core.actions import execution
from kopf._core.intents import callbacks, causes, filters, handlers, registries
from kopf._core.reactor import subhandling

ActivityDecorator = Callable[[callbacks.ActivityFn], callbacks.ActivityFn]
IndexingDecorator = Callable[[callbacks.IndexingFn], callbacks.IndexingFn]
WatchingDecorator = Callable[[callbacks.WatchingFn], callbacks.WatchingFn]
ChangingDecorator = Callable[[callbacks.ChangingFn], callbacks.ChangingFn]
WebhookDecorator = Callable[[callbacks.WebhookFn], callbacks.WebhookFn]
DaemonDecorator = Callable[[callbacks.DaemonFn], callbacks.DaemonFn]
TimerDecorator = Callable[[callbacks.TimerFn], callbacks.TimerFn]


def startup(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=causes.Activity.STARTUP,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def cleanup(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ActivityDecorator:
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ActivityFn,
    ) -> callbacks.ActivityFn:
        real_registry = registry if registry is not None else registries.get_default_registry()
        real_id = registries.generate_id(fn=fn, id=id)
        handler = handlers.ActivityHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            activity=causes.Activity.CLEANUP,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def login(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
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
            activity=causes.Activity.AUTHENTICATION,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def probe(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
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
            activity=causes.Activity.PROBE,
        )
        real_registry._activities.append(handler)
        return fn
    return decorator


def validate(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        operation: reviews.Operation | None = None,  # deprecated -> .webhooks.*.rules.*.operations[0]
        operations: Collection[reviews.Operation] | None = None,  # -> .webhooks.*.rules.*.operations
        subresource: str | None = None,  # -> .webhooks.*.rules.*.resources[]
        persistent: bool | None = None,
        side_effects: bool | None = None,  # -> .webhooks.*.sideEffects
        ignore_failures: bool | None = None,  # -> .webhooks.*.failurePolicy=Ignore
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> WebhookDecorator:
    """ ``@kopf.on.validate()`` handler for validating admission webhooks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.WebhookFn,
    ) -> callbacks.WebhookFn:
        nonlocal operations
        operations = _verify_operations(operation, operations)
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
        handler = handlers.WebhookHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,  # TODO: add some meaning later
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            reason=causes.WebhookType.VALIDATING, operations=operations, subresource=subresource,
            persistent=persistent, side_effects=side_effects, ignore_failures=ignore_failures,
        )
        real_registry._webhooks.append(handler)
        return fn
    return decorator


def mutate(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        operation: reviews.Operation | None = None,  # deprecated -> .webhooks.*.rules.*.operations[0]
        operations: Collection[reviews.Operation] | None = None,  # -> .webhooks.*.rules.*.operations
        subresource: str | None = None,  # -> .webhooks.*.rules.*.resources[]
        persistent: bool | None = None,
        side_effects: bool | None = None,  # -> .webhooks.*.sideEffects
        ignore_failures: bool | None = None,  # -> .webhooks.*.failurePolicy=Ignore
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> WebhookDecorator:
    """ ``@kopf.on.mutate()`` handler for mutating admission webhooks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.WebhookFn,
    ) -> callbacks.WebhookFn:
        nonlocal operations
        operations = _verify_operations(operation, operations)
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
        handler = handlers.WebhookHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,  # TODO: add some meaning later
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            reason=causes.WebhookType.MUTATING, operations=operations, subresource=subresource,
            persistent=persistent, side_effects=side_effects, ignore_failures=ignore_failures,
        )
        real_registry._webhooks.append(handler)
        return fn
    return decorator


def resume(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        deleted: bool | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ChangingDecorator:
    """ ``@kopf.on.resume()`` handler for the object resuming on operator (re)start. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
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
        handler = handlers.ChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=True, deleted=deleted, requires_finalizer=None,
            reason=None,
        )
        real_registry._changing.append(handler)
        return fn
    return decorator


def create(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ChangingDecorator:
    """ ``@kopf.on.create()`` handler for the object creation. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
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
        handler = handlers.ChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=None,
            reason=causes.Reason.CREATE,
        )
        real_registry._changing.append(handler)
        return fn
    return decorator


def update(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        old: filters.ValueFilter | None = None,
        new: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ChangingDecorator:
    """ ``@kopf.on.update()`` handler for the object update or change. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
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
        handler = handlers.ChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
            initial=None, deleted=None, requires_finalizer=None,
            reason=causes.Reason.UPDATE,
        )
        real_registry._changing.append(handler)
        return fn
    return decorator


def delete(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        optional: bool | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ChangingDecorator:
    """ ``@kopf.on.delete()`` handler for the object deletion. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
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
        handler = handlers.ChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=None, new=None, field_needs_change=False,
            initial=None, deleted=None, requires_finalizer=bool(not optional),
            reason=causes.Reason.DELETE,
        )
        real_registry._changing.append(handler)
        return fn
    return decorator


def field(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec,
        value: filters.ValueFilter | None = None,
        old: filters.ValueFilter | None = None,
        new: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> ChangingDecorator:
    """ ``@kopf.on.field()`` handler for the individual field changes. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
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
        handler = handlers.ChangingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value, old=old, new=new, field_needs_change=True,
            initial=None, deleted=None, requires_finalizer=None,
            reason=None,
        )
        real_registry._changing.append(handler)
        return fn
    return decorator


def index(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> IndexingDecorator:
    """ ``@kopf.index()`` handler for the indexing callbacks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.IndexingFn,
    ) -> callbacks.IndexingFn:
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
        handler = handlers.IndexingHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
        )
        real_registry._indexing.append(handler)
        return fn
    return decorator


def event(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> WatchingDecorator:
    """ ``@kopf.on.event()`` handler for the silent spies on the events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.WatchingFn,
    ) -> callbacks.WatchingFn:
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
        handler = handlers.WatchingHandler(
            fn=fn, id=real_id, param=param,
            errors=None, timeout=None, retries=None, backoff=None,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
        )
        real_registry._watching.append(handler)
        return fn
    return decorator


def daemon(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        initial_delay: float | callbacks.DelayFn | None = None,
        cancellation_backoff: float | None = None,
        cancellation_timeout: float | None = None,
        cancellation_polling: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> DaemonDecorator:
    """ ``@kopf.daemon()`` decorator for the background threads/tasks. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.DaemonFn,
    ) -> callbacks.DaemonFn:
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
        handler = handlers.DaemonHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            cancellation_backoff=cancellation_backoff,
            cancellation_timeout=cancellation_timeout,
            cancellation_polling=cancellation_polling,
        )
        real_registry._spawning.append(handler)
        return fn
    return decorator


def timer(  # lgtm[py/similar-function]
        # Resource type specification:
        __group_or_groupversion_or_name: str | references.Marker | None = None,
        __version_or_name: str | references.Marker | None = None,
        __name: str | references.Marker | None = None,
        *,
        group: str | None = None,
        version: str | None = None,
        kind: str | None = None,
        plural: str | None = None,
        singular: str | None = None,
        shortcut: str | None = None,
        category: str | None = None,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        interval: float | None = None,
        initial_delay: float | callbacks.DelayFn | None = None,
        sharp: bool | None = None,
        idle: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        # Operator specification:
        registry: registries.OperatorRegistry | None = None,
) -> TimerDecorator:
    """ ``@kopf.timer()`` handler for the regular events. """
    def decorator(  # lgtm[py/similar-function]
            fn: callbacks.TimerFn,
    ) -> callbacks.TimerFn:
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
        handler = handlers.TimerHandler(
            fn=fn, id=real_id, param=param,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff,
            selector=selector, labels=labels, annotations=annotations, when=when,
            field=real_field, value=value,
            initial_delay=initial_delay, requires_finalizer=True,
            sharp=sharp, idle=idle, interval=interval,
        )
        real_registry._spawning.append(handler)
        return fn
    return decorator


def subhandler(  # lgtm[py/similar-function]
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
        field: dicts.FieldSpec | None = None,
        value: filters.ValueFilter | None = None,
        old: filters.ValueFilter | None = None,  # only for on.update's subhandlers
        new: filters.ValueFilter | None = None,  # only for on.update's subhandlers
) -> ChangingDecorator:
    """
    ``@kopf.subhandler()`` decorator for the dynamically generated sub-handlers.

    Can be used only inside of the handler function.
    It is efficiently a syntax sugar to look like all other handlers:

    .. code-block:: python

        import kopf

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
            fn: callbacks.ChangingFn,
    ) -> callbacks.ChangingFn:
        parent_handler = execution.handler_var.get()
        if not isinstance(parent_handler, handlers.ChangingHandler):
            raise TypeError("Sub-handlers are only supported for resource-changing handlers.")
        _warn_incompatible_parent_with_oldnew(parent_handler, old, new)
        _warn_conflicting_values(field, value, old, new)
        _verify_filters(labels, annotations)
        real_registry = subhandling.subregistry_var.get()
        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id,
                                         prefix=parent_handler.id if parent_handler else None)
        handler = handlers.ChangingHandler(
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
        fn: callbacks.ChangingFn,
        *,
        # Handler's behaviour specification:
        id: str | None = None,
        param: Any | None = None,
        errors: execution.ErrorsMode | None = None,
        timeout: float | None = None,
        retries: int | None = None,
        backoff: float | None = None,
        # Resource object specification:
        labels: filters.MetaFilter | None = None,
        annotations: filters.MetaFilter | None = None,
        when: callbacks.WhenFilterFn | None = None,
) -> callbacks.ChangingFn:
    """
    Register a function as a sub-handler of the currently executed handler.

    Example:

    .. code-block:: python

        import kopf

        @kopf.on.create('kopfexamples')
        def create_it(spec, **kwargs):
            for task in spec.get('tasks', []):

                def create_single_task(task=task, **_):
                    pass

                kopf.register(id=task, fn=create_single_task)

    This is efficiently an equivalent for:

    .. code-block:: python

        import kopf

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


def _verify_operations(
        operation: reviews.Operation | None = None,  # deprecated
        operations: Collection[reviews.Operation] | None = None,
) -> Collection[reviews.Operation] | None:
    if operation is not None:
        warnings.warn("operation= is deprecated, use operations={...}.", DeprecationWarning)
        operations = frozenset([] if operations is None else operations) | {operation}
    if operations is not None and not operations:
        raise ValueError(f"Operations should be either None or non-empty. Got empty {operations}.")
    return operations


def _verify_filters(
        labels: filters.MetaFilter | None,
        annotations: filters.MetaFilter | None,
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
        field: dicts.FieldSpec | None,
        value: filters.ValueFilter | None,
        old: filters.ValueFilter | None = None,
        new: filters.ValueFilter | None = None,
) -> None:
    if field is None and (value is not None or old is not None or new is not None):
        raise TypeError("Value/old/new filters are specified without a mandatory field.")
    if value is not None and (old is not None or new is not None):
        raise TypeError("Either value= or old=/new= can be defined, not both.")


def _warn_incompatible_parent_with_oldnew(
        handler: execution.Handler,
        old: Any,
        new: Any,
) -> None:
    if old is not None or new is not None:
        if isinstance(handler, handlers.ChangingHandler):
            is_on_update = handler.reason == causes.Reason.UPDATE
            is_on_field = handler.reason is None and not handler.initial
            if not is_on_update and not is_on_field:
                raise TypeError("Filters old=/new= can only be used in update handlers.")
