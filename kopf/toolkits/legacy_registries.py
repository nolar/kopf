"""
Backward-compatibility of a semi-public interface of the legacy registries.

Originally, these registries were part of the public interface (exported
via ``kopf`` top-level package), but later replaced by other registries
with an incompatible class hierarchy and method signatures.
"""
import abc
import warnings
from typing import Any, Union, Sequence, Iterator, Container, Optional

from kopf.reactor import causation
from kopf.reactor import registries
from kopf.structs import bodies
from kopf.structs import callbacks
from kopf.structs import dicts
from kopf.structs import filters
from kopf.structs import handlers
from kopf.structs import patches
from kopf.structs import resources as resources_

AnyCause = Union[causation.ResourceWatchingCause, causation.ResourceChangingCause]
AnyHandler = Union[handlers.ResourceWatchingHandler, handlers.ResourceChangingHandler]
AnyHandlerFn = Union[callbacks.ResourceWatchingFn, callbacks.ResourceChangingFn]


# A frankenclass to match both watching- & changing handlers for signatures & typing.
# An equivalent of the all-purpose ResourceHandler before it was split to specialised sub-classes.
class LegacyAllPurposeResourcerHandler(
    handlers.ResourceChangingHandler,
    handlers.ResourceWatchingHandler,
):
    fn: AnyHandlerFn  # type: ignore


class BaseRegistry(metaclass=abc.ABCMeta):
    """
    .. deprecated: 1.0

        Removed in the new class hierarchy.
    """

    @abc.abstractmethod
    def get_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Sequence[handlers.ResourceWatchingHandler]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[handlers.ResourceChangingHandler]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[handlers.ResourceChangingHandler]:
        raise NotImplementedError


class SimpleRegistry(BaseRegistry, registries.ResourceRegistry[
        AnyCause,
        AnyHandlerFn,
        AnyHandler]):
    """
    .. deprecated: 1.0

        Replaced with `ResourceWatchingRegistry` and `ResourceChangingRegistry`.
    """

    def register(
            self,
            fn: AnyHandlerFn,
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
    ) -> AnyHandlerFn:
        warnings.warn("registry.register() is deprecated; "
                      "use @kopf.on... decorators with registry= kwarg.",
                      DeprecationWarning)

        if reason is None and event is not None:
            reason = handlers.Reason(event)

        real_field = dicts.parse_field(field) or None  # to not store tuple() as a no-field case.
        real_id = registries.generate_id(fn=fn, id=id, suffix=".".join(real_field or []))
        handler = LegacyAllPurposeResourcerHandler(
            id=real_id, fn=fn,  # type: ignore
            reason=reason, field=real_field,
            errors=errors, timeout=timeout, retries=retries, backoff=backoff, cooldown=cooldown,
            initial=initial, deleted=deleted, requires_finalizer=requires_finalizer,
            labels=labels, annotations=annotations, when=when,
        )
        self.append(handler)
        return fn

    # A dummy to avoid ABCMeta restrictions, and for rudimentary class testing.
    # Yield/return everything unconditionally -- it was not used previously anyway.
    def iter_handlers(
            self,
            cause: AnyCause,
            excluded: Container[handlers.HandlerId] = frozenset(),  # only for signature matching
    ) -> Iterator[AnyHandler]:
        yield from self._handlers

    def get_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Sequence[handlers.ResourceWatchingHandler]:
        warnings.warn("SimpleRegistry.get_event_handlers() is deprecated; use "
                      "ResourceWatchingRegistry.get_handlers().", DeprecationWarning)
        return list(registries._deduplicated(self.iter_event_handlers(
            resource=resource, event=event)))

    def get_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[handlers.ResourceChangingHandler]:
        warnings.warn("SimpleRegistry.get_cause_handlers() is deprecated; use "
                      "ResourceChangingRegistry.get_handlers().", DeprecationWarning)
        return list(registries._deduplicated(self.iter_cause_handlers(cause=cause)))

    def iter_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        warnings.warn("SimpleRegistry.iter_event_handlers() is deprecated; use "
                      "ResourceWatchingRegistry.iter_handlers().", DeprecationWarning)

        cause = _create_watching_cause(resource, event)
        for handler in self._handlers:
            if not isinstance(handler, handlers.ResourceWatchingHandler):
                pass
            elif registries.match(handler=handler, cause=cause, ignore_fields=True):
                yield handler

    def iter_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[handlers.ResourceChangingHandler]:
        warnings.warn("SimpleRegistry.iter_cause_handlers() is deprecated; use "
                      "ResourceChangingRegistry.iter_handlers().", DeprecationWarning)

        changed_fields = frozenset(field for _, field, _, _ in cause.diff or [])
        for handler in self._handlers:
            if not isinstance(handler, handlers.ResourceChangingHandler):
                pass
            elif handler.reason is None or handler.reason == cause.reason:
                if handler.initial and not cause.initial:
                    pass  # ignore initial handlers in non-initial causes.
                elif registries.match(handler=handler, cause=cause,
                                      changed_fields=changed_fields):
                    yield handler


class GlobalRegistry(BaseRegistry, registries.OperatorRegistry):
    """
    .. deprecated: 1.0

        Replaced with `MultiRegistry`.
    """

    def register_event_handler(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("GlobalRegistry.register_event_handler() is deprecated; use "
                      "OperatorRegistry.register_resource_watching_handler().", DeprecationWarning)
        return self.register_resource_watching_handler(*args, **kwargs)

    def register_cause_handler(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("GlobalRegistry.register_cause_handler() is deprecated; use "
                      "OperatorRegistry.register_resource_changing_handler().", DeprecationWarning)
        return self.register_resource_changing_handler(*args, **kwargs)

    def has_event_handlers(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("GlobalRegistry.has_event_handlers() is deprecated; use "
                      "OperatorRegistry.has_resource_watching_handlers().", DeprecationWarning)
        return self.has_resource_watching_handlers(*args, **kwargs)

    def has_cause_handlers(self, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("GlobalRegistry.has_cause_handlers() is deprecated; use "
                      "OperatorRegistry.has_resource_changing_handlers().", DeprecationWarning)
        return self.has_resource_changing_handlers(*args, **kwargs)

    def get_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Sequence[handlers.ResourceWatchingHandler]:
        warnings.warn("GlobalRegistry.get_event_handlers() is deprecated; use "
                      "OperatorRegistry.get_resource_watching_handlers().", DeprecationWarning)
        cause = _create_watching_cause(resource=resource, event=event)
        return self.get_resource_watching_handlers(cause=cause)

    def get_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Sequence[handlers.ResourceChangingHandler]:
        warnings.warn("GlobalRegistry.get_cause_handlers() is deprecated; use "
                      "OperatorRegistry.get_resource_changing_handlers().", DeprecationWarning)
        return self.get_resource_changing_handlers(cause=cause)

    def iter_event_handlers(
            self,
            resource: resources_.Resource,
            event: bodies.RawEvent,
    ) -> Iterator[handlers.ResourceWatchingHandler]:
        warnings.warn("GlobalRegistry.iter_event_handlers() is deprecated; use "
                      "OperatorRegistry.iter_resource_watching_handlers().", DeprecationWarning)
        cause = _create_watching_cause(resource=resource, event=event)
        yield from self.iter_resource_watching_handlers(cause=cause)

    def iter_cause_handlers(
            self,
            cause: causation.ResourceChangingCause,
    ) -> Iterator[handlers.ResourceChangingHandler]:
        warnings.warn("GlobalRegistry.iter_cause_handlers() is deprecated; use "
                      "OperatorRegistry.iter_resource_changing_handlers().", DeprecationWarning)
        yield from self.iter_resource_changing_handlers(cause=cause)


class SmartGlobalRegistry(registries.SmartOperatorRegistry, GlobalRegistry):
    pass


def _create_watching_cause(
        resource: resources_.Resource,
        event: bodies.RawEvent,
) -> causation.ResourceWatchingCause:
    return causation.detect_resource_watching_cause(
        resource=resource,
        raw_event=event,
        patch=patches.Patch(),  # unused
        type=event['type'],  # unused
        body=bodies.Body(event['object']),  # unused
        raw=event,  # unused
    )
