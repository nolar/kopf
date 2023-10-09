"""
Execution of pre-selected handlers, in batches or individually.

These functions are invoked from `kopf._core.reactor.processing`,
where the raw watch-events are interpreted and wrapped into extended *causes*.

The handler execution can also be used in other places, such as in-memory
activities, when there is no underlying Kubernetes object to patch'n'watch.
"""
import asyncio
import contextlib
import dataclasses
import datetime
import enum
from contextvars import ContextVar
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Collection, Iterable, \
                   Mapping, MutableMapping, NewType, Optional, Sequence, Set, TypeVar

from typing_extensions import Protocol

from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import ids
from kopf._core.actions import invocation

# The default delay duration for the regular exception in retry-mode.
DEFAULT_RETRY_DELAY = 1 * 60


class PermanentError(Exception):
    """ A fatal handler error, the retries are useless. """


class TemporaryError(Exception):
    """ A potentially recoverable error, should be retried. """
    def __init__(
            self,
            __msg: Optional[str] = None,
            delay: Optional[float] = DEFAULT_RETRY_DELAY,
    ) -> None:
        super().__init__(__msg)
        self.delay = delay


class HandlerTimeoutError(PermanentError):
    """ An error for the handler's timeout (if set). """


class HandlerRetriesError(PermanentError):
    """ An error for the handler's retries exceeded (if set). """


class HandlerChildrenRetry(TemporaryError):
    """ An internal pseudo-error to retry for the next sub-handlers attempt. """


class ErrorsMode(enum.Enum):
    """ How arbitrary (non-temporary/non-permanent) exceptions are treated. """
    IGNORED = enum.auto()
    TEMPORARY = enum.auto()
    PERMANENT = enum.auto()


# A specialised type to highlight the purpose or origin of the data of type Any,
# to not be mixed with other arbitrary Any values, where it is indeed "any".
Result = NewType('Result', object)


@dataclasses.dataclass(frozen=True)
class Outcome:
    """
    An in-memory outcome of one single invocation of one single handler.

    Conceptually, an outcome is similar to the async futures, but some cases
    are handled specially: e.g., the temporary errors have exceptions,
    but the handler should be retried later, unlike with the permanent errors.

    Note the difference: `HandlerState` is a persistent state of the handler,
    possibly after a few executions, and consisting of simple data types
    (for YAML/JSON serialisation) rather than the actual in-memory objects.
    """
    final: bool
    delay: Optional[float] = None
    result: Optional[Result] = None
    exception: Optional[Exception] = None
    subrefs: Collection[ids.HandlerId] = ()


@dataclasses.dataclass(frozen=True)
class HandlerState:
    """
    A persisted state of a single handler, as stored on the resource's status.

    Note the difference: `Outcome` is for in-memory results of handlers,
    which is then additionally converted before being storing as a state.

    Active handler states are those used in .done/.delays for the current
    handling cycle & the current cause. Passive handler states are those
    carried over for logging of counts/extras, and for final state purging,
    but not participating in the current handling cycle.
    """
    started: Optional[datetime.datetime] = None  # None means this information was lost.
    stopped: Optional[datetime.datetime] = None  # None means it is still running (e.g. delayed).
    delayed: Optional[datetime.datetime] = None  # None means it is finished (succeeded/failed).
    retries: int = 0
    success: bool = False
    failure: bool = False

    @property
    def finished(self) -> bool:
        return bool(self.success or self.failure)

    @property
    def sleeping(self) -> bool:
        ts = self.delayed
        now = datetime.datetime.now(datetime.timezone.utc)
        return not self.finished and ts is not None and ts > now

    @property
    def awakened(self) -> bool:
        return bool(not self.finished and not self.sleeping)

    @property
    def runtime(self) -> datetime.timedelta:
        now = datetime.datetime.now(datetime.timezone.utc)
        return now - (self.started if self.started else now)


class State(Mapping[ids.HandlerId, HandlerState]):
    pass


@dataclasses.dataclass
class Cause(invocation.Kwargable):
    """ Base non-specific cause as used in the framework's reactor. """
    logger: typedefs.Logger

    @property
    def _kwargs(self) -> Mapping[str, Any]:
        # Similar to `dataclasses.asdict()`, but not recursive for other dataclasses.
        return {field.name: getattr(self, field.name) for field in dataclasses.fields(self)}


CauseT = TypeVar('CauseT', bound=Cause)


@dataclasses.dataclass(frozen=True)
class Handler:
    """ A handler is a function bound with its behavioral constraints. """
    id: ids.HandlerId
    fn: invocation.Invokable
    param: Optional[Any]
    errors: Optional[ErrorsMode]
    timeout: Optional[float]
    retries: Optional[int]
    backoff: Optional[float]

    # Used in the logs. Overridden in some (but not all) handler types for better log messages.
    def __str__(self) -> str:
        return f"Handler {self.id!r}"

    # Overridden in handlers with fields for causes with field-specific old/new/diff.
    def adjust_cause(self, cause: CauseT) -> CauseT:
        return cause


class LifeCycleFn(Protocol):
    """ A callback type for handlers selection based on the event/cause. """
    def __call__(
            self,
            handlers: Sequence[Handler],
            *,
            state: State,
            **kwargs: Any,
    ) -> Sequence[Handler]: ...


# The task-local context; propagated down the stack instead of multiple kwargs.
# Used in `@kopf.subhandler` and `kopf.execute()` to add/get the sub-handlers.
sublifecycle_var: ContextVar[Optional[LifeCycleFn]] = ContextVar('sublifecycle_var')
subsettings_var: ContextVar[configuration.OperatorSettings] = ContextVar('subsettings_var')
subrefs_var: ContextVar[Iterable[Set[ids.HandlerId]]] = ContextVar('subrefs_var')
handler_var: ContextVar[Handler] = ContextVar('handler_var')
cause_var: ContextVar[Cause] = ContextVar('cause_var')


ExtraContext = Callable[[], AsyncContextManager[None]]


@contextlib.asynccontextmanager
async def no_extra_context() -> AsyncIterator[None]:
    yield


async def execute_handlers_once(
        lifecycle: LifeCycleFn,
        settings: configuration.OperatorSettings,
        handlers: Collection[Handler],
        cause: Cause,
        state: State,
        extra_context: ExtraContext = no_extra_context,
        default_errors: ErrorsMode = ErrorsMode.TEMPORARY,
) -> Mapping[ids.HandlerId, Outcome]:
    """
    Call the next handler(s) from the chain of the handlers.

    Keep the record on the progression of the handlers in the object's state,
    and use it on the next invocation to determined which handler(s) to call.

    This routine is used both for the global handlers (via global registry),
    and for the sub-handlers (via a simple registry of the current handler).
    """

    # Filter and select the handlers to be executed right now, on this event reaction cycle.
    handlers_todo = [h for h in handlers if state[h.id].awakened]
    handlers_plan = lifecycle(handlers_todo, state=state, **cause.kwargs)

    # Execute all planned (selected) handlers in one event reaction cycle, even if there are a few.
    outcomes: MutableMapping[ids.HandlerId, Outcome] = {}
    for handler in handlers_plan:
        outcome = await execute_handler_once(
            settings=settings,
            handler=handler,
            state=state[handler.id],
            cause=cause,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            extra_context=extra_context,
            default_errors=default_errors,
        )
        outcomes[handler.id] = outcome

    return outcomes


async def execute_handler_once(
        settings: configuration.OperatorSettings,
        handler: Handler,
        cause: Cause,
        state: HandlerState,
        lifecycle: Optional[LifeCycleFn] = None,
        extra_context: ExtraContext = no_extra_context,
        default_errors: ErrorsMode = ErrorsMode.TEMPORARY,
) -> Outcome:
    """
    Execute one and only one handler for one and only one time.

    *Execution* means not just *calling* the handler in properly set context
    (see `_call_handler`), but also interpreting its result and errors, and
    wrapping them into am `Outcome` object -- to be stored in the state.

    The *execution* can be long -- depending on how the handler is implemented.
    For daemons, it is normal to run for hours and days if needed.
    This is different from the regular handlers, which are supposed
    to be finished as soon as possible.

    This method is not supposed to raise any exceptions from the handlers:
    exceptions mean the failure of execution itself.
    """
    errors_mode = handler.errors if handler.errors is not None else default_errors
    backoff = handler.backoff if handler.backoff is not None else DEFAULT_RETRY_DELAY
    logger = cause.logger

    # Mutable accumulator for all the sub-handlers of any level deep; populated in `kopf.execute`.
    subrefs: Set[ids.HandlerId] = set()

    # The exceptions are handled locally and are not re-raised, to keep the operator running.
    try:
        logger.debug(f"{handler} is invoked.")

        if handler.timeout is not None and state.runtime.total_seconds() >= handler.timeout:
            raise HandlerTimeoutError(f"{handler} has timed out after {state.runtime}.")

        if handler.retries is not None and state.retries >= handler.retries:
            raise HandlerRetriesError(f"{handler} has exceeded {state.retries} retries.")

        result = await invoke_handler(
            handler=handler,
            cause=cause,
            retry=state.retries,
            started=state.started or datetime.datetime.now(datetime.timezone.utc),  # "or" is for type-checking.
            runtime=state.runtime,
            settings=settings,
            lifecycle=lifecycle,  # just a default for the sub-handlers, not used directly.
            extra_context=extra_context,
            subrefs=subrefs,
        )

    # The cancellations are an excepted way of stopping the handler. Especially for daemons.
    except asyncio.CancelledError:
        logger.warning(f"{handler} is cancelled. Will escalate.")
        raise

    # Unfinished children cause the regular retry, but with less logging and event reporting.
    except HandlerChildrenRetry as e:
        logger.debug(f"{handler} has unfinished sub-handlers. Will retry soon.")
        return Outcome(final=False, exception=e, delay=e.delay, subrefs=subrefs)

    # Definitely a temporary error, regardless of the error strictness.
    except TemporaryError as e:
        logger.error(f"{handler} failed temporarily: {str(e) or repr(e)}")
        return Outcome(final=False, exception=e, delay=e.delay, subrefs=subrefs)

    # Same as permanent errors below, but with better logging for our internal cases.
    except HandlerTimeoutError as e:
        logger.error(f"{str(e) or repr(e)}")  # already formatted
        return Outcome(final=True, exception=e, subrefs=subrefs)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Definitely a permanent error, regardless of the error strictness.
    except PermanentError as e:
        logger.error(f"{handler} failed permanently: {str(e) or repr(e)}")
        return Outcome(final=True, exception=e, subrefs=subrefs)
        # TODO: report the handling failure somehow (beside logs/events). persistent status?

    # Regular errors behave as either temporary or permanent depending on the error strictness.
    except Exception as e:
        if errors_mode == ErrorsMode.IGNORED:
            logger.exception(f"{handler} failed with an exception. Will ignore.")
            return Outcome(final=True, subrefs=subrefs)
        elif errors_mode == ErrorsMode.TEMPORARY:
            logger.exception(f"{handler} failed with an exception. Will retry.")
            return Outcome(final=False, exception=e, delay=backoff, subrefs=subrefs)
        elif errors_mode == ErrorsMode.PERMANENT:
            logger.exception(f"{handler} failed with an exception. Will stop.")
            return Outcome(final=True, exception=e, subrefs=subrefs)
            # TODO: report the handling failure somehow (beside logs/events). persistent status?
        else:
            raise RuntimeError(f"Unknown mode for errors: {errors_mode!r}")

    # No errors means the handler should be excluded from future runs in this reaction cycle.
    else:
        logger.info(f"{handler} succeeded.")
        return Outcome(final=True, result=result, subrefs=subrefs)


async def invoke_handler(
        *,
        handler: Handler,
        cause: Cause,
        retry: int,
        started: datetime.datetime,
        runtime: datetime.timedelta,
        settings: configuration.OperatorSettings,
        lifecycle: Optional[LifeCycleFn],
        subrefs: Set[ids.HandlerId],
        extra_context: ExtraContext,
) -> Optional[Result]:
    """
    Invoke one handler only, according to the calling conventions.

    Specifically, calculate the handler-specific fields (e.g. field diffs).

    Ensure the global context for this asyncio task is set to the handler and
    its cause -- for proper population of the sub-handlers via the decorators
    (see `@kopf.subhandler`).
    """

    # For the field-handlers, the old/new/diff values must match the field, not the whole object.
    cause = handler.adjust_cause(cause)

    # The context makes it possible and easy to pass the kwargs _through_ the user-space handlers:
    # from the framework to the framework's helper functions (e.g. sub-handling, hierarchies, etc).
    with invocation.context([
        (sublifecycle_var, lifecycle),
        (subsettings_var, settings),
        (subrefs_var, list(subrefs_var.get([])) + [subrefs]),
        (handler_var, handler),
        (cause_var, cause),
    ]):
        async with extra_context():
            result = await invocation.invoke(
                handler.fn,
                settings=settings,
                kwargsrc=cause,
                kwargs=dict(
                    param=handler.param,
                    retry=retry,
                    started=started,
                    runtime=runtime,
                ),
            )

            # Since we know that we invoked the handler, we cast "any" result to a handler result.
            return Result(result)
