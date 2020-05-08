"""
The routines to manipulate the handler progression over the event cycle.

Used to track which handlers are finished, which are not yet,
and how many retries were there.

There could be more than one low-level k8s watch-events per one actual
high-level kopf-event (a cause). The handlers are called at different times,
and the overall handling routine should persist the handler status somewhere.

The states are persisted in a state storage: see `kopf.storage.progress`.
"""

import collections.abc
import copy
import dataclasses
import datetime
from typing import Any, Optional, Mapping, Dict, Collection, Iterator, overload

from kopf.storage import progress
from kopf.structs import bodies
from kopf.structs import callbacks
from kopf.structs import handlers as handlers_
from kopf.structs import patches


@dataclasses.dataclass(frozen=True)
class HandlerOutcome:
    """
    An in-memory outcome of one single invocation of one single handler.

    Conceptually, an outcome is similar to the async futures, but some cases
    are handled specially: e.g., the temporary errors have exceptions,
    but the handler should be retried later, unlike with the permanent errors.

    Note the difference: `HandlerState` is a persistent state of the handler,
    possibly after few executions, and consisting of simple data types
    (for YAML/JSON serialisation) rather than the actual in-memory objects.
    """
    final: bool
    delay: Optional[float] = None
    result: Optional[callbacks.Result] = None
    exception: Optional[Exception] = None


@dataclasses.dataclass(frozen=True)
class HandlerState:
    """
    A persisted state of a single handler, as stored on the resource's status.

    Note the difference: `HandlerOutcome` is for in-memory results of handlers,
    which is then additionally converted before being storing as a state.
    """
    started: Optional[datetime.datetime] = None  # None means this information was lost.
    stopped: Optional[datetime.datetime] = None  # None means it is still running (e.g. delayed).
    delayed: Optional[datetime.datetime] = None  # None means it is finished (succeeded/failed).
    retries: int = 0
    success: bool = False
    failure: bool = False
    message: Optional[str] = None
    _origin: Optional[progress.ProgressRecord] = None  # to check later if it has actually changed.

    @classmethod
    def from_scratch(cls) -> "HandlerState":
        return cls(
            started=datetime.datetime.utcnow(),
        )

    @classmethod
    def from_storage(cls, __d: progress.ProgressRecord) -> "HandlerState":
        return cls(
            started=_datetime_fromisoformat(__d.get('started')) or datetime.datetime.utcnow(),
            stopped=_datetime_fromisoformat(__d.get('stopped')),
            delayed=_datetime_fromisoformat(__d.get('delayed')),
            retries=__d.get('retries') or 0,
            success=__d.get('success') or False,
            failure=__d.get('failure') or False,
            message=__d.get('message'),
            _origin=__d,
        )

    def for_storage(self) -> progress.ProgressRecord:
        return progress.ProgressRecord(
            started=None if self.started is None else _datetime_toisoformat(self.started),
            stopped=None if self.stopped is None else _datetime_toisoformat(self.stopped),
            delayed=None if self.delayed is None else _datetime_toisoformat(self.delayed),
            retries=None if self.retries is None else int(self.retries),
            success=None if self.success is None else bool(self.success),
            failure=None if self.failure is None else bool(self.failure),
            message=None if self.message is None else str(self.message),
        )

    def as_in_storage(self) -> Mapping[str, Any]:
        # Nones are not stored by Kubernetes, so we filter them out for comparison.
        return {key: val for key, val in self.for_storage().items() if val is not None}

    def with_outcome(
            self,
            outcome: HandlerOutcome,
    ) -> "HandlerState":
        now = datetime.datetime.utcnow()
        cls = type(self)
        return cls(
            started=self.started if self.started else now,
            stopped=self.stopped if self.stopped else now if outcome.final else None,
            delayed=now + datetime.timedelta(seconds=outcome.delay) if outcome.delay is not None else None,
            success=bool(outcome.final and outcome.exception is None),
            failure=bool(outcome.final and outcome.exception is not None),
            retries=(self.retries if self.retries is not None else 0) + 1,
            message=None if outcome.exception is None else str(outcome.exception),
            _origin=self._origin,
        )

    @property
    def finished(self) -> bool:
        return bool(self.success or self.failure)

    @property
    def sleeping(self) -> bool:
        ts = self.delayed
        now = datetime.datetime.utcnow()
        return not self.finished and ts is not None and ts > now

    @property
    def awakened(self) -> bool:
        return bool(not self.finished and not self.sleeping)

    @property
    def runtime(self) -> datetime.timedelta:
        now = datetime.datetime.utcnow()
        return now - (self.started if self.started else now)


class State(Mapping[handlers_.HandlerId, HandlerState]):
    """
    A state of selected handlers, as persisted in the object's status.

    The state consists of simple YAML-/JSON-serializable values only:
    string handler ids as the keys; strings, booleans, integers as the values.

    The state is immutable: once created, it cannot be changed, and does not
    reflect the changes in the object's status. A new state is created every
    time some changes/outcomes are merged into the current state.
    """
    _states: Mapping[handlers_.HandlerId, HandlerState]

    def __init__(
            self,
            __src: Mapping[handlers_.HandlerId, HandlerState],
    ):
        super().__init__()
        self._states = dict(__src)

    @classmethod
    def from_scratch(
            cls,
            *,
            handlers: Collection[handlers_.BaseHandler],
    ) -> "State":
        return cls({handler.id: HandlerState.from_scratch() for handler in handlers})

    @classmethod
    def from_storage(
            cls,
            *,
            body: bodies.Body,
            storage: progress.ProgressStorage,
            handlers: Collection[handlers_.BaseHandler],
    ) -> "State":
        handler_states: Dict[handlers_.HandlerId, HandlerState] = {}
        for handler in handlers:
            content = storage.fetch(key=handler.id, body=body)
            handler_states[handler.id] = (HandlerState.from_storage(content) if content else
                                          HandlerState.from_scratch())
        return cls(handler_states)

    def with_outcomes(
            self,
            outcomes: Mapping[handlers_.HandlerId, HandlerOutcome],
    ) -> "State":
        unknown_ids = [handler_id for handler_id in outcomes if handler_id not in self]
        if unknown_ids:
            raise RuntimeError(f"Unexpected outcomes for unknown handlers: {unknown_ids!r}")

        cls = type(self)
        return cls({
            handler_id: (handler_state if handler_id not in outcomes else
                         handler_state.with_outcome(outcomes[handler_id]))
            for handler_id, handler_state in self.items()
        })

    def store(
            self,
            body: bodies.Body,
            patch: patches.Patch,
            storage: progress.ProgressStorage,
    ) -> None:
        for handler_id, handler_state in self.items():
            full_record = handler_state.for_storage()
            pure_record = handler_state.as_in_storage()
            if pure_record != handler_state._origin:
                storage.store(key=handler_id, record=full_record, body=body, patch=patch)
        storage.flush()

    def purge(
            self,
            *,
            body: bodies.Body,
            patch: patches.Patch,
            storage: progress.ProgressStorage,
    ) -> None:
        # Purge only our own handlers. Ignore others (e.g. other operators).
        for handler_id in self.keys():
            storage.purge(key=handler_id, body=body, patch=patch)
        storage.flush()

    def __len__(self) -> int:
        return len(self._states)

    def __iter__(self) -> Iterator[handlers_.HandlerId]:
        return iter(self._states)

    def __getitem__(self, item: handlers_.HandlerId) -> HandlerState:
        return self._states[item]

    @property
    def done(self) -> bool:
        # In particular, no handlers means that it is "done" even before doing.
        return all(handler_state.finished for handler_state in self._states.values())

    @property
    def delay(self) -> Optional[float]:
        delays = self.delays  # calculate only once, to save bit of CPU
        return min(delays) if delays else None

    @property
    def delays(self) -> Collection[float]:
        """
        Resulting delays for the handlers (only the postponed ones).

        The delays are then reduced to one single sleep in the top-level
        processing routine, based on all delays of different origin:
        e.g. postponed daemons, stopping daemons, temporarily failed handlers.
        """
        now = datetime.datetime.utcnow()
        return [
            max(0, (handler_state.delayed - now).total_seconds()) if handler_state.delayed else 0
            for handler_state in self._states.values()
            if not handler_state.finished
        ]


def deliver_results(
        *,
        outcomes: Mapping[handlers_.HandlerId, HandlerOutcome],
        patch: patches.Patch,
) -> None:
    """
    Store the results (as returned from the handlers) to the resource.

    This is not the handlers' state persistence, but the results' persistence.

    First, the state persistence is stored under ``.status.kopf.progress``,
    and can (later) be configured to be stored in different fields for different
    operators operating the same objects: ``.status.kopf.{somename}.progress``.
    The handlers' result are stored in the top-level ``.status``.

    Second, the handler results can (also later) be delivered to other objects,
    e.g. to their owners or label-selected related objects. For this, another
    class/module will be added.

    For now, we keep state- and result persistence in one module, but separated.
    """
    for handler_id, outcome in outcomes.items():
        if outcome.exception is not None:
            pass
        elif outcome.result is None:
            pass
        elif isinstance(outcome.result, collections.abc.Mapping):
            # TODO: merge recursively (patch-merge), do not overwrite the keys if they are present.
            patch.setdefault('status', {}).setdefault(handler_id, {}).update(outcome.result)
        else:
            patch.setdefault('status', {})[handler_id] = copy.deepcopy(outcome.result)


@overload
def _datetime_toisoformat(val: None) -> None: ...


@overload
def _datetime_toisoformat(val: datetime.datetime) -> str: ...


def _datetime_toisoformat(val: Optional[datetime.datetime]) -> Optional[str]:
    if val is None:
        return None
    else:
        return val.isoformat(timespec='microseconds')


@overload
def _datetime_fromisoformat(val: None) -> None: ...


@overload
def _datetime_fromisoformat(val: str) -> datetime.datetime: ...


def _datetime_fromisoformat(val: Optional[str]) -> Optional[datetime.datetime]:
    if val is None:
        return None
    else:
        return datetime.datetime.fromisoformat(val)
