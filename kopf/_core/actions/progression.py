"""
The routines to manipulate the handler progression over the event cycle.

Used to track which handlers are finished, which are not yet,
and how many retries were there.

There could be more than one low-level k8s watch-events per one actual
high-level kopf-event (a cause). The handlers are called at different times,
and the overall handling routine should persist the handler status somewhere.

The states are persisted in a state storage: see `kopf._cogs.configs.progress`.
"""

import collections.abc
import copy
import dataclasses
import datetime
from typing import Any, Collection, Dict, Iterable, Iterator, \
                   Mapping, NamedTuple, Optional, overload

import iso8601

from kopf._cogs.configs import progress
from kopf._cogs.structs import bodies, ids, patches
from kopf._core.actions import execution


@dataclasses.dataclass(frozen=True)
class HandlerState(execution.HandlerState):
    """
    A persisted state of a single handler, as stored on the resource's status.

    Note the difference: `Outcome` is for in-memory results of handlers,
    which is then additionally converted before being storing as a state.

    Active handler states are those used in .done/.delays for the current
    handling cycle & the current cause. Passive handler states are those
    carried over for logging of counts/extras, and for final state purging,
    but not participating in the current handling cycle.
    """

    # Some fields may overlap the base class's fields, but this is fine (the types are the same).
    active: Optional[bool] = None  # is it used in done/delays [T]? or only in counters/purges [F]?
    started: Optional[datetime.datetime] = None  # None means this information was lost.
    stopped: Optional[datetime.datetime] = None  # None means it is still running (e.g. delayed).
    delayed: Optional[datetime.datetime] = None  # None means it is finished (succeeded/failed).
    purpose: Optional[str] = None  # None is a catch-all marker for upgrades/rollbacks.
    retries: int = 0
    success: bool = False
    failure: bool = False
    message: Optional[str] = None
    subrefs: Collection[ids.HandlerId] = ()  # ids of actual sub-handlers of all levels deep.
    _origin: Optional[progress.ProgressRecord] = None  # to check later if it has actually changed.

    @classmethod
    def from_scratch(cls, *, purpose: Optional[str] = None) -> "HandlerState":
        return cls(
            active=True,
            started=datetime.datetime.now(datetime.timezone.utc),
            purpose=purpose,
        )

    @classmethod
    def from_storage(cls, __d: progress.ProgressRecord) -> "HandlerState":
        return cls(
            active=False,
            started=_parse_iso8601(__d.get('started')) or datetime.datetime.now(datetime.timezone.utc),
            stopped=_parse_iso8601(__d.get('stopped')),
            delayed=_parse_iso8601(__d.get('delayed')),
            purpose=__d.get('purpose') if __d.get('purpose') else None,
            retries=__d.get('retries') or 0,
            success=__d.get('success') or False,
            failure=__d.get('failure') or False,
            message=__d.get('message'),
            subrefs=__d.get('subrefs') or (),
            _origin=__d,
        )

    def for_storage(self) -> progress.ProgressRecord:
        return progress.ProgressRecord(
            started=None if self.started is None else _format_iso8601(self.started),
            stopped=None if self.stopped is None else _format_iso8601(self.stopped),
            delayed=None if self.delayed is None else _format_iso8601(self.delayed),
            purpose=None if self.purpose is None else str(self.purpose),
            retries=None if self.retries is None else int(self.retries),
            success=None if self.success is None else bool(self.success),
            failure=None if self.failure is None else bool(self.failure),
            message=None if self.message is None else str(self.message),
            subrefs=None if not self.subrefs else list(sorted(self.subrefs)),
        )

    def as_in_storage(self) -> Mapping[str, Any]:
        # Nones are not stored by Kubernetes, so we filter them out for comparison.
        return {key: val for key, val in self.for_storage().items() if val is not None}

    def as_active(self) -> "HandlerState":
        return dataclasses.replace(self, active=True)

    def with_purpose(
            self,
            purpose: Optional[str],
    ) -> "HandlerState":
        return dataclasses.replace(self, purpose=purpose)

    def with_outcome(
            self,
            outcome: execution.Outcome,
    ) -> "HandlerState":
        now = datetime.datetime.now(datetime.timezone.utc)
        cls = type(self)
        return cls(
            active=self.active,
            purpose=self.purpose,
            started=self.started if self.started else now,
            stopped=self.stopped if self.stopped else now if outcome.final else None,
            delayed=now + datetime.timedelta(seconds=outcome.delay) if outcome.delay is not None else None,
            success=bool(outcome.final and outcome.exception is None),
            failure=bool(outcome.final and outcome.exception is not None),
            retries=(self.retries if self.retries is not None else 0) + 1,
            message=None if outcome.exception is None else str(outcome.exception),
            subrefs=list(sorted(set(self.subrefs) | set(outcome.subrefs))),  # deduplicate
            _origin=self._origin,
        )


class StateCounters(NamedTuple):
    success: int
    failure: int
    running: int


class State(execution.State):
    """
    A state of selected handlers, as persisted in the object's status.

    The state consists of simple YAML-/JSON-serializable values only:
    string handler ids as the keys; strings, booleans, integers as the values.

    The state is immutable: once created, it cannot be changed, and does not
    reflect the changes in the object's status. A new state is created every
    time some changes/outcomes are merged into the current state.
    """
    _states: Mapping[ids.HandlerId, HandlerState]

    def __init__(
            self,
            __src: Mapping[ids.HandlerId, HandlerState],
            *,
            purpose: Optional[str] = None,
    ):
        super().__init__()
        self._states = dict(__src)
        self.purpose = purpose

    @classmethod
    def from_scratch(cls) -> "State":
        return cls({})

    @classmethod
    def from_storage(
            cls,
            *,
            body: bodies.Body,
            storage: progress.ProgressStorage,
            handlers: Iterable[execution.Handler],
    ) -> "State":
        handler_ids = {handler.id for handler in handlers}
        handler_states: Dict[ids.HandlerId, HandlerState] = {}
        for handler_id in handler_ids:
            content = storage.fetch(key=handler_id, body=body)
            if content is not None:
                handler_states[handler_id] = HandlerState.from_storage(content)
        return cls(handler_states)

    def with_purpose(
            self,
            purpose: Optional[str],
            handlers: Iterable[execution.Handler] = (),  # to be re-purposed
    ) -> "State":
        handler_states: Dict[ids.HandlerId, HandlerState] = dict(self)
        for handler in handlers:
            handler_states[handler.id] = handler_states[handler.id].with_purpose(purpose)
        cls = type(self)
        return cls(handler_states, purpose=purpose)

    def with_handlers(
            self,
            handlers: Iterable[execution.Handler],
    ) -> "State":
        handler_states: Dict[ids.HandlerId, HandlerState] = dict(self)
        for handler in handlers:
            if handler.id not in handler_states:
                handler_states[handler.id] = HandlerState.from_scratch(purpose=self.purpose)
            else:
                handler_states[handler.id] = handler_states[handler.id].as_active()
        cls = type(self)
        return cls(handler_states, purpose=self.purpose)

    def with_outcomes(
            self,
            outcomes: Mapping[ids.HandlerId, execution.Outcome],
    ) -> "State":
        unknown_ids = [handler_id for handler_id in outcomes if handler_id not in self]
        if unknown_ids:
            raise RuntimeError(f"Unexpected outcomes for unknown handlers: {unknown_ids!r}")

        cls = type(self)
        return cls({
            handler_id: (handler_state if handler_id not in outcomes else
                         handler_state.with_outcome(outcomes[handler_id]))
            for handler_id, handler_state in self._states.items()
        }, purpose=self.purpose)

    def without_successes(self) -> "State":
        cls = type(self)
        return cls({
            handler_id: handler_state
            for handler_id, handler_state in self._states.items()
            if not handler_state.success # i.e. failures & in-progress/retrying
        })

    def store(
            self,
            body: bodies.Body,
            patch: patches.Patch,
            storage: progress.ProgressStorage,
    ) -> None:
        for handler_id, handler_state in self._states.items():
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
            handlers: Iterable[execution.Handler],
    ) -> None:
        # Purge only our own handlers and their direct & indirect sub-handlers of all levels deep.
        # Ignore other handlers (e.g. handlers of other operators).
        handler_ids = {handler.id for handler in handlers}
        for handler_id in handler_ids:
            storage.purge(key=handler_id, body=body, patch=patch)
        for handler_id, handler_state in self._states.items():
            if handler_id not in handler_ids:
                storage.purge(key=handler_id, body=body, patch=patch)
            for subref in handler_state.subrefs:
                storage.purge(key=subref, body=body, patch=patch)
        storage.flush()

    def __len__(self) -> int:
        return len(self._states)

    def __iter__(self) -> Iterator[ids.HandlerId]:
        return iter(self._states)

    def __getitem__(self, item: ids.HandlerId) -> HandlerState:
        return self._states[item]

    @property
    def done(self) -> bool:
        # In particular, no handlers means that it is "done" even before doing.
        return all(
            handler_state.finished for handler_state in self._states.values()
            if handler_state.active
        )

    @property
    def extras(self) -> Mapping[str, StateCounters]:
        purposes = {
            handler_state.purpose for handler_state in self._states.values()
            if handler_state.purpose is not None and handler_state.purpose != self.purpose
        }
        counters = {
            purpose: StateCounters(
                success=len([1 for handler_state in self._states.values()
                            if handler_state.purpose == purpose and handler_state.success]),
                failure=len([1 for handler_state in self._states.values()
                            if handler_state.purpose == purpose and handler_state.failure]),
                running=len([1 for handler_state in self._states.values()
                            if handler_state.purpose == purpose and not handler_state.finished]),
            )
            for purpose in purposes
        }
        return counters

    @property
    def counts(self) -> StateCounters:
        purposeful_states = [
            handler_state for handler_state in self._states.values()
            if self.purpose is None or handler_state.purpose is None
               or handler_state.purpose == self.purpose
        ]
        return StateCounters(
            success=len([1 for handler_state in purposeful_states if handler_state.success]),
            failure=len([1 for handler_state in purposeful_states if handler_state.failure]),
            running=len([1 for handler_state in purposeful_states if not handler_state.finished]),
        )

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
        now = datetime.datetime.now(datetime.timezone.utc)
        return [
            max(0, (handler_state.delayed - now).total_seconds()) if handler_state.delayed else 0
            for handler_state in self._states.values()
            if handler_state.active and not handler_state.finished
        ]


def deliver_results(
        *,
        outcomes: Mapping[ids.HandlerId, execution.Outcome],
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
def _format_iso8601(val: None) -> None: ...


@overload
def _format_iso8601(val: datetime.datetime) -> str: ...


def _format_iso8601(val: Optional[datetime.datetime]) -> Optional[str]:
    return None if val is None else val.isoformat(timespec='microseconds')


@overload
def _parse_iso8601(val: None) -> None: ...


@overload
def _parse_iso8601(val: str) -> datetime.datetime: ...


def _parse_iso8601(val: Optional[str]) -> Optional[datetime.datetime]:
    return None if val is None else iso8601.parse_date(val)  # always TZ-aware
