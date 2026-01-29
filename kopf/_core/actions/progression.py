"""
The routines to manipulate the handler progression over the event cycle.

Used to track which handlers are finished, which are not yet,
and how many retries were there.

There could be more than one low-level k8s watch-events per one actual
high-level kopf-event (a cause). The handlers are called at different times,
and the overall handling routine should persist the handler status somewhere.

The states are persisted in a state storage:
see :mod:`kopf._cogs.configs.progress`.

MOCKED LOOP TIME:

**For testability,** we use ``basetime + timedelta(seconds=loop.time())``
to calculate the "now" moment instead of ``datetime.utcnow()``.

The "basetime" is an imaginary UTC time when the loop clock was zero (``0.0``)
and is calculated as ``datetime.utcnow() - timedelta(seconds=loop.time())``
(assuming these two calls are almost instant and the precision loss is low).

In normal run mode, the "basetime" remains constant for the entire life time
of an event loop, since both loop time and wall-clock time move forward with
the same speed: the calculation of "basetime" always produces the same result.

In test mode, the loop time is mocked and moves as events (e.g. sleeps) happen:
it can move (much) faster than the wall-clock time, e.g. 100s of loop seconds
in 1/100th of a wall-clock second; or it can freeze and not move at all.

PROBLEMATIC INACCURACY:

Because of a highly unprecise and everchanging component in the formula
of the "basetime" — the non-mockable UTC clock — the "basetime" calculation
can give different results at different times even if executed fast enough.

To reduce the inaccuracy introduced by sequential UTC time measurements,
we calculate the "basetime" once per every global state object created
and push it down to owned state objects of the individual handlers
in this halding cycle of this resource object in this unit-test.

That gives us sufficient accuracy while remaining simple enough, assuming that
there are no multiple concurrent global state objects per single unit-test.
_(An alternative would be to calculate the "basetime" on event loop creation
or to cache it per event loop in a global WeakDict, but that is an overkill.)_

SUFFICIENT ACCURACY:

With this approach and ``looptime``__, we can detach from the wall-clock time
in tests and simulate the time's rapid movement into the future by "recovering"
the "now" moment as ``basetime + timedelta(seconds=loop.time())`` (see above) —
without wall-clock delays or hitting the issues with code execution overhead.

Note that there is no UTC clock involved now, only the controled loop clock,
so multiple sequential calculation will lead to predictable abd precise results,
especially when the loop clock is frozen (i.e. constant for a short duration).

__ https://github.com/nolar/looptime

USER PERSPECTIVE:

This time math is never exposed to users and never persisted in storages.
It is used only internally to decouple the operator routines from the system
clock and strictly couple it to the time of the loop.

IMPLEMENTATION DETAILS:

Q: Why do we store UTC time in the fields instead of the floats with loop time?
A: If we store floats in the fields, we need to do the math on every
fetching/storing operation, which introduces minor divergence in supposedly
constant data as stored in the external storages. Instead, we only calculate
the "now" moment. As a result, the precision loss is seen only at runtime checks
and is indistinguishanle from the loop clock sensitivity.
"""
import asyncio
import collections.abc
import copy
import dataclasses
import datetime
from collections.abc import Collection, Iterable, Iterator, Mapping
from typing import Any, NamedTuple, overload

import iso8601

from kopf._cogs.configs import progress
from kopf._cogs.structs import bodies, ids, patches
from kopf._core.actions import execution


@dataclasses.dataclass(frozen=True)
class HandlerState(execution.HandlerState):
    """
    A persisted state of a single handler, as stored on the resource's status.

    Note the difference: :class:`Outcome` is for in-memory results of handlers,
    which is then additionally converted before being storing as a state.

    Active handler states are those used in .done/.delays for the current
    handling cycle & the current cause. Passive handler states are those
    carried over for logging of counts/extras, and for final state purging,
    but not participating in the current handling cycle.
    """

    active: bool  # whether it is used in done/delays [T] or only in counters/purges [F].
    basetime: datetime.datetime  # a moment when the loop time was zero
    started: datetime.datetime
    stopped: datetime.datetime | None = None  # None means it is still running (e.g. delayed).
    delayed: datetime.datetime | None = None  # None means it is finished (succeeded/failed).
    purpose: str | None = None  # None is a catch-all marker for upgrades/rollbacks.
    retries: int = 0
    success: bool = False
    failure: bool = False
    message: str | None = None
    subrefs: Collection[ids.HandlerId] = ()  # ids of actual sub-handlers of all levels deep.
    _origin: progress.ProgressRecord | None = None  # to check later if it has actually changed.

    @property
    def finished(self) -> bool:
        return bool(self.success or self.failure)

    @property
    def sleeping(self) -> bool:
        now = self.basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        return not self.finished and self.delayed is not None and self.delayed > now

    @property
    def awakened(self) -> bool:
        return bool(not self.finished and not self.sleeping)

    @property
    def runtime(self) -> datetime.timedelta:
        now = self.basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        return now - self.started

    @classmethod
    def from_scratch(
            cls,
            *,
            basetime: datetime.datetime,
            purpose: str | None = None,
    ) -> "HandlerState":
        now = basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        return cls(active=True, basetime=basetime, started=now, purpose=purpose)

    @classmethod
    def from_storage(
            cls,
            __d: progress.ProgressRecord,
            *,
            basetime: datetime.datetime,
    ) -> "HandlerState":
        now = basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        return cls(
            active=False,
            basetime=basetime,
            started=parse_iso8601(__d.get('started')) or now,
            stopped=parse_iso8601(__d.get('stopped')),
            delayed=parse_iso8601(__d.get('delayed')),
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
            started=None if self.started is None else format_iso8601(self.started),
            stopped=None if self.stopped is None else format_iso8601(self.stopped),
            delayed=None if self.delayed is None else format_iso8601(self.delayed),
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
            purpose: str | None,
    ) -> "HandlerState":
        return dataclasses.replace(self, purpose=purpose)

    def with_outcome(
            self,
            outcome: execution.Outcome,
    ) -> "HandlerState":
        now = self.basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        cls = type(self)
        return cls(
            active=self.active,
            basetime=self.basetime,
            purpose=self.purpose,
            started=self.started,
            stopped=self.stopped if self.stopped is not None else now if outcome.final else None,
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

    # Eliminate even the smallest microsecond-scale deviations by using the shared base time.
    # The deviations can come from UTC wall-clock time slowly moving during the run (CPU overhead).
    basetime: datetime.datetime

    def __init__(
            self,
            __src: Mapping[ids.HandlerId, HandlerState],
            *,
            basetime: datetime.datetime,
            purpose: str | None = None,
    ):
        super().__init__()
        self._states = dict(__src)
        self.purpose = purpose
        self.basetime = basetime

    @classmethod
    def from_scratch(cls) -> "State":
        return cls({}, basetime=_get_basetime())

    @classmethod
    def from_storage(
            cls,
            *,
            body: bodies.Body,
            storage: progress.ProgressStorage,
            handlers: Iterable[execution.Handler],
    ) -> "State":
        basetime = _get_basetime()
        handler_ids = {handler.id for handler in handlers}
        handler_states: dict[ids.HandlerId, HandlerState] = {}
        for handler_id in handler_ids:
            content = storage.fetch(key=handler_id, body=body)
            if content is not None:
                handler_states[handler_id] = HandlerState.from_storage(content, basetime=basetime)
        return cls(handler_states, basetime=basetime)

    def with_purpose(
            self,
            purpose: str | None,
            handlers: Iterable[execution.Handler] = (),  # to be re-purposed
    ) -> "State":
        handler_states: dict[ids.HandlerId, HandlerState] = dict(self)
        for handler in handlers:
            handler_states[handler.id] = handler_states[handler.id].with_purpose(purpose)
        cls = type(self)
        return cls(handler_states, basetime=self.basetime, purpose=purpose)

    def with_handlers(
            self,
            handlers: Iterable[execution.Handler],
    ) -> "State":
        handler_states: dict[ids.HandlerId, HandlerState] = dict(self)
        for handler in handlers:
            if handler.id not in handler_states:
                handler_states[handler.id] = HandlerState.from_scratch(
                    basetime=self.basetime, purpose=self.purpose)
            else:
                handler_states[handler.id] = handler_states[handler.id].as_active()
        cls = type(self)
        return cls(handler_states, basetime=self.basetime, purpose=self.purpose)

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
        }, basetime=self.basetime, purpose=self.purpose)

    def without_successes(self) -> "State":
        cls = type(self)
        return cls({
            handler_id: handler_state
            for handler_id, handler_state in self._states.items()
            if not handler_state.success # i.e. failures & in-progress/retrying
        }, basetime=self.basetime)

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
    def delay(self) -> float | None:
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
        now = self.basetime + datetime.timedelta(seconds=asyncio.get_running_loop().time())
        return [
            max(0.0, (handler_state.delayed - now).total_seconds()) if handler_state.delayed else 0
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
def format_iso8601(val: None) -> None:
    ...


@overload
def format_iso8601(val: datetime.datetime) -> str:
    ...


def format_iso8601(val: datetime.datetime | None) -> str | None:
    return None if val is None else val.isoformat(timespec='microseconds')


@overload
def parse_iso8601(val: None) -> None:
    ...


@overload
def parse_iso8601(val: str) -> datetime.datetime:
    ...


def parse_iso8601(val: str | None) -> datetime.datetime | None:
    return None if val is None else iso8601.parse_date(val, default_timezone=None)


def _get_basetime() -> datetime.datetime:
    loop = asyncio.get_running_loop()
    return datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=loop.time())
