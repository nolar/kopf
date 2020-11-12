from typing import Mapping
from unittest.mock import Mock

import freezegun
import pytest

from kopf.reactor.activities import ActivityError, run_activity
from kopf.reactor.handling import PermanentError, TemporaryError
from kopf.reactor.lifecycles import all_at_once
from kopf.reactor.registries import OperatorRegistry
from kopf.storage.states import HandlerOutcome
from kopf.structs.handlers import Activity, ActivityHandler, HandlerId


@pytest.fixture()
def handler():
    return Mock(id=HandlerId('id'), spec_set=['id'])


def test_activity_error_exception(handler):
    outcome = HandlerOutcome(final=True, handler=handler)
    outcomes: Mapping[HandlerId, HandlerOutcome]
    outcomes = {handler.id: outcome}
    error = ActivityError("message", outcomes=outcomes)
    assert str(error) == "message"
    assert error.outcomes == outcomes


@pytest.mark.parametrize('activity', list(Activity))
async def test_results_are_returned_on_success(settings, activity):

    def sample_fn1(**_):
        return 123

    def sample_fn2(**_):
        return 456

    registry = OperatorRegistry()
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn1, id='id1', activity=activity,
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
    ))
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn2, id='id2', activity=activity,
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
    ))

    results = await run_activity(
        registry=registry,
        settings=settings,
        activity=activity,
        lifecycle=all_at_once,
    )

    assert set(results.keys()) == {'id1', 'id2'}
    assert results['id1'] == 123
    assert results['id2'] == 456


@pytest.mark.parametrize('activity', list(Activity))
async def test_errors_are_raised_aggregated(settings, activity):

    def sample_fn1(**_):
        raise PermanentError("boo!123")

    def sample_fn2(**_):
        raise PermanentError("boo!456")

    registry = OperatorRegistry()
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn1, id='id1', activity=activity,
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
    ))
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn2, id='id2', activity=activity,
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
    ))

    with pytest.raises(ActivityError) as e:
        await run_activity(
            registry=registry,
            settings=settings,
            activity=activity,
            lifecycle=all_at_once,
        )

    assert set(e.value.outcomes.keys()) == {'id1', 'id2'}
    assert e.value.outcomes['id1'].final
    assert e.value.outcomes['id1'].delay is None
    assert e.value.outcomes['id1'].result is None
    assert e.value.outcomes['id1'].exception is not None
    assert e.value.outcomes['id2'].final
    assert e.value.outcomes['id2'].delay is None
    assert e.value.outcomes['id2'].result is None
    assert e.value.outcomes['id2'].exception is not None
    assert str(e.value.outcomes['id1'].exception) == "boo!123"
    assert str(e.value.outcomes['id2'].exception) == "boo!456"


@pytest.mark.parametrize('activity', list(Activity))
async def test_errors_are_cascaded_from_one_of_the_originals(settings, activity):

    def sample_fn(**_):
        raise PermanentError("boo!")

    registry = OperatorRegistry()
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn, id='id', activity=activity,
        errors=None, timeout=None, retries=None, backoff=None, cooldown=None,
    ))

    with pytest.raises(ActivityError) as e:
        await run_activity(
            registry=registry,
            settings=settings,
            activity=activity,
            lifecycle=all_at_once,
        )

    assert e.value.__cause__
    assert type(e.value.__cause__) is PermanentError
    assert str(e.value.__cause__) == "boo!"


@pytest.mark.parametrize('activity', list(Activity))
async def test_retries_are_simulated(settings, activity, mocker):
    mock = mocker.MagicMock()

    def sample_fn(**_):
        mock()
        raise TemporaryError('to be retried', delay=0)

    registry = OperatorRegistry()
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn, id='id', activity=activity,
        errors=None, timeout=None, retries=3, backoff=None, cooldown=None,
    ))

    with pytest.raises(ActivityError) as e:
        await run_activity(
            registry=registry,
            settings=settings,
            activity=activity,
            lifecycle=all_at_once,
        )

    assert isinstance(e.value.outcomes['id'].exception, PermanentError)
    assert mock.call_count == 3


@pytest.mark.parametrize('activity', list(Activity))
async def test_delays_are_simulated(settings, activity, mocker):

    def sample_fn(**_):
        raise TemporaryError('to be retried', delay=123)

    registry = OperatorRegistry()
    registry.activity_handlers.append(ActivityHandler(
        fn=sample_fn, id='id', activity=activity,
        errors=None, timeout=None, retries=3, backoff=None, cooldown=None,
    ))

    with freezegun.freeze_time() as frozen:

        async def sleep_or_wait_substitute(*_, **__):
            frozen.tick(123)

        sleep_or_wait = mocker.patch('kopf.reactor.effects.sleep_or_wait',
                                     wraps=sleep_or_wait_substitute)

        with pytest.raises(ActivityError) as e:
            await run_activity(
                registry=registry,
                settings=settings,
                activity=activity,
                lifecycle=all_at_once,
            )

    assert sleep_or_wait.call_count >= 3  # 3 retries, 1 sleep each
    assert sleep_or_wait.call_count <= 4  # 3 retries, 1 final success (delay=None), not more
    if sleep_or_wait.call_count > 3:
        sleep_or_wait.call_args_list[-1][0][0] is None
