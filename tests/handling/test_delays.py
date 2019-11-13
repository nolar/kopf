import asyncio
import datetime
import logging

import freezegun
import pytest

import kopf
from kopf.reactor.causation import Reason, HANDLER_REASONS
from kopf.reactor.handling import TemporaryError
from kopf.reactor.handling import WAITING_KEEPALIVE_INTERVAL
from kopf.reactor.handling import resource_handler
from kopf.reactor.states import HandlerState
from kopf.structs.containers import ResourceMemories
from kopf.structs.finalizers import FINALIZER


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
@pytest.mark.parametrize('now, delayed_iso, delay', [
    ['2020-01-01T00:00:00', '2020-01-01T00:04:56.789000', 4 * 60 + 56.789],
], ids=['fast'])
async def test_delayed_handlers_progress(
        registry, handlers, resource, cause_mock, cause_reason,
        caplog, assert_logs, k8s_mocked, now, delayed_iso, delay):
    caplog.set_level(logging.DEBUG)

    handlers.create_mock.side_effect = TemporaryError("oops", delay=delay)
    handlers.update_mock.side_effect = TemporaryError("oops", delay=delay)
    handlers.delete_mock.side_effect = TemporaryError("oops", delay=delay)
    handlers.resume_mock.side_effect = TemporaryError("oops", delay=delay)

    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason

    with freezegun.freeze_time(now):
        await resource_handler(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            resource=resource,
            memories=ResourceMemories(),
            event={'type': event_type, 'object': cause_mock.body},
            freeze=asyncio.Event(),
            replenished=asyncio.Event(),
            event_queue=asyncio.Queue(),
        )

    assert handlers.create_mock.call_count == (1 if cause_reason == Reason.CREATE else 0)
    assert handlers.update_mock.call_count == (1 if cause_reason == Reason.UPDATE else 0)
    assert handlers.delete_mock.call_count == (1 if cause_reason == Reason.DELETE else 0)
    assert handlers.resume_mock.call_count == (1 if cause_reason == Reason.RESUME else 0)

    assert not k8s_mocked.sleep_or_wait.called
    assert k8s_mocked.patch_obj.called

    fname = f'{cause_reason}_fn'
    patch = k8s_mocked.patch_obj.call_args_list[0][1]['patch']
    assert patch['status']['kopf']['progress'][fname]['delayed'] == delayed_iso

    assert_logs([
        "Invoking handler .+",
        "Handler .+ failed temporarily: oops",
    ])


@pytest.mark.parametrize('cause_reason', HANDLER_REASONS)
@pytest.mark.parametrize('now, delayed_iso, delay', [
    ['2020-01-01T00:00:00', '2020-01-01T00:04:56.789000', 4 * 60 + 56.789],
    ['2020-01-01T00:00:00', '2099-12-31T23:59:59.000000', WAITING_KEEPALIVE_INTERVAL],
], ids=['fast', 'slow'])
async def test_delayed_handlers_sleep(
        registry, handlers, resource, cause_mock, cause_reason,
        caplog, assert_logs, k8s_mocked, now, delayed_iso, delay):
    caplog.set_level(logging.DEBUG)

    # Simulate the original persisted state of the resource.
    started_dt = datetime.datetime.fromisoformat('2000-01-01T00:00:00')  # long time ago is fine.
    delayed_dt = datetime.datetime.fromisoformat(delayed_iso)
    event_type = None if cause_reason == Reason.RESUME else 'irrelevant'
    cause_mock.reason = cause_reason
    cause_mock.body.update({
        'status': {'kopf': {'progress': {
            'create_fn': HandlerState(started=started_dt, delayed=delayed_dt).as_dict(),
            'update_fn': HandlerState(started=started_dt, delayed=delayed_dt).as_dict(),
            'delete_fn': HandlerState(started=started_dt, delayed=delayed_dt).as_dict(),
            'resume_fn': HandlerState(started=started_dt, delayed=delayed_dt).as_dict(),
        }}}
    })
    # make sure the finalizer is added since there are mandatory deletion handlers
    cause_mock.body.setdefault('metadata', {})['finalizers'] = [FINALIZER]

    with freezegun.freeze_time(now):
        await resource_handler(
            lifecycle=kopf.lifecycles.all_at_once,
            registry=registry,
            resource=resource,
            memories=ResourceMemories(),
            event={'type': event_type, 'object': cause_mock.body},
            freeze=asyncio.Event(),
            replenished=asyncio.Event(),
            event_queue=asyncio.Queue(),
        )

    assert not handlers.create_mock.called
    assert not handlers.update_mock.called
    assert not handlers.delete_mock.called
    assert not handlers.resume_mock.called

    # The dummy patch is needed to trigger the further changes. The value is irrelevant.
    assert k8s_mocked.patch_obj.called
    assert 'dummy' in k8s_mocked.patch_obj.call_args_list[-1][1]['patch']['status']['kopf']

    # The duration of sleep should be as expected.
    assert k8s_mocked.sleep_or_wait.called
    assert k8s_mocked.sleep_or_wait.call_args_list[0][0][0] == delay

    assert_logs([
        r"Sleeping for [\d\.]+ seconds",
    ])
