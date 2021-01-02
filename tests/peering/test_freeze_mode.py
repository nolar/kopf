import asyncio
import dataclasses
from unittest.mock import Mock

import freezegun
import pytest

from kopf.engines.peering import process_peering_event
from kopf.structs import bodies, primitives
from kopf.structs.references import Resource

NAMESPACED_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'kopfpeerings')
CLUSTER_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'clusterkopfpeerings')


@dataclasses.dataclass(frozen=True, eq=False)
class K8sMocks:
    patch_obj: Mock


@pytest.fixture(autouse=True)
def k8s_mocked(mocker, resp_mocker):
    # We mock on the level of our own K8s API wrappers, not the K8s client.
    return K8sMocks(
        patch_obj=mocker.patch('kopf.clients.patching.patch_obj', return_value={}),
    )


@pytest.fixture
async def replenished(mocker):
    # Make sure that freeze-sleeps are not actually executed, i.e. exit instantly.
    replenished = asyncio.Event()
    replenished.set()
    mocker.patch.object(replenished, 'wait')  # to avoid RuntimeWarnings for unwaited coroutines
    return replenished


@pytest.mark.parametrize('our_name, our_namespace, their_name, their_namespace', [
    ['our-name', 'our-namespace', 'their-name', 'their-namespace'],
    ['our-name', 'our-namespace', 'their-name', 'our-namespace'],
    ['our-name', 'our-namespace', 'their-name', None],
    ['our-name', 'our-namespace', 'our-name', 'their-namespace'],
    ['our-name', 'our-namespace', 'our-name', None],
    ['our-name', None, 'their-name', 'their-namespace'],
    ['our-name', None, 'their-name', 'our-namespace'],
    ['our-name', None, 'their-name', None],
    ['our-name', None, 'our-name', 'their-namespace'],
    ['our-name', None, 'their-name', 'our-namespace'],
])
@pytest.mark.parametrize('peering_resource', [NAMESPACED_PEERING_RESOURCE, CLUSTER_PEERING_RESOURCE])
async def test_other_peering_objects_are_ignored(
        mocker, k8s_mocked, settings, replenished,
        peering_resource, our_name, our_namespace, their_name, their_namespace):

    status = mocker.Mock()
    status.items.side_effect = Exception("This should not be called.")
    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': their_name, 'namespace': their_namespace},
            'status': status,
        })

    wait_for = mocker.patch('asyncio.wait_for')

    settings.peering.name = our_name
    await process_peering_event(
        raw_event=event,
        freeze_toggle=primitives.Toggle(),
        replenished=replenished,
        autoclean=False,
        identity='id',
        resource=peering_resource,
        settings=settings,
        namespace=our_namespace,
    )
    assert not status.items.called
    assert not k8s_mocked.patch_obj.called
    assert wait_for.call_count == 0


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_higher_priority_peer_when_initially_off(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 101,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(False)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_off()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_on()
    assert wait_for.call_count == 1
    assert 9 < wait_for.call_args[1]['timeout'] < 10
    assert not k8s_mocked.patch_obj.called
    assert_logs(["Freezing operations in favour of"], prohibited=[
        "Possibly conflicting operators",
        "Freezing all operators, including self",
        "Resuming operations after the freeze",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_higher_priority_peer_when_already_on(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 101,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(True)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_on()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_on()
    assert wait_for.call_count == 1
    assert 9 < wait_for.call_args[1]['timeout'] < 10
    assert not k8s_mocked.patch_obj.called
    assert_logs([], prohibited=[
        "Possibly conflicting operators",
        "Freezing all operators, including self",
        "Freezing operations in favour of",
        "Resuming operations after the freeze",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_off_for_lower_priority_peer_when_initially_on(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 99,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(True)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_on()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_off()
    assert wait_for.call_count == 0
    assert not k8s_mocked.patch_obj.called
    assert_logs(["Resuming operations after the freeze"], prohibited=[
        "Possibly conflicting operators",
        "Freezing all operators, including self",
        "Freezing operations in favour of",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_lower_priority_peer_when_already_off(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 99,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(False)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_off()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_off()
    assert wait_for.call_count == 0
    assert not k8s_mocked.patch_obj.called
    assert_logs([], prohibited=[
        "Possibly conflicting operators",
        "Freezing all operators, including self",
        "Freezing operations in favour of",
        "Resuming operations after the freeze",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_same_priority_peer_when_initially_off(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(False)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_off()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_on()
    assert wait_for.call_count == 1
    assert 9 < wait_for.call_args[1]['timeout'] < 10
    assert not k8s_mocked.patch_obj.called
    assert_logs([
        "Possibly conflicting operators",
        "Freezing all operators, including self",
    ], prohibited=[
        "Freezing operations in favour of",
        "Resuming operations after the freeze",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_same_priority_peer_when_already_on(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(True)
    wait_for = mocker.patch('asyncio.wait_for')

    caplog.set_level(0)
    assert freeze_toggle.is_on()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_on()
    assert wait_for.call_count == 1
    assert 9 < wait_for.call_args[1]['timeout'] < 10
    assert not k8s_mocked.patch_obj.called
    assert_logs([
        "Possibly conflicting operators",
    ], prohibited=[
        "Freezing all operators, including self",
        "Freezing operations in favour of",
        "Resuming operations after the freeze",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('priority', [100, 101])
async def test_resumes_immediately_on_expiration_of_blocking_peers(
        mocker, k8s_mocked, replenished, caplog, assert_logs, settings, priority):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': 'namespace'},  # for matching
            'status': {
                'higher-prio': {
                    'priority': priority,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100

    freeze_toggle = primitives.Toggle(True)
    wait_for = mocker.patch('asyncio.wait_for', side_effect=asyncio.TimeoutError)

    caplog.set_level(0)
    assert freeze_toggle.is_on()
    await process_peering_event(
        raw_event=event,
        freeze_toggle=freeze_toggle,
        replenished=replenished,
        autoclean=False,
        namespace='namespace',
        resource=NAMESPACED_PEERING_RESOURCE,
        identity='id',
        settings=settings,
    )
    assert freeze_toggle.is_on()
    assert wait_for.call_count == 1
    assert 9 < wait_for.call_args[1]['timeout'] < 10
    assert k8s_mocked.patch_obj.called
