import freezegun
import pytest

from kopf._cogs.aiokits import aiotoggles
from kopf._cogs.structs import bodies
from kopf._core.engines.peering import process_peering_event


async def test_other_peering_objects_are_ignored(
        mocker, k8s_mocked, settings,
        peering_resource, peering_namespace):

    status = mocker.Mock()
    status.items.side_effect = Exception("This should not be called.")
    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'their-name'},
            'status': status,
        })

    settings.peering.name = 'our-name'
    await process_peering_event(
        raw_event=event,
        autoclean=False,
        identity='id',
        settings=settings,
        namespace=peering_namespace,
        resource=peering_resource,
    )
    assert not status.items.called
    assert not k8s_mocked.patch.called
    assert k8s_mocked.sleep.call_count == 0


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_higher_priority_peer_when_initially_off(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(False)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert k8s_mocked.sleep.call_count == 1
    assert 9 < k8s_mocked.sleep.call_args[0][0][0] < 10
    assert not k8s_mocked.patch.called
    assert_logs(["Pausing operations in favour of"], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Resuming operations after the pause",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_higher_priority_peer_when_already_on(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(True)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert k8s_mocked.sleep.call_count == 1
    assert 9 < k8s_mocked.sleep.call_args[0][0][0] < 10
    assert not k8s_mocked.patch.called
    assert_logs([], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_off_for_lower_priority_peer_when_initially_on(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(True)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert k8s_mocked.sleep.call_count == 1
    assert k8s_mocked.sleep.call_args[0][0] == []
    assert not k8s_mocked.patch.called
    assert_logs(["Resuming operations after the pause"], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_lower_priority_peer_when_already_off(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(False)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert k8s_mocked.sleep.call_count == 1
    assert k8s_mocked.sleep.call_args[0][0] == []
    assert not k8s_mocked.patch.called
    assert_logs([], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_same_priority_peer_when_initially_off(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(False)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert k8s_mocked.sleep.call_count == 1
    assert 9 < k8s_mocked.sleep.call_args[0][0][0] < 10
    assert not k8s_mocked.patch.called
    assert_logs([
        "Possibly conflicting operators",
        "Pausing all operators, including self",
    ], prohibited=[
        "Pausing operations in favour of",
        "Resuming operations after the pause",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_same_priority_peer_when_already_on(
        k8s_mocked, caplog, assert_logs, settings,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(True)
    k8s_mocked.sleep.return_value = 1  # as if interrupted by stream pressure

    caplog.set_level(0)
    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert k8s_mocked.sleep.call_count == 1
    assert 9 < k8s_mocked.sleep.call_args[0][0][0] < 10
    assert not k8s_mocked.patch.called
    assert_logs([
        "Possibly conflicting operators",
    ], prohibited=[
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('priority', [100, 101])
async def test_resumes_immediately_on_expiration_of_blocking_peers(
        k8s_mocked, caplog, assert_logs, settings, priority,
        peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
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

    conflicts_found = aiotoggles.Toggle(True)
    k8s_mocked.sleep.return_value = None  # as if finished sleeping uninterrupted

    caplog.set_level(0)
    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert k8s_mocked.sleep.call_count == 1
    assert 9 < k8s_mocked.sleep.call_args[0][0][0] < 10
    assert k8s_mocked.patch.called
