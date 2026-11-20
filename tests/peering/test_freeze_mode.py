import asyncio

import aiohttp
import freezegun
import pytest

from kopf._cogs.aiokits import aiotoggles
from kopf._cogs.clients.errors import APIForbiddenError, APIServerError, \
                                      APITooManyRequestsError, APIUnauthorizedError
from kopf._cogs.structs import bodies
from kopf._core.engines.peering import process_peering_event


async def test_other_peering_objects_are_ignored(
        mocker, k8s_mocked, assert_logs, settings, looptime,
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
    assert looptime == 0
    assert_logs(prohibited=["patching"])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_higher_priority_peer_when_initially_off(
        k8s_mocked, assert_logs, settings, looptime,
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
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs(["Pausing operations in favour of"], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_higher_priority_peer_when_already_on(
        k8s_mocked, assert_logs, settings, looptime,
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
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs(prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_off_for_lower_priority_peer_when_initially_on(
        k8s_mocked, assert_logs, settings, looptime,
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
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert looptime == 0
    assert not k8s_mocked.patch.called
    assert_logs(["Resuming operations after the pause"], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_lower_priority_peer_when_already_off(
        k8s_mocked, assert_logs, settings, looptime,
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
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert looptime == 0
    assert not k8s_mocked.patch.called
    assert_logs(prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_toggled_on_for_same_priority_peer_when_initially_off_without_sharding(
        k8s_mocked, assert_logs, settings, looptime,
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
    settings.watching.shard_selectors.clear()

    conflicts_found = aiotoggles.Toggle(False)
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs([
        "Possibly conflicting operators",
        "Pausing all operators, including self",
    ], prohibited=[
        "Pausing operations in favour of",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_same_priority_peer_when_initially_off_with_sharding(
        k8s_mocked, assert_logs, settings, looptime,
        peering_resource, peering_namespace, caplog):

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
    settings.watching.shard_selectors.clear()
    settings.watching.shard_selectors['whatever'] = 'just-the-existance-matters'

    conflicts_found = aiotoggles.Toggle(False)
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_off()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs([
    ], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_ignored_for_same_priority_peer_when_already_on_without_sharding(
        k8s_mocked, assert_logs, settings, looptime,
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
    settings.watching.shard_selectors.clear()

    conflicts_found = aiotoggles.Toggle(True)
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs([
        "Possibly conflicting operators",
    ], prohibited=[
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "Resuming operations after the pause",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_resumes_for_same_priority_peer_when_already_on_with_sharding(
        k8s_mocked, assert_logs, settings, looptime,
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
    settings.watching.shard_selectors.clear()
    settings.watching.shard_selectors['whatever'] = 'just-the-existance-matters'

    conflicts_found = aiotoggles.Toggle(True)
    stream_pressure = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(1.23, stream_pressure.set)

    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_off()
    assert looptime == 1.23
    assert not k8s_mocked.patch.called
    assert_logs([
        "Resuming operations after the pause",
    ], prohibited=[
        "Possibly conflicting operators",
        "Pausing all operators, including self",
        "Pausing operations in favour of",
        "patching",
    ])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('priority', [100, 101])
async def test_resumes_immediately_on_expiration_of_blocking_peers(
        k8s_mocked, assert_logs, settings, priority, looptime,
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
    stream_pressure = asyncio.Event()

    assert conflicts_found.is_on()
    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=stream_pressure,
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )
    assert conflicts_found.is_on()
    assert looptime == 9.876544
    assert k8s_mocked.patch.called
    assert_logs(prohibited=["patching"])


TEMPORARY_ERRORS = [
    aiohttp.ClientConnectionError(),
    aiohttp.ClientPayloadError(),
    TimeoutError(),
    APIServerError("Unavailable.", status=503, headers={}),
    APITooManyRequestsError("Too many requests.", status=429, headers={}),
]
FATAL_ERRORS = [
    APIUnauthorizedError("Unauthorized.", status=401, headers={}),
    APIForbiddenError("Forbidden.", status=403, headers={}),
    RuntimeError("Unexpected."),
]


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
async def test_own_stale_record_is_not_cleaned_as_dead_peer(
        k8s_mocked, settings, peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='MODIFIED',
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},
            'status': {
                'id': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-30T00:00:00'
                },
            },
        })
    settings.peering.name = 'name'

    await process_peering_event(
        raw_event=event,
        autoclean=True,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )

    assert not k8s_mocked.patch.called


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('exc', TEMPORARY_ERRORS)
async def test_temporary_errors_in_cleaning_dead_peers_are_tolerated(
        k8s_mocked, settings, exc, assert_logs, peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
            'status': {
                'dead-peer': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-30T00:00:00'
                },
            },
        })
    settings.peering.name = 'name'
    k8s_mocked.patch.side_effect = exc

    await process_peering_event(
        raw_event=event,
        autoclean=True,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )

    assert k8s_mocked.patch.called
    assert_logs(["Cleaning the dead peers .* failed; will retry later"])


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('exc', TEMPORARY_ERRORS)
async def test_conflict_is_applied_after_dead_peer_cleanup_fails(
        k8s_mocked, settings, exc, peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
            'status': {
                'dead-peer': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-30T00:00:00'
                },
                'higher-prio': {
                    'priority': 101,
                    'lifetime': 10,
                    'lastseen': '2020-12-31T23:59:59'
                },
            },
        })
    settings.peering.name = 'name'
    settings.peering.priority = 100
    k8s_mocked.patch.side_effect = exc
    conflicts_found = aiotoggles.Toggle(False)

    await process_peering_event(
        raw_event=event,
        conflicts_found=conflicts_found,
        stream_pressure=asyncio.Event(),
        autoclean=True,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )

    assert conflicts_found.is_on()
    assert k8s_mocked.patch.called


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('exc', TEMPORARY_ERRORS)
async def test_temporary_errors_in_touching_on_expiration_are_tolerated(
        k8s_mocked, settings, exc, looptime, assert_logs, peering_resource, peering_namespace):

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
    k8s_mocked.patch.side_effect = exc

    await process_peering_event(
        raw_event=event,
        conflicts_found=aiotoggles.Toggle(True),
        stream_pressure=asyncio.Event(),
        autoclean=False,
        namespace=peering_namespace,
        resource=peering_resource,
        identity='id',
        settings=settings,
    )

    assert looptime == 9.876544
    assert k8s_mocked.patch.called
    assert_logs(["Touching the expired peers .* failed; will retry later"])


# Authentication and authorization errors are not tolerated: the operator must fail,
# so that it is restarted as a whole with fresh credentials & permissions.
@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('exc', FATAL_ERRORS)
async def test_other_errors_in_cleaning_dead_peers_escalate(
        k8s_mocked, settings, exc, peering_resource, peering_namespace):

    event = bodies.RawEvent(
        type='ADDED',  # irrelevant
        object={
            'metadata': {'name': 'name', 'namespace': peering_namespace},  # for matching
            'status': {
                'dead-peer': {
                    'priority': 100,
                    'lifetime': 10,
                    'lastseen': '2020-12-30T00:00:00'
                },
            },
        })
    settings.peering.name = 'name'
    k8s_mocked.patch.side_effect = exc

    with pytest.raises(type(exc)):
        await process_peering_event(
            raw_event=event,
            autoclean=True,
            namespace=peering_namespace,
            resource=peering_resource,
            identity='id',
            settings=settings,
        )


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
@pytest.mark.parametrize('exc', FATAL_ERRORS)
async def test_other_errors_in_touching_on_expiration_escalate(
        k8s_mocked, settings, exc, looptime, peering_resource, peering_namespace):

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
    k8s_mocked.patch.side_effect = exc

    with pytest.raises(type(exc)):
        await process_peering_event(
            raw_event=event,
            conflicts_found=aiotoggles.Toggle(True),
            stream_pressure=asyncio.Event(),
            autoclean=False,
            namespace=peering_namespace,
            resource=peering_resource,
            identity='id',
            settings=settings,
        )
