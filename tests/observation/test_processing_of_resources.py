import asyncio

import aiohttp.web
import pytest

import kopf
from kopf._cogs.structs.bodies import RawBody, RawEvent
from kopf._cogs.structs.references import NAMESPACES, Insights, Resource
from kopf._core.reactor.observation import process_discovered_resource_event

# Implementation awareness: the events only trigger the re-scan, so the fields can be reduced
# to only the group name which is being rescanned. Other fields are ignored in the events.
# The actual data is taken from the API. It is tested elsewhere, so we rely on its correctness here.
GROUP1_EVENT = RawBody(spec={'group': 'group1'})


@pytest.fixture()
def core_mock(resp_mocker, aresponses, hostname):
    mock = resp_mocker(return_value=aiohttp.web.json_response({'versions': ['v1']}))
    aresponses.add(hostname, '/api', 'get', mock)
    return mock


@pytest.fixture()
def apis_mock(resp_mocker, aresponses, hostname):
    mock = resp_mocker(return_value=aiohttp.web.json_response({'groups': [
        {
            'name': 'group1',
            'preferredVersion': {'version': 'version1'},
            'versions': [{'version': 'version1'}],
        },
    ]}))
    aresponses.add(hostname, '/apis', 'get', mock)
    return mock


@pytest.fixture()
def corev1_mock(resp_mocker, aresponses, hostname, core_mock):
    mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
        {
            'kind': 'Namespace',
            'name': 'namespaces',
            'singularName': 'namespace',
            'namespaced': False,
            'categories': [],
            'shortNames': [],
            'verbs': ['list', 'watch', 'patch'],
        },
    ]}))
    aresponses.add(hostname, '/api/v1', 'get', mock)
    return mock


@pytest.fixture()
def group1_mock(resp_mocker, aresponses, hostname, apis_mock):
    mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': [
        {
            'kind': 'kind1',
            'name': 'plural1',
            'singularName': 'singular1',
            'namespaced': True,
            'categories': ['category1a', 'category1b'],
            'shortNames': ['shortname1a', 'shortname1b'],
            'verbs': ['list', 'watch', 'patch'],
        },
    ]}))
    aresponses.add(hostname, '/apis/group1/version1', 'get', mock)
    return mock


@pytest.fixture()
def group1_empty_mock(resp_mocker, aresponses, hostname, apis_mock):
    mock = resp_mocker(return_value=aiohttp.web.json_response({'resources': []}))
    aresponses.add(hostname, '/apis/group1/version1', 'get', mock)
    return mock


@pytest.fixture()
def group1_404mock(resp_mocker, aresponses, hostname, apis_mock):
    mock = resp_mocker(return_value=aresponses.Response(status=404, reason='oops'))
    aresponses.add(hostname, '/apis/group1/version1', 'get', mock)
    return mock


@pytest.fixture()
async def insights():
    return Insights()


@pytest.fixture(params=[
    (kopf.on.event, 'watched_resources'),
    (kopf.daemon, 'watched_resources'),
    (kopf.timer, 'watched_resources'),
    (kopf.index, 'watched_resources'),
    (kopf.index, 'indexed_resources'),
    (kopf.on.resume, 'watched_resources'),
    (kopf.on.create, 'watched_resources'),
    (kopf.on.update, 'watched_resources'),
    (kopf.on.delete, 'watched_resources'),
    (kopf.on.validate, 'webhook_resources'),
    (kopf.on.mutate, 'webhook_resources'),
])
def insights_resources(request, registry, insights):
    decorator, insights_field = request.param

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    return getattr(insights, insights_field)


@pytest.mark.parametrize('decorator', [kopf.on.validate, kopf.on.mutate])
@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED'])
async def test_nonwatchable_resources_are_ignored(
        settings, registry, apis_mock, group1_mock, looptime, etype, decorator, insights):

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(1.23))
    async with insights.revised:
        await insights.revised.wait()
    await task
    assert looptime == 1.23
    assert not insights.watched_resources
    assert apis_mock.called
    assert group1_mock.called


async def test_initial_listing_is_ignored(
        settings, registry, apis_mock, group1_mock, looptime, insights):

    e1 = RawEvent(type=None, object=RawBody(spec={'group': 'group1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(0))
    with pytest.raises(asyncio.TimeoutError):
        async with insights.revised:
            await asyncio.wait_for(insights.revised.wait(), timeout=1.23)
    await task

    assert looptime == 1.23
    assert not insights.indexed_resources
    assert not insights.watched_resources
    assert not insights.webhook_resources
    assert not apis_mock.called
    assert not group1_mock.called


@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED'])
async def test_followups_for_addition(
        settings, registry, apis_mock, group1_mock, looptime, etype, insights, insights_resources):

    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(1.23))
    async with insights.revised:
        await insights.revised.wait()
    await task

    assert looptime == 1.23
    assert insights_resources == {r1}
    assert apis_mock.called
    assert group1_mock.called


@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED', 'DELETED'])
async def test_followups_for_deletion_of_resource(
        settings, registry, apis_mock, group1_empty_mock, looptime, etype,
        insights, insights_resources):

    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights_resources.add(r1)

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(1.23))
    async with insights.revised:
        await insights.revised.wait()
    await task

    assert looptime == 1.23
    assert not insights_resources
    assert apis_mock.called
    assert group1_empty_mock.called


@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED', 'DELETED'])
async def test_followups_for_deletion_of_group(
        settings, registry, apis_mock, group1_404mock, looptime, etype, insights, insights_resources):

    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights_resources.add(r1)

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(1.23))
    async with insights.revised:
        await insights.revised.wait()
    await task

    assert looptime == 1.23
    assert not insights_resources
    assert apis_mock.called
    assert group1_404mock.called


@pytest.mark.parametrize('etype', ['DELETED'])
async def test_backbone_is_filled(
        settings, registry, core_mock, corev1_mock, looptime, etype, insights):

    e1 = RawEvent(type=etype, object=RawBody(spec={'group': ''}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry, settings=settings)

    task = asyncio.create_task(delayed_injection(1.23))
    await insights.backbone.wait_for(NAMESPACES)
    await task

    assert looptime == 1.23
    assert NAMESPACES in insights.backbone
    assert core_mock.called
    assert corev1_mock.called
