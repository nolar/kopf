import asyncio

import aiohttp.web
import async_timeout
import pytest

import kopf
from kopf.reactor.observation import process_discovered_resource_event
from kopf.structs.bodies import RawBody, RawEvent
from kopf.structs.references import NAMESPACES, Insights, Resource

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
    mock = resp_mocker(return_value=aresponses.Response(status=404))
    aresponses.add(hostname, '/apis/group1/version1', 'get', mock)
    return mock


@pytest.fixture(params=[
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def handlers(request, registry):
    @request.param('group1', 'version1', 'plural1')
    def fn(**_): ...



async def test_initial_listing_is_ignored(registry, apis_mock, group1_mock):
    insights = Insights()
    e1 = RawEvent(type=None, object=RawBody(spec={'group': 'group1'}))

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0))
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.1) as timeout:
            async with insights.revised:
                await insights.revised.wait()
    await task
    assert timeout.expired
    assert not insights.resources
    assert not apis_mock.called
    assert not group1_mock.called


@pytest.mark.usefixtures('handlers')
@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED'])
async def test_followups_for_addition(registry, apis_mock, group1_mock, timer, etype):
    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights = Insights()

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1.0):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 1.0
    assert insights.resources == {r1}
    assert apis_mock.called
    assert group1_mock.called


@pytest.mark.usefixtures('handlers')
@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED', 'DELETED'])
async def test_followups_for_deletion_of_resource(registry, apis_mock, group1_empty_mock, timer, etype):
    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights = Insights()
    insights.resources.add(r1)

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1.0):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 1.0
    assert not insights.resources
    assert apis_mock.called
    assert group1_empty_mock.called


@pytest.mark.usefixtures('handlers')
@pytest.mark.parametrize('etype', ['ADDED', 'MODIFIED', 'DELETED'])
async def test_followups_for_deletion_of_group(registry, apis_mock, group1_404mock, timer, etype):
    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights = Insights()
    insights.resources.add(r1)

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1.0):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 1.0
    assert not insights.resources
    assert apis_mock.called
    assert group1_404mock.called


@pytest.mark.usefixtures('handlers')
@pytest.mark.parametrize('etype', ['DELETED'])
async def test_followups_for_deletion_of_group(registry, apis_mock, group1_404mock, timer, etype):
    e1 = RawEvent(type=etype, object=RawBody(spec={'group': 'group1'}))
    r1 = Resource(group='group1', version='version1', plural='plural1')
    insights = Insights()
    insights.resources.add(r1)

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1.0):
        async with insights.revised:
            await insights.revised.wait()
    await task
    assert 0.1 < timer.seconds < 1.0
    assert not insights.resources
    assert apis_mock.called
    assert group1_404mock.called


@pytest.mark.parametrize('etype', ['DELETED'])
async def test_backbone_is_filled(registry, core_mock, corev1_mock, timer, etype):
    e1 = RawEvent(type=etype, object=RawBody(spec={'group': ''}))
    insights = Insights()

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await process_discovered_resource_event(
            insights=insights, raw_event=e1, registry=registry)

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1.0):
        await insights.backbone.wait_for(NAMESPACES)
    await task
    assert 0.1 < timer.seconds < 1.0
    assert NAMESPACES in insights.backbone
    assert core_mock.called
    assert corev1_mock.called
