import asyncio

import async_timeout
import pytest

from kopf.structs.references import CLUSTER_PEERINGS, CRDS, EVENTS, NAMESPACED_PEERINGS, \
                                    NAMESPACES, Backbone, Resource, Selector


@pytest.mark.parametrize('selector', [
    CRDS, EVENTS, NAMESPACES, CLUSTER_PEERINGS, NAMESPACED_PEERINGS,
])
def test_empty_backbone(selector: Selector):
    backbone = Backbone()
    assert len(backbone) == 0
    assert set(backbone) == set()
    with pytest.raises(KeyError):
        assert backbone[selector]


@pytest.mark.parametrize('selector, resource', [
    (CRDS, Resource('apiextensions.k8s.io', 'v1beta1', 'customresourcedefinitions')),
    (CRDS, Resource('apiextensions.k8s.io', 'v1', 'customresourcedefinitions')),
    (CRDS, Resource('apiextensions.k8s.io', 'vX', 'customresourcedefinitions')),
    (EVENTS, Resource('', 'v1', 'events')),
    (NAMESPACES, Resource('', 'v1', 'namespaces')),
    (CLUSTER_PEERINGS, Resource('kopf.dev', 'v1', 'clusterkopfpeerings')),
    (NAMESPACED_PEERINGS, Resource('kopf.dev', 'v1', 'kopfpeerings')),
    (CLUSTER_PEERINGS, Resource('zalando.org', 'v1', 'clusterkopfpeerings')),
    (NAMESPACED_PEERINGS, Resource('zalando.org', 'v1', 'kopfpeerings')),
])
async def test_refill_populates_the_resources(selector: Selector, resource: Resource):
    backbone = Backbone()
    await backbone.fill(resources=[resource])
    assert len(backbone) == 1
    assert set(backbone) == {selector}
    assert backbone[selector] == resource


async def test_refill_is_cumulative_ie_does_not_reset():
    backbone = Backbone()
    await backbone.fill(resources=[Resource('', 'v1', 'namespaces')])
    await backbone.fill(resources=[Resource('', 'v1', 'events')])
    assert len(backbone) == 2
    assert set(backbone) == {NAMESPACES, EVENTS}


async def test_waiting_for_absent_resources_never_ends(timer):
    backbone = Backbone()
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.1) as timeout:
            await backbone.wait_for(NAMESPACES)
    assert timeout.expired


async def test_waiting_for_preexisting_resources_ends_instantly(timer):
    resource = Resource('', 'v1', 'namespaces')
    backbone = Backbone()
    await backbone.fill(resources=[resource])
    async with timer, async_timeout.timeout(1):
        found_resource = await backbone.wait_for(NAMESPACES)
    assert timer.seconds < 0.1
    assert found_resource == resource


async def test_waiting_for_delayed_resources_ends_once_delivered(timer):
    resource = Resource('', 'v1', 'namespaces')
    backbone = Backbone()

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await backbone.fill(resources=[resource])

    task = asyncio.create_task(delayed_injection(0.1))
    async with timer, async_timeout.timeout(1):
        found_resource = await backbone.wait_for(NAMESPACES)
    await task
    assert 0.1 < timer.seconds < 0.11
    assert found_resource == resource
