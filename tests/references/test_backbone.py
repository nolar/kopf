import asyncio

import pytest

from kopf._cogs.structs.references import CLUSTER_PEERINGS_K, CLUSTER_PEERINGS_Z, CRDS, EVENTS, \
                                          NAMESPACED_PEERINGS_K, NAMESPACED_PEERINGS_Z, \
                                          NAMESPACES, Backbone, Resource, Selector


@pytest.mark.parametrize('selector', [
    CRDS, EVENTS, NAMESPACES,
    CLUSTER_PEERINGS_K, NAMESPACED_PEERINGS_K,
    CLUSTER_PEERINGS_Z, NAMESPACED_PEERINGS_Z,
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
    (CLUSTER_PEERINGS_K, Resource('kopf.dev', 'v1', 'clusterkopfpeerings')),
    (NAMESPACED_PEERINGS_K, Resource('kopf.dev', 'v1', 'kopfpeerings')),
    (CLUSTER_PEERINGS_Z, Resource('zalando.org', 'v1', 'clusterkopfpeerings')),
    (NAMESPACED_PEERINGS_Z, Resource('zalando.org', 'v1', 'kopfpeerings')),
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


async def test_waiting_for_absent_resources_never_ends(looptime):
    backbone = Backbone()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(backbone.wait_for(NAMESPACES), timeout=1.23)
    assert looptime == 1.23


async def test_waiting_for_preexisting_resources_ends_instantly(looptime):
    resource = Resource('', 'v1', 'namespaces')
    backbone = Backbone()
    await backbone.fill(resources=[resource])
    found_resource = await backbone.wait_for(NAMESPACES)
    assert looptime == 0
    assert found_resource == resource


async def test_waiting_for_delayed_resources_ends_once_delivered(looptime):
    resource = Resource('', 'v1', 'namespaces')
    backbone = Backbone()

    async def delayed_injection(delay: float):
        await asyncio.sleep(delay)
        await backbone.fill(resources=[resource])

    task = asyncio.create_task(delayed_injection(123))
    found_resource = await backbone.wait_for(NAMESPACES)
    await task
    assert looptime == 123
    assert found_resource == resource
