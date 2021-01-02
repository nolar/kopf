import re

import pytest

from kopf.engines.peering import detect
from kopf.structs.references import Resource

NAMESPACED_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'kopfpeerings')
CLUSTER_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'clusterkopfpeerings')


@pytest.fixture()
def with_cluster_cr(hostname, aresponses):
    url = CLUSTER_PEERING_RESOURCE.get_url(namespace=None, name='existent')
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.fixture()
def with_namespaced_cr(hostname, aresponses):
    url = NAMESPACED_PEERING_RESOURCE.get_url(namespace='namespace', name='existent')
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.mark.usefixtures('with_cluster_cr')
@pytest.mark.usefixtures('with_cluster_crd')
@pytest.mark.parametrize('mandatory', [False, True], ids=['optional', 'mandatory'])
async def test_cluster_scoped_when_existent(mandatory, settings):
    settings.peering.mandatory = mandatory
    settings.peering.name = 'existent'
    peering = await detect(settings=settings, namespace=None, resource=CLUSTER_PEERING_RESOURCE)
    assert peering is True


@pytest.mark.usefixtures('with_namespaced_cr')
@pytest.mark.usefixtures('with_namespaced_crd')
@pytest.mark.parametrize('mandatory', [False, True], ids=['optional', 'mandatory'])
async def test_namespace_scoped_when_existent(mandatory, settings):
    settings.peering.mandatory = mandatory
    settings.peering.name = 'existent'
    peering = await detect(settings=settings, namespace='namespace', resource=NAMESPACED_PEERING_RESOURCE)
    assert peering is True


@pytest.mark.usefixtures('with_cluster_crd')
async def test_cluster_scoped_when_absent(hostname, aresponses, settings):
    settings.peering.mandatory = True
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The mandatory peering 'absent' was not found") as e:
        await detect(settings=settings, namespace=None, resource=CLUSTER_PEERING_RESOURCE)


@pytest.mark.usefixtures('with_namespaced_crd')
async def test_namespace_scoped_when_absent(hostname, aresponses, settings):
    settings.peering.mandatory = True
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The mandatory peering 'absent' was not found") as e:
        await detect(settings=settings, namespace='namespace', resource=NAMESPACED_PEERING_RESOURCE)


@pytest.mark.usefixtures('with_cluster_crd')
async def test_fallback_with_cluster_scoped(hostname, aresponses, assert_logs, caplog, settings):
    settings.peering.mandatory = False
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peering = await detect(settings=settings, namespace=None, resource=CLUSTER_PEERING_RESOURCE)
    assert peering is False
    assert_logs([
        "Default peering object is not found, falling back to the standalone mode."
    ])


@pytest.mark.usefixtures('with_namespaced_crd')
async def test_fallback_with_namespace_scoped(hostname, aresponses, assert_logs, caplog, settings):
    settings.peering.mandatory = False
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peering = await detect(settings=settings, namespace='namespace', resource=NAMESPACED_PEERING_RESOURCE)
    assert peering is False
    assert_logs([
        "Default peering object is not found, falling back to the standalone mode."
    ])
