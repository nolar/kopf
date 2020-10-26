import re

import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, \
                                 NAMESPACED_PEERING_RESOURCE, detect_presence


@pytest.fixture()
def with_cluster_cr(hostname, aresponses):
    url = CLUSTER_PEERING_RESOURCE.get_url(namespace=None, name='existent')
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.fixture()
def with_namespaced_cr(hostname, aresponses):
    url = NAMESPACED_PEERING_RESOURCE.get_url(namespace='namespace', name='existent')
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.mark.usefixtures('with_namespaced_cr')
@pytest.mark.usefixtures('with_cluster_cr')
@pytest.mark.usefixtures('with_both_crds')
@pytest.mark.parametrize('name', ['existent', 'absent'])
@pytest.mark.parametrize('namespace', [None, 'namespace'], ids=['cluster', 'namespaced'])
@pytest.mark.parametrize('mandatory', [False, True], ids=['optional', 'mandatory'])
async def test_standalone(mandatory, namespace, name, settings):
    settings.peering.standalone = True
    settings.peering.mandatory = mandatory
    settings.peering.name = name
    peering = await detect_presence(settings=settings, namespace=namespace)
    assert peering is None


@pytest.mark.usefixtures('with_cluster_cr')
@pytest.mark.usefixtures('with_cluster_crd')
@pytest.mark.parametrize('mandatory', [False, True], ids=['optional', 'mandatory'])
async def test_cluster_scoped_when_existent(mandatory, settings):
    settings.peering.mandatory = mandatory
    settings.peering.name = 'existent'
    peering = await detect_presence(settings=settings, namespace=None)
    assert peering is True


@pytest.mark.usefixtures('with_namespaced_cr')
@pytest.mark.usefixtures('with_namespaced_crd')
@pytest.mark.parametrize('mandatory', [False, True], ids=['optional', 'mandatory'])
async def test_namespace_scoped_when_existent(mandatory, settings):
    settings.peering.mandatory = mandatory
    settings.peering.name = 'existent'
    peering = await detect_presence(settings=settings, namespace='namespace')
    assert peering is True


@pytest.mark.usefixtures('with_cluster_crd')
async def test_cluster_scoped_when_absent(hostname, aresponses, settings):
    settings.peering.mandatory = True
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The mandatory peering 'absent' was not found") as e:
        await detect_presence(settings=settings, namespace=None)


@pytest.mark.usefixtures('with_namespaced_crd')
async def test_namespace_scoped_when_absent(hostname, aresponses, settings):
    settings.peering.mandatory = True
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The mandatory peering 'absent' was not found") as e:
        await detect_presence(settings=settings, namespace='namespace')


@pytest.mark.usefixtures('with_cluster_crd')
async def test_fallback_with_cluster_scoped(hostname, aresponses, assert_logs, caplog, settings):
    settings.peering.mandatory = False
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peering = await detect_presence(settings=settings, namespace=None)
    assert peering is False
    assert_logs([
        "Default peering object not found, falling back to the standalone mode."
    ])


@pytest.mark.usefixtures('with_namespaced_crd')
async def test_fallback_with_namespace_scoped(hostname, aresponses, assert_logs, caplog, settings):
    settings.peering.mandatory = False
    settings.peering.name = 'absent'
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peering = await detect_presence(settings=settings, namespace='namespace')
    assert peering is False
    assert_logs([
        "Default peering object not found, falling back to the standalone mode."
    ])
