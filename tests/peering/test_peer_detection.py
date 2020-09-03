import re

import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, NAMESPACED_PEERING_RESOURCE, \
                                 PEERING_DEFAULT_NAME, Peer

# Note: the legacy peering is intentionally not tested: it was long time before
# these tests were written, so it does not make sense to keep it stable.
# The legacy peering is going to be removed in version 1.0 when it happens.


@pytest.fixture()
def with_cluster_default(hostname, aresponses):
    url = CLUSTER_PEERING_RESOURCE.get_url(name=PEERING_DEFAULT_NAME)
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.fixture()
def with_cluster_specific(hostname, aresponses):
    url = CLUSTER_PEERING_RESOURCE.get_url(name='peering-name')
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.fixture()
def with_namespaced_default(hostname, aresponses):
    url = NAMESPACED_PEERING_RESOURCE.get_url(namespace='namespace', name=PEERING_DEFAULT_NAME)
    aresponses.add(hostname, url, 'get', {'spec': {}})


@pytest.fixture()
def with_namespaced_specific(hostname, aresponses):
    url = NAMESPACED_PEERING_RESOURCE.get_url(namespace='namespace', name='peering-name')
    aresponses.add(hostname, url, 'get', {'spec': {}})


#
# Parametrization via fixtures (it does not work from tests).
#
@pytest.fixture(params=[
    pytest.param(None, id='no-crds'),
    pytest.param('with_both_crds', id='both-crds'),
    pytest.param('with_cluster_crd', id='only-cluster-crd'),
    pytest.param('with_namespaced_crd', id='only-namespaced-crd'),
])
def all_crd_modes(request):
    return request.getfixturevalue(request.param) if request.param else None


@pytest.fixture(params=[
    pytest.param('with_both_crds', id='both-crds'),
    pytest.param('with_cluster_crd', id='only-cluster-crd'),
])
def all_crd_modes_with_cluster_scoped_crd(request):
    return request.getfixturevalue(request.param) if request.param else None


@pytest.fixture(params=[
    pytest.param('with_both_crds', id='both-crds'),
    pytest.param('with_namespaced_crd', id='only-namespaced-crd'),
])
def all_crd_modes_with_namespace_scoped_crd(request):
    return request.getfixturevalue(request.param) if request.param else None


@pytest.fixture(params=[
    pytest.param(True, id='with-cluster-default'),
    pytest.param(False, id='without-cluster-default')
])
def both_cluster_default_modes(request):
    return request.getfixturevalue('with_cluster_default') if request.param else None


@pytest.fixture(params=[
    pytest.param(True, id='with-cluster-specific'),
    pytest.param(False, id='without-cluster-specific')
])
def both_cluster_specific_modes(request):
    return request.getfixturevalue('with_cluster_specific') if request.param else None


@pytest.fixture(params=[
    pytest.param(True, id='with-namespaced-default'),
    pytest.param(False, id='without-namespaced-default')
])
def both_namespaced_default_modes(request):
    return request.getfixturevalue('with_namespaced_default') if request.param else None


@pytest.fixture(params=[
    pytest.param(True, id='with-namespaced-specific'),
    pytest.param(False, id='without-namespaced-specific')
])
def both_namespaced_specific_modes(request):
    return request.getfixturevalue('with_namespaced_specific') if request.param else None


#
# Actual tests: only the action & assertions.
#
@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes')
@pytest.mark.parametrize('name', [None, 'name'])
@pytest.mark.parametrize('namespace', [None, 'namespaced'])
async def test_standalone(namespace, name):
    peer = await Peer.detect(standalone=True, namespace=namespace, name=name)
    assert peer is None


@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('with_cluster_default')
@pytest.mark.usefixtures('all_crd_modes_with_cluster_scoped_crd')
async def test_cluster_scoped_with_default_name():
    peer = await Peer.detect(id='id', standalone=False, namespace=None, name=None)
    assert peer.name == PEERING_DEFAULT_NAME
    assert peer.namespace is None


@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('with_namespaced_default')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_namespace_scoped_crd')
async def test_namespace_scoped_with_default_name():
    peer = await Peer.detect(id='id', standalone=False, namespace='namespace', name=None)
    assert peer.name == PEERING_DEFAULT_NAME
    assert peer.namespace == 'namespace'


@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('with_cluster_specific')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_cluster_scoped_crd')
async def test_cluster_scoped_with_specific_name():
    peer = await Peer.detect(id='id', standalone=False, namespace=None, name='peering-name')
    assert peer.name == 'peering-name'
    assert peer.namespace is None


@pytest.mark.usefixtures('with_namespaced_specific')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_namespace_scoped_crd')
async def test_namespace_scoped_with_specific_name():
    peer = await Peer.detect(id='id', standalone=False, namespace='namespace', name='peering-name')
    assert peer.name == 'peering-name'
    assert peer.namespace == 'namespace'


@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_cluster_scoped_crd')
async def test_cluster_scoped_with_absent_name(hostname, aresponses):
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The peering 'absent-name' was not found") as e:
        await Peer.detect(id='id', standalone=False, namespace=None, name='absent-name')


@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_namespace_scoped_crd')
async def test_namespace_scoped_with_absent_name(hostname, aresponses):
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    with pytest.raises(Exception, match=r"The peering 'absent-name' was not found") as e:
        await Peer.detect(id='id', standalone=False, namespace='namespace', name='absent-name')


# NB: without cluster-default peering.
@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_namespaced_default_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('all_crd_modes_with_cluster_scoped_crd')
async def test_cluster_scoped_with_warning(hostname, aresponses, assert_logs, caplog):
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peer = await Peer.detect(id='id', standalone=False, namespace=None, name=None)
    assert peer is None
    assert_logs([
        "Default peering object not found, falling back to the standalone mode."
    ])


# NB: without namespaced-default peering.
@pytest.mark.usefixtures('both_namespaced_specific_modes')
@pytest.mark.usefixtures('both_cluster_specific_modes')
@pytest.mark.usefixtures('both_cluster_default_modes')
@pytest.mark.usefixtures('all_crd_modes_with_namespace_scoped_crd')
async def test_namespace_scoped_with_warning(hostname, aresponses, assert_logs, caplog):
    aresponses.add(hostname, re.compile(r'.*'), 'get', aresponses.Response(status=404), repeat=999)
    peer = await Peer.detect(id='id', standalone=False, namespace='namespace', name=None)
    assert peer is None
    assert_logs([
        "Default peering object not found, falling back to the standalone mode."
    ])
