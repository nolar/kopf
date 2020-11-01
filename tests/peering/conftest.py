import pytest

from kopf.structs.references import Resource

DEFAULTS = dict(
    kind='...', singular='...', namespaced=True, preferred=True,
    shortcuts=[], categories=[], subresources=[],
    verbs=['list', 'watch', 'patch'],
)

NAMESPACED_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'kopfpeerings', **DEFAULTS)
CLUSTER_PEERING_RESOURCE = Resource('zalando.org', 'v1', 'clusterkopfpeerings', **DEFAULTS)


@pytest.fixture(autouse=True)
def _autouse_fake_vault(fake_vault):
    pass


@pytest.fixture()
def with_cluster_crd(hostname, aresponses):
    result = {'resources': [{
        **DEFAULTS,
        'group': CLUSTER_PEERING_RESOURCE.group,
        'version': CLUSTER_PEERING_RESOURCE.version,
        'name': CLUSTER_PEERING_RESOURCE.plural,
        'namespaced': False,
    }]}
    url = CLUSTER_PEERING_RESOURCE.get_version_url()
    aresponses.add(hostname, url, 'get', result)


@pytest.fixture()
def with_namespaced_crd(hostname, aresponses):
    result = {'resources': [{
        **DEFAULTS,
        'group': NAMESPACED_PEERING_RESOURCE.group,
        'version': NAMESPACED_PEERING_RESOURCE.version,
        'name': NAMESPACED_PEERING_RESOURCE.plural,
        'namespaced': True,
    }]}
    url = NAMESPACED_PEERING_RESOURCE.get_version_url()
    aresponses.add(hostname, url, 'get', result)


@pytest.fixture()
def with_both_crds(hostname, aresponses):
    result = {'resources': [{
        **DEFAULTS,
        'group': CLUSTER_PEERING_RESOURCE.group,
        'version': CLUSTER_PEERING_RESOURCE.version,
        'name': CLUSTER_PEERING_RESOURCE.plural,
        'namespaced': False,
    }, {
        **DEFAULTS,
        'group': NAMESPACED_PEERING_RESOURCE.group,
        'version': NAMESPACED_PEERING_RESOURCE.version,
        'name': NAMESPACED_PEERING_RESOURCE.plural,
        'namespaced': True,
    }]}
    urls = {
        CLUSTER_PEERING_RESOURCE.get_version_url(),
        NAMESPACED_PEERING_RESOURCE.get_version_url(),
    }
    for url in urls:
        aresponses.add(hostname, url, 'get', result)
