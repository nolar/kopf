import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, \
                                 NAMESPACED_PEERING_RESOURCE, guess_resource


@pytest.mark.parametrize('namespace, expected_resource', [
    (None, CLUSTER_PEERING_RESOURCE),
    ('ns', NAMESPACED_PEERING_RESOURCE),
])
def test_resource(namespace, expected_resource):
    resource = guess_resource(namespace=namespace)
    assert resource == expected_resource
