import pytest

from kopf._cogs.structs.references import CLUSTER_PEERINGS_K, CLUSTER_PEERINGS_Z, \
                                          NAMESPACED_PEERINGS_K, NAMESPACED_PEERINGS_Z
from kopf._core.engines.peering import guess_selectors


@pytest.mark.parametrize('namespaced, expected_selectors', [
    (False, [CLUSTER_PEERINGS_K, CLUSTER_PEERINGS_Z]),
    (True, [NAMESPACED_PEERINGS_K, NAMESPACED_PEERINGS_Z]),
])
@pytest.mark.parametrize('mandatory', [False, True])
def test_resource_when_not_standalone(settings, namespaced, mandatory, expected_selectors):
    settings.peering.standalone = False
    settings.peering.namespaced = namespaced
    settings.peering.mandatory = mandatory
    selectors = guess_selectors(settings=settings)
    assert selectors == expected_selectors


@pytest.mark.parametrize('namespaced', [False, True])
@pytest.mark.parametrize('mandatory', [False, True])
def test_resource_when_standalone(settings, namespaced, mandatory):
    settings.peering.standalone = True
    settings.peering.namespaced = namespaced
    settings.peering.mandatory = mandatory
    selectors = guess_selectors(settings=settings)
    assert not selectors
