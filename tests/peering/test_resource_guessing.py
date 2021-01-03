import pytest

from kopf.engines.peering import guess_selector
from kopf.structs.references import CLUSTER_PEERINGS, NAMESPACED_PEERINGS


@pytest.mark.parametrize('namespaced, expected_resource', [
    (False, CLUSTER_PEERINGS),
    (True, NAMESPACED_PEERINGS),
])
@pytest.mark.parametrize('mandatory', [False, True])
def test_resource_when_not_standalone(settings, namespaced, mandatory, expected_resource):
    settings.peering.standalone = False
    settings.peering.namespaced = namespaced
    settings.peering.mandatory = mandatory
    selector = guess_selector(settings=settings)
    assert selector == expected_resource


@pytest.mark.parametrize('namespaced', [False, True])
@pytest.mark.parametrize('mandatory', [False, True])
def test_resource_when_standalone(settings, namespaced, mandatory):
    settings.peering.standalone = True
    settings.peering.namespaced = namespaced
    settings.peering.mandatory = mandatory
    selector = guess_selector(settings=settings)
    assert selector is None
