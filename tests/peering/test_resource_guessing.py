import pytest

from kopf.engines.peering import guess_selector
from kopf.structs.references import CLUSTER_PEERINGS, NAMESPACED_PEERINGS


@pytest.mark.parametrize('namespaced, expected_selector', [
    (False, CLUSTER_PEERINGS),
    (True, NAMESPACED_PEERINGS),
])
def test_guessing(settings, namespaced, expected_selector):
    settings.peering.namespaced = namespaced
    selector = guess_selector(settings=settings)
    assert selector == expected_selector
