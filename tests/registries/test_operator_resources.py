import collections
from unittest.mock import Mock

import pytest

from kopf.reactor.registries import ResourceChangingRegistry, ResourceSpawningRegistry, \
                                    ResourceWatchingRegistry
from kopf.structs.references import Resource, Selector


@pytest.mark.parametrize('registry_cls', [
    ResourceWatchingRegistry,
    ResourceSpawningRegistry,
    ResourceChangingRegistry,
])
def test_resources(registry_cls):
    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group2', 'version2', 'plural2')
    selector1 = Selector('group1', 'version1', 'plural1')
    selector2 = Selector('group2', 'version2', 'plural2')
    selector3 = Selector('group1', 'version1', 'plural1')  # the same as the preceding ones

    registry = registry_cls()
    registry.append(Mock(selector=selector1))
    registry.append(Mock(selector=selector2))
    registry.append(Mock(selector=selector3))

    resources = registry.resources

    assert isinstance(resources, collections.abc.Collection)
    assert len(resources) == 2

    assert resource1 in resources
    assert resource2 in resources
