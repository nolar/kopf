import collections
from unittest.mock import Mock

from kopf.reactor.registries import OperatorRegistry
from kopf.structs.resources import Resource


def test_resources():
    handler = Mock()

    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group2', 'version2', 'plural2')

    registry = OperatorRegistry()
    registry.resource_watching_handlers[resource1].append(handler)
    registry.resource_changing_handlers[resource2].append(handler)
    registry.resource_watching_handlers[resource2].append(handler)
    registry.resource_changing_handlers[resource1].append(handler)

    resources = registry.resources

    assert isinstance(resources, collections.abc.Collection)
    assert len(resources) == 2

    assert resource1 in resources
    assert resource2 in resources
