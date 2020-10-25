import collections
from unittest.mock import Mock

from kopf.reactor.registries import OperatorRegistry
from kopf.structs.references import Resource, Selector


def test_resources():
    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group2', 'version2', 'plural2')
    selector1 = Selector('group1', 'version1', 'plural1')
    selector2 = Selector('group2', 'version2', 'plural2')
    handler1 = Mock(selector=selector1)
    handler2 = Mock(selector=selector2)

    registry = OperatorRegistry()
    registry.resource_watching_handlers.append(handler1)
    registry.resource_changing_handlers.append(handler2)
    registry.resource_watching_handlers.append(handler2)
    registry.resource_changing_handlers.append(handler1)

    resources = registry.resources

    assert isinstance(resources, collections.abc.Collection)
    assert len(resources) == 2

    assert resource1 in resources
    assert resource2 in resources
