from unittest.mock import Mock

from kopf.reactor.registries import _matches_resource
from kopf.structs.references import Resource, Selector


def test_different_resource():
    selector = Selector('group1', 'version1', 'plural1')
    resource = Resource('group2', 'version2', 'plural2')
    handler = Mock(selector=selector)
    matches = _matches_resource(handler, resource)
    assert not matches


def test_equivalent_resources():
    selector = Selector('group1', 'version1', 'plural1')
    resource = Resource('group1', 'version1', 'plural1')
    handler = Mock(selector=selector)
    matches = _matches_resource(handler, resource)
    assert matches


def test_catchall_with_none():
    resource = Resource('group2', 'version2', 'plural2')
    handler = Mock(selector=None)
    matches = _matches_resource(handler, resource)
    assert matches
