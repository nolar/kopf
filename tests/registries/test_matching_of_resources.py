from unittest.mock import Mock

from kopf.reactor.registries import _matches_resource
from kopf.structs.resources import Resource


def test_different_resource():
    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group2', 'version2', 'plural2')
    handler1 = Mock(resource=resource1)
    matches = _matches_resource(handler1, resource2)
    assert not matches


def test_equivalent_resources():
    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group1', 'version1', 'plural1')
    handler1 = Mock(resource=resource1)
    matches = _matches_resource(handler1, resource2)
    assert matches


def test_catchall_with_none():
    resource2 = Resource('group2', 'version2', 'plural2')
    handler1 = Mock(resource=None)
    matches = _matches_resource(handler1, resource2)
    assert matches
