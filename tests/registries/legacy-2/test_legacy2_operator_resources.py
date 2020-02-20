import collections

import pytest

from kopf.reactor.registries import OperatorRegistry
from kopf.structs.resources import Resource


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn():
    pass


def test_resources():
    registry = OperatorRegistry()

    with pytest.deprecated_call(match=r"use @kopf.on"):
        registry.register_resource_watching_handler('group1', 'version1', 'plural1', some_fn)
    with pytest.deprecated_call(match=r"use @kopf.on"):
        registry.register_resource_changing_handler('group2', 'version2', 'plural2', some_fn)
    with pytest.deprecated_call(match=r"use @kopf.on"):
        registry.register_resource_watching_handler('group2', 'version2', 'plural2', some_fn)
    with pytest.deprecated_call(match=r"use @kopf.on"):
        registry.register_resource_changing_handler('group1', 'version1', 'plural1', some_fn)

    resources = registry.resources

    assert isinstance(resources, collections.abc.Collection)
    assert len(resources) == 2

    resource1 = Resource('group1', 'version1', 'plural1')
    resource2 = Resource('group2', 'version2', 'plural2')
    assert resource1 in resources
    assert resource2 in resources
