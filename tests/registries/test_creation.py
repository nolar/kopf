from kopf.reactor.registries import ActivityRegistry, OperatorRegistry, ResourceRegistry


def test_activity_registry(activity_registry_cls):
    registry = activity_registry_cls()
    assert isinstance(registry, ActivityRegistry)


def test_resource_registry(resource_registry_cls):
    registry = resource_registry_cls()
    assert isinstance(registry, ResourceRegistry)


def test_operator_registry(operator_registry_cls):
    registry = operator_registry_cls()
    assert isinstance(registry, OperatorRegistry)
    assert not isinstance(registry, ResourceRegistry)
