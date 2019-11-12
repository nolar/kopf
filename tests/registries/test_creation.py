from kopf import ActivityRegistry, ResourceRegistry, OperatorRegistry


def test_activity_registry_with_no_prefix(activity_registry_cls):
    registry = activity_registry_cls()
    assert isinstance(registry, ActivityRegistry)
    assert registry.prefix is None


def test_resource_registry_with_no_prefix(resource_registry_cls):
    registry = resource_registry_cls()
    assert isinstance(registry, ResourceRegistry)
    assert registry.prefix is None


def test_resource_registry_with_prefix_argument(resource_registry_cls):
    registry = resource_registry_cls('hello')
    assert registry.prefix == 'hello'


def test_resource_registry_with_prefix_keyword(resource_registry_cls):
    registry = resource_registry_cls(prefix='hello')
    assert registry.prefix == 'hello'


def test_operator_registry(operator_registry_cls):
    registry = operator_registry_cls()
    assert isinstance(registry, OperatorRegistry)
    assert not isinstance(registry, ResourceRegistry)
