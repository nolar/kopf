from kopf import ResourceRegistry, OperatorRegistry


def test_creation_of_simple_no_prefix():
    registry = ResourceRegistry()
    assert isinstance(registry, ResourceRegistry)
    assert registry.prefix is None


def test_creation_of_simple_with_prefix_argument():
    registry = ResourceRegistry('hello')
    assert registry.prefix == 'hello'


def test_creation_of_simple_with_prefix_keyword():
    registry = ResourceRegistry(prefix='hello')
    assert registry.prefix == 'hello'


def test_creation_of_global():
    registry = OperatorRegistry()
    assert isinstance(registry, OperatorRegistry)
