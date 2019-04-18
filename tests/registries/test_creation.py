from kopf import BaseRegistry, SimpleRegistry, GlobalRegistry


def test_creation_of_simple_no_prefix():
    registry = SimpleRegistry()
    assert isinstance(registry, BaseRegistry)
    assert isinstance(registry, SimpleRegistry)
    assert registry.prefix is None


def test_creation_of_simple_with_prefix_argument():
    registry = SimpleRegistry('hello')
    assert registry.prefix == 'hello'


def test_creation_of_simple_with_prefix_keyword():
    registry = SimpleRegistry(prefix='hello')
    assert registry.prefix == 'hello'


def test_creation_of_global():
    registry = GlobalRegistry()
    assert isinstance(registry, BaseRegistry)
    assert isinstance(registry, GlobalRegistry)
