from kopf import BaseRegistry, SimpleRegistry, GlobalRegistry


def test_creation_of_simple():
    registry = SimpleRegistry()
    assert isinstance(registry, BaseRegistry)
    assert isinstance(registry, SimpleRegistry)


def test_creation_of_global():
    registry = GlobalRegistry()
    assert isinstance(registry, BaseRegistry)
    assert isinstance(registry, GlobalRegistry)
