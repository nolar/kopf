import kopf


def test_getting_default_registry():
    registry = kopf.get_default_registry()
    assert isinstance(registry, kopf.OperatorRegistry)


def test_setting_default_registry():
    registry_expected = kopf.OperatorRegistry()
    kopf.set_default_registry(registry_expected)
    registry_actual = kopf.get_default_registry()
    assert registry_actual is registry_expected
