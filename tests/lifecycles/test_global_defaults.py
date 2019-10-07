import kopf


def test_getting_default_lifecycle():
    lifecycle = kopf.get_default_lifecycle()
    assert lifecycle is kopf.lifecycles.asap


def test_setting_default_lifecycle():
    lifecycle_expected = lambda handlers, *args, **kwargs: handlers
    kopf.set_default_lifecycle(lifecycle_expected)
    lifecycle_actual = kopf.get_default_lifecycle()
    assert lifecycle_actual is lifecycle_expected


def test_resetting_default_lifecycle():
    lifecycle_distracting = lambda handlers, *args, **kwargs: handlers
    kopf.set_default_lifecycle(lifecycle_distracting)
    kopf.set_default_lifecycle(None)
    lifecycle_actual = kopf.get_default_lifecycle()
    assert lifecycle_actual is kopf.lifecycles.asap
