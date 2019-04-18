import pytest

import kopf


@pytest.fixture(autouse=True)
def clear_default_registry():
    registry = kopf.get_default_registry()
    kopf.set_default_registry(kopf.GlobalRegistry())
    try:
        yield
    finally:
        kopf.set_default_registry(registry)

