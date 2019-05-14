import pytest

import kopf


@pytest.fixture(autouse=True)
def clear_default_lifecycle():
    try:
        yield
    finally:
        kopf.set_default_lifecycle(None)
