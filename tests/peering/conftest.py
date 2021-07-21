import pytest


@pytest.fixture(autouse=True)
def _autouse_fake_vault(fake_vault):
    pass
