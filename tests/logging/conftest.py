import pytest


@pytest.fixture(autouse=True)
def _caplog_all_levels(caplog):
    caplog.set_level(0)
