import pytest


@pytest.fixture(autouse=True)
def _autouse_req_mock(req_mock):
    pass
