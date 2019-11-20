import pytest


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker, version_api):
    pass
