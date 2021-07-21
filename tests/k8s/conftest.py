import pytest


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker, version_api):
    pass


@pytest.fixture(autouse=True)
def _prevent_retries_in_api_tests(settings):
    settings.networking.error_backoffs = []
