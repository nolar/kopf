import pytest


@pytest.fixture(autouse=True)
def _enforced_api_server(fake_vault, enforced_session, resource):
    pass


@pytest.fixture(autouse=True)
def _prevent_retries_in_api_tests(settings):
    settings.networking.error_backoffs = []
