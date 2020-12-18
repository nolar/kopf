import pytest


@pytest.fixture(autouse=True)
def _autouse_resp_mocker(resp_mocker, version_api):
    pass


@pytest.fixture(params=[
    pytest.param('something', id='namespace'),
    pytest.param(None, id='cluster'),
])
def namespace(request):
    return request.param
