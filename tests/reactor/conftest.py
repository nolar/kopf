import pytest


@pytest.fixture()
def stream(mocker):
    stream = mocker.patch('kubernetes.watch.Watch.stream')
    return stream
