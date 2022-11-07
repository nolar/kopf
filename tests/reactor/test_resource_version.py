import pytest

from kopf._core.reactor.queueing import get_version, EOS


@pytest.mark.parametrize('raw_event, expected_version', [
    pytest.param(EOS, None, id='eos'),
    pytest.param({}, None, id='empty-event'),
    pytest.param({'object': {}}, None, id='empty-object'),
    pytest.param({'object': {'metadata': {}}}, None, id='empty-metadata'),
    pytest.param({'object': {'metadata': {'resourceVersion': '123abc'}}}, '123abc', id='123abc'),
])
def test_resource_version_detection(raw_event, expected_version):
    resource_version = get_version(raw_event)
    assert resource_version == expected_version
