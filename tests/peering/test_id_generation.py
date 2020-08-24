import os

import freezegun
import pytest

from kopf.engines.peering import detect_own_id


@pytest.fixture(autouse=True)
def _intercept_os_calls(mocker):
    mocker.patch('getpass.getuser', return_value='some-user')
    mocker.patch('socket.getfqdn', return_value='some-host')
    mocker.patch('random.choices', return_value='random-str')


def test_from_a_pod_id(mocker):
    mocker.patch.dict(os.environ, POD_ID='some-pod-1')
    own_id = detect_own_id()
    assert own_id == 'some-pod-1'


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_with_defaults():
    own_id = detect_own_id()
    assert own_id == 'some-user@some-host/2020-12-31T23:59:59.123456/random-str'
