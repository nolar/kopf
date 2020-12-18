import os

import freezegun
import pytest

from kopf.engines.peering import detect_own_id

SAME_GOOD = [
    ('some-host.example.com', 'other-host.example.com'),
    ('some-host', 'other-host'),
]

# Priorities: A (left) should be selected over B (right).
GOOD_BAD = [
    ('some-host.example.com', 'some-host'),
    ('some-host.example.com', '1.2.3.4'),
    ('some-host.example.com', '::1'),
    ('some-host.example.com', '1.0...0.ip6.arpa'),
    ('some-host.example.com', '4.3.2.1.in-addr.arpa'),
    ('some-host.example.com', '1.2.3.4'),
    ('some-host', '1.2.3.4'),
    ('some-host', '::1'),
    ('some-host', '1.0...0.ip6.arpa'),
    ('some-host', '4.3.2.1.in-addr.arpa'),
    ('some-host', '1.2.3.4'),
]


@pytest.fixture(autouse=True)
def _intercept_os_calls(mocker):
    mocker.patch('getpass.getuser', return_value='some-user')
    mocker.patch('socket.gethostname')
    mocker.patch('socket.gethostbyaddr')


@pytest.mark.parametrize('manual', [True, False])
def test_from_a_pod_id(mocker, manual):
    mocker.patch('socket.gethostname', return_value='some-host')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [], []))
    mocker.patch.dict(os.environ, POD_ID='some-pod-1')
    own_id = detect_own_id(manual=manual)
    assert own_id == 'some-pod-1'


def test_suffixes_appended(mocker):
    mocker.patch('random.choices', return_value='random-str')
    mocker.patch('socket.gethostname', return_value='some-host')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [], []))
    with freezegun.freeze_time('2020-12-31T23:59:59.123456'):
        own_id = detect_own_id(manual=False)
    assert own_id == 'some-user@some-host/20201231235959/random-str'


def test_suffixes_ignored(mocker):
    mocker.patch('socket.gethostname', return_value='some-host')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [], []))
    own_id = detect_own_id(manual=True)
    assert own_id == 'some-user@some-host'


@pytest.mark.parametrize('good1, good2', SAME_GOOD)
def test_good_hostnames_over_good_aliases__symmetric(mocker, good1, good2):
    mocker.patch('socket.gethostname', return_value=good1)
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [good2], []))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good1}'

    mocker.patch('socket.gethostname', return_value=good2)
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [good1], []))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good2}'


@pytest.mark.parametrize('good1, good2', SAME_GOOD)
def test_good_aliases_over_good_addresses__symmetric(mocker, good1, good2):
    mocker.patch('socket.gethostname', return_value='localhost')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [good1], [good2]))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good1}'

    mocker.patch('socket.gethostname', return_value='localhost')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [good2], [good1]))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good2}'


@pytest.mark.parametrize('good, bad', GOOD_BAD)
def test_good_aliases_over_bad_hostnames(mocker, good, bad):
    mocker.patch('socket.gethostname', return_value=bad)
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [good], []))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good}'


@pytest.mark.parametrize('good, bad', GOOD_BAD)
def test_good_addresses_over_bad_aliases(mocker, good, bad):
    mocker.patch('socket.gethostname', return_value='localhost')
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [bad], [good]))
    own_id = detect_own_id(manual=True)
    assert own_id == f'some-user@{good}'


@pytest.mark.parametrize('fqdn', [
    'my-host',
    'my-host.local',
    'my-host.localdomain',
    'my-host.local.localdomain',
    'my-host.localdomain.local',
])
def test_useless_suffixes_removed(mocker, fqdn):
    mocker.patch('socket.gethostname', return_value=fqdn)
    mocker.patch('socket.gethostbyaddr', side_effect=lambda fqdn: (fqdn, [], []))
    own_id = detect_own_id(manual=True)
    assert own_id == 'some-user@my-host'
