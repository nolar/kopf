import datetime

import freezegun
import pytest

from kopf.engines.peering import CLUSTER_PEERING_RESOURCE, LEGACY_PEERING_RESOURCE, \
                                 NAMESPACED_PEERING_RESOURCE, Peer


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_defaults():
    peer = Peer(id='id', name='name')
    assert peer.id == 'id'
    assert peer.name == 'name'
    assert peer.namespace is None
    assert peer.legacy is False
    assert peer.lifetime == datetime.timedelta(seconds=60)
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_repr():
    peer = Peer(id='some-id', name='some-name', namespace='some-namespace')
    text = repr(peer)
    assert text.startswith('Peer(')
    assert text.endswith(')')
    assert '(some-id, ' in text
    assert 'priority=0' in text
    assert 'lastseen=' in text
    assert 'lifetime=' in text

    # The peering object's name is of no interest, the peer's id is.
    assert 'name=' not in text

    # The namespace of the operator can affect the conflict detection.
    # It is not always the same as the peering object's namespace.
    assert 'namespace=some-namespace' in text


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_priority_specified():
    peer = Peer(id='id', name='name', priority=123)
    assert peer.priority == 123


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_priority_unspecified():
    peer = Peer(id='id', name='name')
    assert peer.priority == 0


@pytest.mark.parametrize('namespace', [None, 'namespaced'])
def test_resource_for_legacy_peering(namespace):
    peer = Peer(id='id', name='name', legacy=True, namespace=namespace)
    assert peer.legacy is True
    assert peer.resource == LEGACY_PEERING_RESOURCE


def test_resource_for_cluster_peering():
    peer = Peer(id='id', name='name', legacy=False, namespace=None)
    assert peer.legacy is False
    assert peer.resource == CLUSTER_PEERING_RESOURCE
    assert peer.namespace is None


def test_resource_for_namespaced_peering():
    peer = Peer(id='id', name='name', legacy=False, namespace='namespaced')
    assert peer.legacy is False
    assert peer.resource == NAMESPACED_PEERING_RESOURCE
    assert peer.namespace == 'namespaced'


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lifetime_as_timedelta():
    peer = Peer(id='id', name='name', lifetime=datetime.timedelta(seconds=123))
    assert peer.lifetime == datetime.timedelta(seconds=123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lifetime_as_number():
    peer = Peer(id='id', name='name', lifetime=123)
    assert peer.lifetime == datetime.timedelta(seconds=123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lifetime_unspecified():
    peer = Peer(id='id', name='name')
    assert peer.lifetime == datetime.timedelta(seconds=60)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lastseen_as_datetime():
    peer = Peer(id='id', name='name', lastseen=datetime.datetime(2020, 1, 1, 12, 34, 56, 789123))
    assert peer.lastseen == datetime.datetime(2020, 1, 1, 12, 34, 56, 789123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lastseen_as_string():
    peer = Peer(id='id', name='name', lastseen='2020-01-01T12:34:56.789123')
    assert peer.lastseen == datetime.datetime(2020, 1, 1, 12, 34, 56, 789123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lastseen_unspecified():
    peer = Peer(id='id', name='name')
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_as_alive():
    peer = Peer(
        id='id',
        name='name',
        lifetime=10,
        lastseen='2020-12-31T23:59:50.123456',  # less than 10 seconds before "now"
    )
    assert peer.lifetime == datetime.timedelta(seconds=10)
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 50, 123456)
    assert peer.deadline == datetime.datetime(2021, 1, 1, 0, 0, 0, 123456)
    assert peer.is_dead is False


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_as_dead():
    peer = Peer(
        id='id',
        name='name',
        lifetime=10,
        lastseen='2020-12-31T23:59:49.123456',  # 10 seconds before "now" sharp
    )
    assert peer.lifetime == datetime.timedelta(seconds=10)
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 49, 123456)
    assert peer.deadline == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)
    assert peer.is_dead is True


def test_touching_when_alive():
    with freezegun.freeze_time('2020-01-01T10:20:30'):
        peer = Peer(id='id1', name='name1', lifetime=123)

    assert not peer.is_dead

    with freezegun.freeze_time('2020-02-02T11:22:33'):
        peer.touch()

    assert peer.lifetime == datetime.timedelta(seconds=123)
    assert peer.lastseen == datetime.datetime(2020, 2, 2, 11, 22, 33)
    assert peer.deadline == datetime.datetime(2020, 2, 2, 11, 24, 36)
    assert not peer.is_dead


def test_touching_when_dead():
    with freezegun.freeze_time('2020-01-01T10:20:30'):
        peer = Peer(id='id1', name='name1', lifetime=123, lastseen='2019-01-01T00:00:00')

    assert peer.is_dead

    with freezegun.freeze_time('2020-02-02T11:22:33'):
        peer.touch()

    assert peer.lifetime == datetime.timedelta(seconds=123)
    assert peer.lastseen == datetime.datetime(2020, 2, 2, 11, 22, 33)
    assert peer.deadline == datetime.datetime(2020, 2, 2, 11, 24, 36)
    assert not peer.is_dead
