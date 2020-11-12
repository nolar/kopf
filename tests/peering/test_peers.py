import datetime

import freezegun

from kopf.engines.peering import Peer


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_defaults():
    peer = Peer(identity='id')
    assert peer.identity == 'id'
    assert peer.lifetime == datetime.timedelta(seconds=60)
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_repr():
    peer = Peer(identity='some-id')
    text = repr(peer)
    assert text == "<Peer some-id: priority=0, lifetime=60, lastseen='2020-12-31T23:59:59.123456'>"


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_priority_specified():
    peer = Peer(identity='id', priority=123)
    assert peer.priority == 123


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_priority_unspecified():
    peer = Peer(identity='id')
    assert peer.priority == 0


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lifetime_as_number():
    peer = Peer(identity='id', lifetime=123)
    assert peer.lifetime == datetime.timedelta(seconds=123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lifetime_unspecified():
    peer = Peer(identity='id')
    assert peer.lifetime == datetime.timedelta(seconds=60)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lastseen_as_string():
    peer = Peer(identity='id', lastseen='2020-01-01T12:34:56.789123')
    assert peer.lastseen == datetime.datetime(2020, 1, 1, 12, 34, 56, 789123)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_with_lastseen_unspecified():
    peer = Peer(identity='id')
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)


@freezegun.freeze_time('2020-12-31T23:59:59.123456')
def test_creation_as_alive():
    peer = Peer(
        identity='id',
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
        identity='id',
        lifetime=10,
        lastseen='2020-12-31T23:59:49.123456',  # 10 seconds before "now" sharp
    )
    assert peer.lifetime == datetime.timedelta(seconds=10)
    assert peer.lastseen == datetime.datetime(2020, 12, 31, 23, 59, 49, 123456)
    assert peer.deadline == datetime.datetime(2020, 12, 31, 23, 59, 59, 123456)
    assert peer.is_dead is True
