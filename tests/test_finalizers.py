import pytest

from kopf.storage.finalizers import FINALIZER, LEGACY_FINALIZER
from kopf.storage.finalizers import block_deletion, allow_deletion
from kopf.storage.finalizers import is_deletion_ongoing, is_deletion_blocked


def test_finalizer_is_fqdn():
    assert FINALIZER.startswith('kopf.zalando.org/')


@pytest.mark.parametrize('expected, body', [
    pytest.param(True, {'metadata': {'deletionTimestamp': '2020-12-31T23:59:59'}}, id='time'),
    pytest.param(False, {'metadata': {'deletionTimestamp': None}}, id='none'),
    pytest.param(False, {'metadata': {}}, id='no-field'),
    pytest.param(False, {}, id='no-metadata'),
])
def test_is_deleted(expected, body):
    result = is_deletion_ongoing(body=body)
    assert result == expected


@pytest.mark.parametrize('expected, body', [
    pytest.param(False, {}, id='no-metadata'),
    pytest.param(False, {'metadata': {}}, id='no-finalizers'),
    pytest.param(False, {'metadata': {'finalizers': []}}, id='empty'),
    pytest.param(False, {'metadata': {'finalizers': ['other']}}, id='others'),
    pytest.param(True, {'metadata': {'finalizers': [FINALIZER]}}, id='normal'),
    pytest.param(True, {'metadata': {'finalizers': [LEGACY_FINALIZER]}}, id='legacy'),
    pytest.param(True, {'metadata': {'finalizers': ['other', FINALIZER]}}, id='mixed'),
])
def test_has_finalizers(expected, body):
    result = is_deletion_blocked(body=body)
    assert result == expected


def test_append_finalizers_to_others():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    block_deletion(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': ['other1', 'other2', FINALIZER]}}


def test_append_finalizers_to_empty():
    body = {}
    patch = {}
    block_deletion(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': [FINALIZER]}}


def test_append_finalizers_when_present():
    body = {'metadata': {'finalizers': ['other1', FINALIZER, 'other2']}}
    patch = {}
    block_deletion(body=body, patch=patch)
    assert patch == {}


@pytest.mark.parametrize('finalizer', [
    pytest.param(LEGACY_FINALIZER, id='legacy'),
    pytest.param(FINALIZER, id='normal'),
])
def test_remove_finalizers_keeps_others(finalizer):
    body = {'metadata': {'finalizers': ['other1', finalizer, 'other2']}}
    patch = {}
    allow_deletion(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': ['other1', 'other2']}}


def test_remove_finalizers_when_absent():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    allow_deletion(body=body, patch=patch)
    assert patch == {}


def test_remove_finalizers_when_empty():
    body = {}
    patch = {}
    allow_deletion(body=body, patch=patch)
    assert patch == {}
