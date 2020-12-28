import pytest

from kopf.storage.finalizers import allow_deletion, block_deletion, \
                                    is_deletion_blocked, is_deletion_ongoing


def test_finalizer_is_fqdn(settings):
    assert settings.persistence.finalizer.startswith('kopf.zalando.org/')


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
    pytest.param(True, {'metadata': {'finalizers': ['fin']}}, id='normal'),
    pytest.param(True, {'metadata': {'finalizers': ['other', 'fin']}}, id='mixed'),
])
def test_has_finalizers(expected, body):
    result = is_deletion_blocked(body=body, finalizer='fin')
    assert result == expected


def test_append_finalizers_to_others():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    block_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {'metadata': {'finalizers': ['other1', 'other2', 'fin']}}


def test_append_finalizers_to_empty():
    body = {}
    patch = {}
    block_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {'metadata': {'finalizers': ['fin']}}


def test_append_finalizers_when_present():
    body = {'metadata': {'finalizers': ['other1', 'fin', 'other2']}}
    patch = {}
    block_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {}


@pytest.mark.parametrize('finalizer', [
    pytest.param('fin', id='normal'),
])
def test_remove_finalizers_keeps_others(finalizer):
    body = {'metadata': {'finalizers': ['other1', finalizer, 'other2']}}
    patch = {}
    allow_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {'metadata': {'finalizers': ['other1', 'other2']}}


def test_remove_finalizers_when_absent():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    allow_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {}


def test_remove_finalizers_when_empty():
    body = {}
    patch = {}
    allow_deletion(body=body, patch=patch, finalizer='fin')
    assert patch == {}
