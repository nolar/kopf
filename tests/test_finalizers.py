import pytest

from kopf.structs.finalizers import FINALIZER, LEGACY_FINALIZER
from kopf.structs.finalizers import append_finalizers, remove_finalizers
from kopf.structs.finalizers import is_deleted, has_finalizers


def test_finalizer_is_fqdn():
    assert FINALIZER.startswith('kopf.zalando.org/')


@pytest.mark.parametrize('expected, body', [
    pytest.param(True, {'metadata': {'deletionTimestamp': '2020-12-31T23:59:59'}}, id='time'),
    pytest.param(False, {'metadata': {'deletionTimestamp': None}}, id='none'),
    pytest.param(False, {'metadata': {}}, id='no-field'),
    pytest.param(False, {}, id='no-metadata'),
])
def test_is_deleted(expected, body):
    result = is_deleted(body=body)
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
    result = has_finalizers(body=body)
    assert result == expected


def test_append_finalizers_to_others():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    append_finalizers(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': ['other1', 'other2', FINALIZER]}}


def test_append_finalizers_to_empty():
    body = {}
    patch = {}
    append_finalizers(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': [FINALIZER]}}


def test_append_finalizers_when_present():
    body = {'metadata': {'finalizers': ['other1', FINALIZER, 'other2']}}
    patch = {}
    append_finalizers(body=body, patch=patch)
    assert patch == {}


@pytest.mark.parametrize('finalizer', [
    pytest.param(LEGACY_FINALIZER, id='legacy'),
    pytest.param(FINALIZER, id='normal'),
])
def test_remove_finalizers_keeps_others(finalizer):
    body = {'metadata': {'finalizers': ['other1', finalizer, 'other2']}}
    patch = {}
    remove_finalizers(body=body, patch=patch)
    assert patch == {'metadata': {'finalizers': ['other1', 'other2']}}


def test_remove_finalizers_when_absent():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = {}
    remove_finalizers(body=body, patch=patch)
    assert patch == {}


def test_remove_finalizers_when_empty():
    body = {}
    patch = {}
    remove_finalizers(body=body, patch=patch)
    assert patch == {}
