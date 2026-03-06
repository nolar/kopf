import functools

import pytest

from kopf._cogs.structs.finalizers import allow_deletion, block_deletion, \
                                          is_deletion_blocked, is_deletion_ongoing
from kopf._cogs.structs.patches import Patch


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
    body = {'metadata': {'finalizers': ['other1', 'other2'], 'resourceVersion': '1234567890'}}
    patch = Patch(fns=[functools.partial(block_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'add', 'path': '/metadata/finalizers/2', 'value': 'fin'}]


def test_append_finalizers_to_empty():
    body = {}
    patch = Patch(fns=[functools.partial(block_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'add', 'path': '/metadata', 'value': {'finalizers': ['fin']}}]


def test_append_finalizers_when_present():
    body = {'metadata': {'finalizers': ['other1', 'fin', 'other2']}}
    patch = Patch(fns=[functools.partial(block_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == []


def test_remove_finalizers_keeps_others():
    body = {'metadata': {'finalizers': ['other1', 'fin', 'other2'], 'resourceVersion': '1234567890'}}
    patch = Patch(fns=[functools.partial(allow_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'remove', 'path': '/metadata/finalizers/1'}]


def test_remove_finalizers_cleans_keys():
    body = {'metadata': {'finalizers': ['fin'], 'resourceVersion': '1234567890'}}
    patch = Patch(fns=[functools.partial(allow_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'remove', 'path': '/metadata/finalizers'}]


def test_remove_finalizers_cleans_metadata():
    body = {'metadata': {'finalizers': ['fin']}}
    patch = Patch(fns=[functools.partial(allow_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'remove', 'path': '/metadata'}]


def test_remove_finalizers_when_absent():
    body = {'metadata': {'finalizers': ['other1', 'other2']}}
    patch = Patch(fns=[functools.partial(allow_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == []


def test_remove_finalizers_when_empty():
    body = {}
    patch = Patch(fns=[functools.partial(allow_deletion, finalizer='fin')])
    ops = patch.as_json_patch(body)
    assert ops == []
