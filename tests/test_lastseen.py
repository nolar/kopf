import json

import pytest

from kopf.structs.lastseen import LAST_SEEN_ANNOTATION
from kopf.structs.lastseen import has_essence_stored, get_essence
from kopf.structs.lastseen import get_essential_diffs
from kopf.structs.lastseen import retrieve_essence, refresh_essence


def test_annotation_is_fqdn():
    assert LAST_SEEN_ANNOTATION.startswith('kopf.zalando.org/')


@pytest.mark.parametrize('expected, body', [
    pytest.param(False, {}, id='no-metadata'),
    pytest.param(False, {'metadata': {}}, id='no-annotations'),
    pytest.param(False, {'metadata': {'annotations': {}}}, id='no-lastseen'),
    pytest.param(True, {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: ''}}}, id='present'),
])
def test_has_essence(expected, body):
    result = has_essence_stored(body=body)
    assert result == expected


def test_get_essence_removes_resource_references():
    body = {'apiVersion': 'group/version', 'kind': 'Kind'}
    essence = get_essence(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
def test_get_essence_removes_system_fields_and_cleans_parents(field):
    body = {'metadata': {field: 'x'}}
    essence = get_essence(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
def test_get_essence_removes_system_fields_but_keeps_extra_fields(field):
    body = {'metadata': {field: 'x', 'other': 'y'}}
    essence = get_essence(body=body, extra_fields=['metadata.other'])
    assert essence == {'metadata': {'other': 'y'}}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
def test_get_essence_removes_garbage_annotations_and_cleans_parents(annotation):
    body = {'metadata': {'annotations': {annotation: 'x'}}}
    essence = get_essence(body=body)
    assert essence == {}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
def test_get_essence_removes_garbage_annotations_but_keeps_others(annotation):
    body = {'metadata': {'annotations': {annotation: 'x', 'other': 'y'}}}
    essence = get_essence(body=body)
    assert essence == {'metadata': {'annotations': {'other': 'y'}}}


def test_get_essence_removes_status_and_cleans_parents():
    body = {'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}}
    essence = get_essence(body=body)
    assert essence == {}


def test_get_essence_removes_status_but_keeps_extra_fields():
    body = {'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}}
    essence = get_essence(body=body, extra_fields=['status.other'])
    assert essence == {'status': {'other': 'z'}}


def test_get_essence_clones_body():
    body = {'spec': {'depth': {'field': 'x'}}}
    essence = get_essence(body=body)
    body['spec']['depth']['field'] = 'y'
    assert essence is not body
    assert essence['spec'] is not body['spec']
    assert essence['spec']['depth'] is not body['spec']['depth']
    assert essence['spec']['depth']['field'] == 'x'


def test_refresh_essence():
    body = {'spec': {'depth': {'field': 'x'}}}
    patch = {}
    encoded = json.dumps(body)  # json formatting can vary across interpreters
    refresh_essence(body=body, patch=patch)
    assert patch['metadata']['annotations'][LAST_SEEN_ANNOTATION] == encoded


def test_retreive_essence_when_present():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}}}
    essence = retrieve_essence(body=body)
    assert essence == data


def test_retreive_essence_when_absent():
    body = {}
    essence = retrieve_essence(body=body)
    assert essence is None


def test_essence_changed_detected():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}}}
    old, new, diff = get_essential_diffs(body=body)
    assert diff


def test_essence_change_ignored_with_garbage_annotations():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}},
            'spec': {'depth': {'field': 'x'}}}
    old, new, diff = get_essential_diffs(body=body)
    assert not diff


def test_essence_changed_ignored_with_system_fields():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded},
                         'finalizers': ['x', 'y', 'z'],
                         'generation': 'x',
                         'resourceVersion': 'x',
                         'creationTimestamp': 'x',
                         'deletionTimestamp': 'x',
                         'any-unexpected-field': 'x',
                         'uid': 'uid',
                         },
            'status': {'kopf': {'progress': 'x', 'anything': 'y'},
                       'other': 'x'
                       },
            'spec': {'depth': {'field': 'x'}}}
    old, new, diff = get_essential_diffs(body=body)
    assert not diff


# This is to ensure it is callable with proper signature.
# For actual tests of diffing, see `/tests/diffs/`.
def test_essence_diff():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}},
            'status': {'x': 'y'},
            'spec': {'depth': {'field': 'y'}}}
    old, new, diff = get_essential_diffs(body=body, extra_fields=['status.x'])
    assert old == {'spec': {'depth': {'field': 'x'}}}
    assert new == {'spec': {'depth': {'field': 'y'}}, 'status': {'x': 'y'}}
    assert len(diff) == 2  # spec.depth.field & status.x, but the order is not known.
