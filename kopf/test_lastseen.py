import json

import pytest

from kopf.structs.lastseen import LAST_SEEN_ANNOTATION
from kopf.structs.lastseen import has_state, get_state
from kopf.structs.lastseen import is_state_changed, get_state_diffs
from kopf.structs.lastseen import retreive_state, refresh_state


def test_annotation_is_fqdn():
    assert LAST_SEEN_ANNOTATION.startswith('kopf.zalando.org/')


@pytest.mark.parametrize('expected, body', [
    pytest.param(False, {}, id='no-metadata'),
    pytest.param(False, {'metadata': {}}, id='no-annotations'),
    pytest.param(False, {'metadata': {'annotations': {}}}, id='no-lastseen'),
    pytest.param(True, {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: ''}}}, id='present'),
])
def test_has_state(expected, body):
    result = has_state(body=body)
    assert result == expected


@pytest.mark.parametrize('field', [
    'uid',
    'selfLink',
    'generation',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
])
def test_get_state_removes_system_fields_and_cleans_parents(field):
    body = {'metadata': {field: 'x'}}
    state = get_state(body=body)
    assert state == {}


@pytest.mark.parametrize('field', [
    'uid',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
])
def test_get_state_removes_system_fields_and_keeps_others(field):
    body = {'metadata': {field: 'x', 'other': 'y'}}
    state = get_state(body=body)
    assert state == {'metadata': {'other': 'y'}}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
def test_get_state_removes_annotations_and_cleans_parents(annotation):
    body = {'metadata': {'annotations': {annotation: 'x'}}}
    state = get_state(body=body)
    assert state == {}


@pytest.mark.parametrize('annotation', [
    pytest.param(LAST_SEEN_ANNOTATION, id='kopf'),
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
def test_get_state_removes_annotations_and_keeps_others(annotation):
    body = {'metadata': {'annotations': {annotation: 'x', 'other': 'y'}}}
    state = get_state(body=body)
    assert state == {'metadata': {'annotations': {'other': 'y'}}}


def test_get_state_removes_kopf_status_and_cleans_parents():
    body = {'status': {'kopf': {'progress': 'x', 'anything': 'y'}}}
    state = get_state(body=body)
    assert state == {}


def test_get_state_removes_kopf_status_and_keeps_others():
    body = {'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}}
    state = get_state(body=body)
    assert state == {'status': {'other': 'z'}}


def test_get_state_clones_body():
    body = {'spec': {'depth': {'field': 'x'}}}
    state = get_state(body=body)
    body['spec']['depth']['field'] = 'y'
    assert state is not body
    assert state['spec'] is not body['spec']
    assert state['spec']['depth'] is not body['spec']['depth']
    assert state['spec']['depth']['field'] == 'x'


def test_refresh_state():
    body = {'spec': {'depth': {'field': 'x'}}}
    patch = {}
    encoded = json.dumps(body)  # json formatting can vary across interpreters
    refresh_state(body=body, patch=patch)
    assert patch['metadata']['annotations'][LAST_SEEN_ANNOTATION] == encoded


def test_retreive_state_when_present():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}}}
    state = retreive_state(body=body)
    assert state == data


def test_retreive_state_when_absent():
    body = {}
    state = retreive_state(body=body)
    assert state is None


def test_state_is_changed():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}}}
    result = is_state_changed(body=body)
    assert isinstance(result, bool)
    assert result == True


def test_state_is_not_changed_clean():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}},
            'spec': {'depth': {'field': 'x'}}}
    result = is_state_changed(body=body)
    assert isinstance(result, bool)
    assert result == False


def test_state_is_not_changed_with_system_noise():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded},
                         'finalizers': ['x', 'y', 'z'],
                         'generation': 'x',
                         'resourceVersion': 'x',
                         'creationTimestamp': 'x',
                         'deletionTimestamp': 'x',
                         'uid': 'uid',
                         },
            'status': {'kopf': {'progress': 'x', 'anything': 'y'}},
            'spec': {'depth': {'field': 'x'}}}
    result = is_state_changed(body=body)
    assert isinstance(result, bool)
    assert result == False


# This is to ensure it is callable with proper signsture.
# For actual tests of diffing, see `/tests/diffs/`.
def test_state_diff():
    data = {'spec': {'depth': {'field': 'x'}}}
    encoded = json.dumps(data)  # json formatting can vary across interpreters
    body = {'metadata': {'annotations': {LAST_SEEN_ANNOTATION: encoded}},
            'status': {'x': 'y'},
            'spec': {'depth': {'field': 'y'}}}
    old, new, diff = get_state_diffs(body=body)
    assert old == {'spec': {'depth': {'field': 'x'}}}
    assert new == {'spec': {'depth': {'field': 'y'}}, 'status': {'x': 'y'}}
    assert len(diff) == 2  # spec.depth.field & status.x, but the order is not known.
