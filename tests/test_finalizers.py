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


# --- Patch.append_finalizer / remove_finalizer ---

def test_patch_append_finalizer_records_operation():
    patch = Patch()
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin']
    assert patch._finalizers_to_remove == []


def test_patch_remove_finalizer_records_operation():
    patch = Patch()
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin']
    assert patch._finalizers_to_append == []


def test_append_finalizer_removes_from_remove_list():
    patch = Patch()
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin']
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin']
    assert patch._finalizers_to_remove == []


def test_remove_finalizer_removes_from_append_list():
    patch = Patch()
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin']
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin']
    assert patch._finalizers_to_append == []


def test_append_finalizer_removes_all_duplicates_from_remove_list():
    patch = Patch()
    patch.remove_finalizer('fin')
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin', 'fin']
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin']
    assert patch._finalizers_to_remove == []


def test_remove_finalizer_removes_all_duplicates_from_append_list():
    patch = Patch()
    patch.append_finalizer('fin')
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin', 'fin']
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin']
    assert patch._finalizers_to_append == []


def test_append_finalizer_does_not_affect_other_finalizers_in_remove_list():
    patch = Patch()
    patch.remove_finalizer('other')
    patch.remove_finalizer('fin')
    patch.append_finalizer('fin')
    assert patch._finalizers_to_append == ['fin']
    assert patch._finalizers_to_remove == ['other']


def test_remove_finalizer_does_not_affect_other_finalizers_in_append_list():
    patch = Patch()
    patch.append_finalizer('other')
    patch.append_finalizer('fin')
    patch.remove_finalizer('fin')
    assert patch._finalizers_to_remove == ['fin']
    assert patch._finalizers_to_append == ['other']


def test_patch_bool_false_when_empty():
    patch = Patch()
    assert not patch


def test_patch_bool_true_with_dict_content():
    patch = Patch({'x': 'y'})
    assert patch


def test_patch_bool_true_with_append_finalizer():
    patch = Patch()
    patch.append_finalizer('fin')
    assert patch


def test_patch_bool_true_with_remove_finalizer():
    patch = Patch()
    patch.remove_finalizer('fin')
    assert patch


def test_patch_copy_preserves_finalizer_ops():
    original = Patch()
    original.append_finalizer('fin1')
    original.remove_finalizer('fin2')
    copy = Patch(original)
    assert copy._finalizers_to_append == ['fin1']
    assert copy._finalizers_to_remove == ['fin2']


def test_patch_copy_is_independent():
    original = Patch()
    original.append_finalizer('fin1')
    copy = Patch(original)
    copy.append_finalizer('fin2')
    assert original._finalizers_to_append == ['fin1']
    assert copy._finalizers_to_append == ['fin1', 'fin2']


# --- build_finalizer_json_patch ---

def test_json_patch_append_to_existing_finalizers():
    patch = Patch()
    patch.append_finalizer('new-fin')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['existing']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv1'},
        {'op': 'add', 'path': '/metadata/finalizers/-', 'value': 'new-fin'},
    ]


def test_json_patch_append_to_no_finalizers_field():
    patch = Patch()
    patch.append_finalizer('new-fin')
    body = {'metadata': {'resourceVersion': 'rv1'}}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv1'},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['new-fin']},
    ]


def test_json_patch_append_skips_if_already_present():
    patch = Patch()
    patch.append_finalizer('existing')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['existing']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == []


def test_json_patch_remove_existing_finalizer():
    patch = Patch()
    patch.remove_finalizer('fin')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['other', 'fin', 'another']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv1'},
        {'op': 'remove', 'path': '/metadata/finalizers/1'},
    ]


def test_json_patch_remove_skips_if_absent():
    patch = Patch()
    patch.remove_finalizer('missing')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['other']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == []


def test_json_patch_remove_adjusts_indices():
    patch = Patch()
    patch.remove_finalizer('a')
    patch.remove_finalizer('c')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['a', 'b', 'c']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv1'},
        {'op': 'remove', 'path': '/metadata/finalizers/0'},
        {'op': 'remove', 'path': '/metadata/finalizers/1'},
    ]


def test_json_patch_combined_append_and_remove():
    patch = Patch()
    patch.append_finalizer('new-fin')
    patch.remove_finalizer('old-fin')
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['old-fin']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': 'rv1'},
        {'op': 'add', 'path': '/metadata/finalizers/-', 'value': 'new-fin'},
        {'op': 'remove', 'path': '/metadata/finalizers/0'},
    ]


def test_json_patch_empty_when_no_operations():
    patch = Patch()
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['existing']}}
    result = patch.build_finalizer_json_patch(body)
    assert result == []


def test_json_patch_no_metadata():
    patch = Patch()
    patch.append_finalizer('fin')
    body = {}
    result = patch.build_finalizer_json_patch(body)
    assert result == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': None},
        {'op': 'add', 'path': '/metadata/finalizers', 'value': ['fin']},
    ]
