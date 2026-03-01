import copy

from kopf._cogs.structs.patches import Patch


def test_patch_fns_empty_produces_no_ops():
    patch = Patch()
    body = {'metadata': {'resourceVersion': 'rv1'}, 'spec': {'x': 1}}
    ops = patch.as_json_patch(body)
    assert ops == []


def test_patch_fns_noop_produces_no_ops():
    patch = Patch(fns=[lambda body: None])
    body = {'metadata': {'resourceVersion': 'rv1'}, 'spec': {'x': 1}}
    ops = patch.as_json_patch(body)
    assert ops == []


def test_patch_fns_field_replacement():
    def set_field(body):
        body['spec']['x'] = 'new'

    patch = Patch(fns=[set_field])
    body = {'metadata': {'resourceVersion': 'rv1'}, 'spec': {'x': 'old'}}
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'replace', 'path': '/spec/x', 'value': 'new'}]


def test_patch_fns_list_append():
    def append_item(body):
        body['metadata']['finalizers'].append('new-fin')

    patch = Patch(fns=[append_item])
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['a', 'b']}}
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'add', 'path': '/metadata/finalizers/2', 'value': 'new-fin'}]


def test_patch_fns_list_remove():
    def remove_item(body):
        body['metadata']['finalizers'].remove('b')

    patch = Patch(fns=[remove_item])
    body = {'metadata': {'resourceVersion': 'rv1', 'finalizers': ['a', 'b', 'c']}}
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'remove', 'path': '/metadata/finalizers/1'}]


def test_patch_fns_multiple_patch_chained():
    def add_field(body):
        body['spec']['y'] = 2

    def modify_field(body):
        body['spec']['y'] = 3

    patch = Patch(fns=[add_field, modify_field])
    body = {'metadata': {'resourceVersion': 'rv1'}, 'spec': {'x': 1}}
    ops = patch.as_json_patch(body)
    assert ops == [{'op': 'add', 'path': '/spec/y', 'value': 3}]


def test_patch_fns_does_not_mutate_original_body():
    def set_field(body):
        body['spec']['x'] = 'modified'

    patch = Patch(fns=[set_field])
    body = {'metadata': {'resourceVersion': 'rv1'}, 'spec': {'x': 'original'}}
    body_before = copy.deepcopy(body)
    patch.as_json_patch(body)
    assert body == body_before
